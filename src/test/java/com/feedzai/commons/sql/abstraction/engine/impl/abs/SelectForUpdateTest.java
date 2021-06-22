/*
 * Copyright 2021 Feedzai
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.feedzai.commons.sql.abstraction.engine.impl.abs;

import com.feedzai.commons.sql.abstraction.ddl.DbColumnType;
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineTimeoutException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.impl.cockroach.SkipTestCockroachDB;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.all;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.column;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.entry;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.eq;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.k;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.select;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.table;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.update;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;

/**
 * Tests the SELECT ... FOR UPDATE case.
 *
 * @author Artur Pedroso (artur.pedroso@feedzai.com)
 * @since 2.8.4
 */
@RunWith(Parameterized.class)
// Cockroach doesn't respect FOR UPDATE. It will execute transactions with Serializable isolation and we need
// to handle errors when updating the same item concurrently on the client side
// (see https://www.cockroachlabs.com/docs/v19.2/postgresql-compatibility#locking-and-for-update).
@Category(SkipTestCockroachDB.class)
public class SelectForUpdateTest {
    /**
     * DB table used in the test.
     */
    private static final String TEST_TABLE = "pretty_locks_table";

    /**
     * The PK column of the table.
     */
    private static final String PK_COL = "pk";

    /**
     * A column that will be updated during the test.
     */
    private static final String COOL_COL = "change_me";

    /**
     * Initial value in the {@link #COOL_COL} column.
     */
    private static final String INIT_COOL_COL_VAL = "init_val";

    /**
     * Expected final value in the {@link #COOL_COL} column.
     */
    private static final String FINAL_COOL_COL_VAL = "final_val";

    /**
     * Value to store in the {@link #PK_COL} column.
     */
    private static final String PK_COL_VAL = "PK";

    /**
     * The primary {@link DatabaseEngine} to use in the test.
     */
    protected DatabaseEngine engine;

    /**
     * Useful properties for the {@link DatabaseEngine}.
     */
    protected Properties properties;

    /**
     * Loads the DB configurations to be used in the tests.
     *
     * @return The list of DB configurations.
     * @throws Exception In case configurations can't be loaded.
     */
    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations();
    }

    /**
     * A specific DB config loaded that will be used in the test.
     */
    @Parameterized.Parameter
    public DatabaseConfiguration config;

    @Before
    public void init() throws Exception {
        this.properties = new Properties() {

            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "create");
            }
        };

        this.engine = DatabaseFactory.getConnection(this.properties);

        // Create table
        DbEntity testEntity = new DbEntity.Builder()
                .name(TEST_TABLE)
                .addColumn(PK_COL, DbColumnType.STRING)
                .addColumn(COOL_COL, DbColumnType.STRING)
                .pkFields(PK_COL)
                .build();
        this.engine.addEntity(testEntity);

        // Populate with first item
        this.engine.persist(
                TEST_TABLE,
                entry().set(PK_COL, PK_COL_VAL)
                        .set(COOL_COL, INIT_COOL_COL_VAL)
                        .build(),
                false
        );
    }

    @After
    public void cleanup() throws Exception {
        this.engine.dropEntity(TEST_TABLE);
        this.engine.close();
    }

    /**
     * Tests that when a client performs a select for update on a table previously selected for update, it will block
     * waiting for the first client to finish the transaction.
     *
     * @throws Exception In case anything goes wrong.
     */
    @Test(timeout = 20000)
    public void testThatSecondClientQueryReadsValueOnlyAfterFirstClientCommit() throws Exception {
        this.engine.beginTransaction();
        queryForUpdate(this.engine);

        final CountDownLatch secondClientSelectedForUpdate = new CountDownLatch(1);
        final AtomicBoolean secondClientRunning = new AtomicBoolean(true);
        final Future<Boolean> secondClient = CompletableFuture.supplyAsync(() -> {
            try {
                final DatabaseEngine engine2 = DatabaseFactory.getConnection(properties);
                while (secondClientRunning.get()) {
                    try {
                        engine2.beginTransaction();
                        Assertions.assertThat(queryForUpdate(engine2).get(0).get(COOL_COL).toString())
                                .as("Second client should read value updated from first client after 1st client commit.")
                                .isEqualTo(FINAL_COOL_COL_VAL);
                        secondClientSelectedForUpdate.countDown();
                        engine2.commit();
                        return true;
                    } catch (final DatabaseEngineTimeoutException e) {
                        // no op
                    }
                }
            } catch (final Exception e) {
                throw new RuntimeException("Unexpected exception while running second client.", e);
            }
            return false;
        });

        Assertions.assertThat(secondClientSelectedForUpdate.await(5, TimeUnit.SECONDS))
                .as("Second client should be waiting for DB lock from first client to be released.")
                .isFalse();

        this.engine.executeUpdate(
                update(table(TEST_TABLE))
                        .set(eq(column((COOL_COL)), k(FINAL_COOL_COL_VAL)))
                        .where(eq(column(PK_COL), k(PK_COL_VAL)))
        );
        this.engine.commit();

        boolean secondClientFinished = false;
        try {
            secondClientFinished = secondClient.get(15, TimeUnit.SECONDS);
        } finally {
            Assertions.assertThat(secondClientFinished).isTrue();
            secondClientRunning.set(false);
        }
    }

    private List<Map<String, ResultColumn>> queryForUpdate(final DatabaseEngine engine) throws DatabaseEngineException {
        return engine.query(select(all())
                .from(table(TEST_TABLE))
                .where(eq(column(PK_COL), k(PK_COL_VAL)))
                .forUpdate(true));
    }
}
