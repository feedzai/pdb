/*
 * Copyright 2019 Feedzai
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

import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.dml.Query;
import com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineDriver;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineTimeoutException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.column;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.table;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SELECT_QUERY_TIMEOUT;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;


/**
 * Unit tests for the query timeout feature. Tests run a query that takes very long to run
 * with a short timeout and verify that the statement aborts with the right error code.
 */
@RunWith(Parameterized.class)
public class QueryTimeoutsTest {

    /**
     * Query timeouts supported only on the Postgres, SQL Server and MySQL. SQL Server is not
     * included in the list below because it it able to handle the test query in ms, and so
     * is not testeable in this test.
     */
    private static List<DatabaseEngineDriver> SUPPORTED_DRIVERS = ImmutableList.of(
            DatabaseEngineDriver.POSTGRES,
            DatabaseEngineDriver.MYSQL
    );

    /**
     * A query that makes a sort in 1.000.000.000 rows, taking WAY more than
     * the test timeout until the first row is returned.
     */
    private static Query HEAVY_QUERY = SqlBuilder
        .select(
                column("QUERY_TIMEOUT_TEST_1", "COL1")
        )
        .from(
                table("QUERY_TIMEOUT_TEST_1"),
                table("QUERY_TIMEOUT_TEST_2"),
                table("QUERY_TIMEOUT_TEST_3")
        )
        .orderby(
                column("QUERY_TIMEOUT_TEST_3", "COL1")
        )
        .limit(
                1
        );

    /**
     * Default timeout in all tests.
     */
    private static final int TEST_TIMEOUT_SECS = 2;

    /**
     * Timeout override in all tests with timeout overrides.
     */
    private static final int TEST_TIMEOUT_OVERRIDE_SECS = 6;

    /**
     * The {@link DatabaseEngine} used in each tests.
     */
    private DatabaseEngine engine;

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations();
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

    @Before
    public void setupTest() throws DatabaseFactoryException, DatabaseEngineException {
        final Properties dbProps = new Properties() {
            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "create-drop");
                put(SELECT_QUERY_TIMEOUT, TEST_TIMEOUT_SECS);
            }
        };

        final DatabaseEngineDriver driver = DatabaseEngineDriver.fromEngine(dbProps.getProperty(ENGINE));
        assumeTrue(driver + " engine doesn't support setting timeouts, tests will be skipped",
                SUPPORTED_DRIVERS.contains(driver));

        // Create connection
        engine = DatabaseFactory.getConnection(dbProps);

        // Create test tables and load each with 1000 rows.
        for (int tblIdx = 1; tblIdx <= 3; tblIdx++) {
            final String tblName = "QUERY_TIMEOUT_TEST_" + tblIdx;
            final DbEntity.Builder entity = dbEntity()
                    .name(tblName)
                    .addColumn("COL1", INT);
            engine.addEntity(entity.build());
            for (int i = 0; i < 1000; i++) {
                engine.persist(tblName, new EntityEntry.Builder()
                        .set("COL1", i)
                        .build()
                );
            }
        }
    }

    @After
    public void cleanResources() {
        if (engine != null) {
            engine.close();
        }
    }

    @Test(expected = DatabaseEngineTimeoutException.class)
    public void iteratorWithDefaultsTest() throws DatabaseEngineException {
        engine.iterator(HEAVY_QUERY).nextResult();
    }

    @Test(expected = DatabaseEngineTimeoutException.class)
    public void iteratorWithOverrideTest() throws DatabaseEngineException {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            engine.iterator(HEAVY_QUERY, 1000, TEST_TIMEOUT_OVERRIDE_SECS).nextResult();
        } finally {
            assertTrue(stopwatch.elapsed(TimeUnit.SECONDS) >= TEST_TIMEOUT_OVERRIDE_SECS - 1);
        }
    }

    @Test(expected = DatabaseEngineTimeoutException.class)
    public void queryWithDefaultsTest() throws DatabaseEngineException {
        engine.query(HEAVY_QUERY);
    }

    @Test(expected = DatabaseEngineTimeoutException.class)
    public void queryWithOverrideTest() throws DatabaseEngineException {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            engine.query(HEAVY_QUERY, 10);
        } finally {
            assertTrue(stopwatch.elapsed(TimeUnit.SECONDS) >= TEST_TIMEOUT_OVERRIDE_SECS - 1);
        }
    }

}
