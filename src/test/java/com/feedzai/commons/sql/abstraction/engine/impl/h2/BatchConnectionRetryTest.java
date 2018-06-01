/*
 * Copyright 2014 Feedzai
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
package com.feedzai.commons.sql.abstraction.engine.impl.h2;

import com.feedzai.commons.sql.abstraction.batch.BatchEntry;
import com.feedzai.commons.sql.abstraction.batch.DefaultBatch;
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.engine.AbstractDatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.BOOLEAN;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.DOUBLE;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.LONG;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.STRING;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.entry;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.MAX_NUMBER_OF_RETRIES;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.RETRY_INTERVAL;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Validates that batches retry the
 *
 * @author Diogo Guerra (diogo.guerra@feedzai.com)
 * @since 2.1.1
 */
@RunWith(Parameterized.class)
public class BatchConnectionRetryTest {
    /*
     * Run only for h2.
     */
    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("h2");
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

    private DatabaseEngine engine;

    private BatchEntry[] failureResults = null;

    @Before
    public void init() throws DatabaseEngineException, DatabaseFactoryException {
        Properties properties = new Properties() {

            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "drop-create");
                setProperty(MAX_NUMBER_OF_RETRIES, "5");
                setProperty(RETRY_INTERVAL, "1000");
            }
        };

        engine = DatabaseFactory.getConnection(properties);
    }

    @After
    public void cleanup() {
        engine.close();
    }

    /**
     * Tests that a batch retries the connection after a failed flush.
     *
     * @throws DatabaseEngineException If the operations on the engine fail.
     */
    @Test
    public void testConnectionRetryAfterBatchFailure() throws DatabaseEngineException {
        final AtomicBoolean allowConnection = new AtomicBoolean(true);
        final AtomicInteger failedConnections = new AtomicInteger(0);

        final AtomicReference<DatabaseEngine> engineCapsule = new AtomicReference<>(engine);

        // mock the connect method to force the retry of the connections to be exhausted
        new MockUp<AbstractDatabaseEngine>() {

            @Mock
            void connect(Invocation inv) throws Exception {
                if (allowConnection.get()) {
                    failedConnections.set(0);
                    inv.proceed();
                } else {
                    if (failedConnections.incrementAndGet() == 6) {
                        allowConnection.set(true);
                    }
                    throw new SQLException("Could not connect");
                }

            }

        };


        // mock the onFlushFailure method to ensure that the failure is invoked when the connection is not ok.
        new MockUp<DefaultBatch>() {

            @Mock
            public void onFlushFailure(BatchEntry[] entries) {
                failureResults = entries;
            }
        };

        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING).build();

        engine.addEntity(entity);

        DefaultBatch batch = DefaultBatch.create(engine, "test", 5, 1000000L, engine.getProperties().getMaximumAwaitTimeBatchShutdown());

        // 1. perform a simple add with a connection ok
        batch.add("TEST", entry().set("COL1", 1).build());
        batch.flush();
        assertNull("Check that there were no failure results", failureResults);

        // 2. force an error disallowing the connection to be fetched
        try {
            engineCapsule.get().getConnection().close();
        } catch (Exception e) {
        }
        // disallow new connections to force to exhaust the retry mechanism
        allowConnection.set(false);


        batch.add("TEST", entry().set("COL1", 2).build());
        batch.flush();
        assertEquals("Check that transactions sent to the failure batch", 1, failureResults.length);
        assertEquals("table name ok?", "TEST", failureResults[0].getTableName());
        assertEquals("COL1 value ok?", 2, failureResults[0].getEntityEntry().get("COL1"));

        //clean failure results state
        failureResults = null;

        // 3. restore the connection and make sure that the code tries to reconnect and doesn't fail the batch
        batch.add("TEST", entry().set("COL1", 4).build());
        batch.flush();
        assertNull("Check that there were no failure results", failureResults);

    }
}
