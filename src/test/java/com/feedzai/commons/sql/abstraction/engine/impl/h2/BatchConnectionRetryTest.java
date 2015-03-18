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
import com.feedzai.commons.sql.abstraction.engine.*;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.*;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.entry;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;
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
    public static Collection<Object[]> data() throws Exception {
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
                setProperty(MAX_NUMBER_OF_RETRIES, "10");
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
     * @throws DatabaseEngineException
     */
    @Test
    public void testConnectionRetryAfterBatchFailure() throws DatabaseEngineException {
        final AtomicBoolean allowTransaction = new AtomicBoolean(true);
        final AtomicBoolean isConnectionOk = new AtomicBoolean(true);

        new MockUp<AbstractDatabaseEngine>() {

            @Mock
            void addBatch(Invocation inv, final String name, final EntityEntry entry) throws DatabaseEngineException {
                // mock the addBatch method as throwing an exception is the connection is ok. the connection status is controlled ty the test case.
                if (isConnectionOk.get()) {
                    inv.proceed();
                } else {
                    throw new DatabaseEngineRuntimeException("Some random database error");
                }

            }

            @Mock
            public Connection getConnection(Invocation inv) throws RetryLimitExceededException, InterruptedException, RecoveryException {
                // in this mock, the test can control when a connection can be retrieved (the retry mechanism is inside the getConnection method,
                // though it will not be executed when the flag is set to false.

                // the isConnectionOk is only set to true/false when the code really calls the getConnection and the test wants to force the broken state of the connection
                if (allowTransaction.get()) {
                    isConnectionOk.set(true);
                    return inv.proceed();
                } else {
                    isConnectionOk.set(false);
                    throw new RetryLimitExceededException();
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
        allowTransaction.set(false);
        batch.add("TEST", entry().set("COL1", 2).build());
        batch.flush();
        assertEquals("Check that transactions sent to the failure batch", 1, failureResults.length);
        assertEquals("table name ok?", "TEST", failureResults[0].getTableName());
        assertEquals("COL1 value ok?", 2, failureResults[0].getEntityEntry().get("COL1"));

        //clean failure results state
        failureResults = null;

        // 3. restore the connection and make sure that the code tries to reconnect and doesn't fail the batch
        allowTransaction.set(true);
        batch.add("TEST", entry().set("COL1", 4).build());
        batch.flush();
        assertNull("Check that there were no failure results", failureResults);

    }
}
