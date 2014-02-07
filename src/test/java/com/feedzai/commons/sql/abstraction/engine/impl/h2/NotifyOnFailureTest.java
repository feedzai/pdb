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
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.*;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.entry;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;
import static org.junit.Assert.assertEquals;

/**
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class NotifyOnFailureTest {
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
    protected Properties properties;

    private BatchEntry[] failureResults = null;

    @Before
    public void init() throws DatabaseEngineException, DatabaseFactoryException {
        properties = new Properties() {

            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "drop-create");
            }
        };

        engine = DatabaseFactory.getConnection(properties);
    }

    @After
    public void cleanup() {
        engine.close();
    }

    @Test
    public void testBatchFailureTest() throws DatabaseEngineException {
        mockClasses();
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING).build();

        engine.addEntity(entity);

        DefaultBatch batch = DefaultBatch.create(engine, "test", 5, 10000L, engine.getProperties().getMaximumAwaitTimeBatchShutdown());

        for (int i = 0; i < 5; i++) {
            batch.add("TEST", entry().set("COL1", i).build());
        }

        assertEquals("", 5, failureResults.length);
        for (int i = 0; i < 5; i++) {
            assertEquals("table name ok?", "TEST", failureResults[i].getTableName());
            assertEquals("COL1 value ok?", new Integer(i), failureResults[i].getEntityEntry().get("COL1"));
        }

    }

    private void mockClasses() {
        new MockUp<DefaultBatch>() {

            @Mock
            public void onFlushFailure(BatchEntry[] entries) {
                failureResults = entries;
            }

            @Mock
            public void run() {
                // Ignore batch flushing on timeout.
            }
        };

        new MockUp<AbstractDatabaseEngine>() {
            @Mock
            public void beginTransaction() throws DatabaseEngineRuntimeException {
                throw new DatabaseEngineRuntimeException("", new RetryLimitExceededException(""));
            }
        };
    }
}
