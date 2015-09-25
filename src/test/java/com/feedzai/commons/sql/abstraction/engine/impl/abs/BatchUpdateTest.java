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
package com.feedzai.commons.sql.abstraction.engine.impl.abs;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.feedzai.commons.sql.abstraction.batch.AbstractBatch;
import com.feedzai.commons.sql.abstraction.batch.BatchEntry;
import com.feedzai.commons.sql.abstraction.batch.DefaultBatch;
import com.feedzai.commons.sql.abstraction.ddl.*;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.engine.*;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import mockit.*;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.*;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;
import static org.junit.Assert.*;

/**
 * Tests for AbstractBatch.
 *
 * @author Paulo Leitao (paulo.leitao@feedzai.com)
 * @since 2.1.4
 */
@RunWith(Parameterized.class)
public class BatchUpdateTest {

    protected DatabaseEngine engine;
    protected Properties properties;

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations();
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

    @BeforeClass
    public static void initStatic() {
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.TRACE);
    }

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
    public void batchInsertExplicitFlushTest() throws Exception {
        final int numTestEntries = 5;
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        DefaultBatch batch = DefaultBatch.create(engine, "batchInsertWithDBConnDownTest", numTestEntries + 1, 100000, 1000000);

        // Add entries to batch no flush should take place because numEntries < batch size and batch timeout is huge
        for(int i = 0 ; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Explicit flush
        batch.flush();

        // Check entries are in DB
        checkTestEntriesInDB(numTestEntries);
    }

    @Test
    public void batchInsertFlushBySizeTest() throws Exception {
        final int numTestEntries = 5;

        addTestEntity();
        DefaultBatch batch = DefaultBatch.create(engine, "batchInsertWithDBConnDownTest", numTestEntries, 100000, 1000000);

        // Add entries to batch, no flush needed because #inserted entries = batch size
        for(int i = 0 ; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Check entries are in DB
        checkTestEntriesInDB(numTestEntries);
    }

    @Test
    public void batchInsertFlushByTimeTest() throws Exception {
        final int numTestEntries = 5;
        final long batchTimeout = 1000;     // Flush after 1 sec

        addTestEntity();
        DefaultBatch batch = DefaultBatch.create(engine, "batchInsertWithDBConnDownTest", numTestEntries + 1, batchTimeout, 1000000);
        for(int i = 0 ; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Wait for flush
        Thread.sleep(batchTimeout + 1000);

        // Check entries are in DB
        checkTestEntriesInDB(numTestEntries);
    }

    @Test
    public void batchInsertFlushBySizeWithDBErrorTest(@Mocked final DatabaseEngine engine) throws Exception {
        final int numTestEntries = 5;

        addTestEntity();
        MockedBatch batch = MockedBatch.create(engine, "batchInsertWithDBConnDownTest", numTestEntries, 100000, 1000000);

        // Simulate failures in beginTransaction() for flush to fail
        new Expectations() {{
            engine.beginTransaction(); result = new DatabaseEngineRuntimeException("Error !");
        }};

        // Add entries to batch, no flush needed because #inserted entries = batch size
        for(int i = 0 ; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Check that entries were added to onFlushFailure()
        assertEquals("Entries were added to failed", batch.getFailedEntries().size(), numTestEntries);
    }

    @Test
    public void batchInsertFlushByTimeWithDBErrorTest(@Mocked final DatabaseEngine engine) throws Exception {
        final int numTestEntries = 5;
        final long batchTimeout = 1000;     // Flush after 1 sec

        addTestEntity();
        MockedBatch batch = MockedBatch.create(engine, "batchInsertWithDBConnDownTest", numTestEntries + 1, batchTimeout, 1000000);

        // Simulate failures in beginTransaction() for flush to fail
        new Expectations() {{
            engine.beginTransaction(); result = new DatabaseEngineRuntimeException("Error !");
        }};

        for(int i = 0 ; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Wait for flush
        Thread.sleep(batchTimeout + 1000);

        // Check that entries were added to onFlushFailure()
        assertEquals("Entries were added to failed", batch.getFailedEntries().size(), numTestEntries);
    }

    private void addTestEntity() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();
        engine.addEntity(entity);
    }

    private void checkTestEntriesInDB(int numEntries) throws DatabaseEngineException {
        List<Map<String, ResultColumn>> result = engine.query(select(all()).from(table("TEST")).orderby(column("COL1").asc()));
        assertTrue("Inserted entries not as expected", result.size() == numEntries);
        for(int i = 0 ; i < numEntries ; i++) {
            checkTestEntry(i, result.get(i));
        }
    }

    private EntityEntry getTestEntry(int idx) {
        return entry()
                .set("COL1", 200 + idx)
                .set("COL2", false)
                .set("COL3", 200D + idx)
                .set("COL4", 300L + idx)
                .set("COL5", "ADEUS" + idx)
                .build();
    }

    private void checkTestEntry(int idx, Map<String,ResultColumn> row) {
        assertTrue("COL1 exists", row.containsKey("COL1"));
        assertTrue("COL2 exists", row.containsKey("COL2"));
        assertTrue("COL3 exists", row.containsKey("COL3"));
        assertTrue("COL4 exists", row.containsKey("COL4"));
        assertTrue("COL5 exists", row.containsKey("COL5"));

        EntityEntry expectedEntry = getTestEntry(idx);
        assertEquals("COL1 ok?", (int) expectedEntry.get("COL1"), (int) row.get("COL1").toInt());
        assertFalse("COL2 ok?", row.get("COL2").toBoolean());
        assertEquals("COL3 ok?", (double) expectedEntry.get("COL3"), row.get("COL3").toDouble(), 0);
        assertEquals("COL4 ok?", (long) expectedEntry.get("COL4"), (long) row.get("COL4").toLong());
        assertEquals("COL5  ok?", (String) expectedEntry.get("COL5"), row.get("COL5").toString());
    }

    private static class MockedBatch extends AbstractBatch {

        private List<BatchEntry> failedEntries = new ArrayList<>();

        private MockedBatch(DatabaseEngine de, String name, int batchSize, long batchTimeout, long maxAwaitTimeShutdown) {
            super(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown);
        }

        public static MockedBatch create(final DatabaseEngine de, final String name, final int batchSize, final long batchTimeout,
                                         final long maxAwaitTimeShutdown) {
            final MockedBatch b = new MockedBatch(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown);
            b.start();
            return b;
        }

        @Override
        public void onFlushFailure(BatchEntry[] entries) {
            Collections.addAll(failedEntries, entries);
        }

        public List<BatchEntry> getFailedEntries() {
            return failedEntries;
        }
    }

}
