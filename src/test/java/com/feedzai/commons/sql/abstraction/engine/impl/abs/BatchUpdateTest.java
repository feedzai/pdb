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
import com.feedzai.commons.sql.abstraction.FailureListener;
import com.feedzai.commons.sql.abstraction.batch.AbstractBatch;
import com.feedzai.commons.sql.abstraction.batch.BatchEntry;
import com.feedzai.commons.sql.abstraction.batch.DefaultBatch;
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.engine.AbstractDatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.google.common.util.concurrent.Uninterruptibles;
import mockit.Expectations;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.BOOLEAN;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.DOUBLE;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.LONG;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.STRING;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.all;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.column;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.entry;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.select;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.table;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

    /**
     * The {@link AbstractBatch} used in each test (extracted to a field so that it can be closed after the test).
     */
    protected AbstractBatch batch;

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations();
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

    @BeforeClass
    public static void initStatic() {
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.TRACE);
    }

    @Before
    public void init() throws DatabaseFactoryException {
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
        if (batch != null) {
            batch.destroy();
        }

    }

    /**
     * Checks that batch entries are inserted in the DB after an explicit flush.
     */
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

        batch = DefaultBatch.create(engine, "batchInsertWithDBConnDownTest", numTestEntries + 1, 100000, 1000000);

        // Add entries to batch no flush should take place because numEntries < batch size and batch timeout is huge
        for(int i = 0 ; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Explicit flush
        batch.flush();

        // Check entries are in DB
        checkTestEntriesInDB(numTestEntries);
    }

    /**
     * Checks that batch entries are inserted in the DB after the buffer fills up.
     */
    @Test
    public void batchInsertFlushBySizeTest() throws Exception {
        final int numTestEntries = 5;

        addTestEntity();
        batch = DefaultBatch.create(engine, "batchInsertWithDBConnDownTest", numTestEntries, 100000, 1000000);

        // Add entries to batch, no flush needed because #inserted entries = batch size
        for(int i = 0 ; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Check entries are in DB
        checkTestEntriesInDB(numTestEntries);
    }

    /**
     * Checks that batch entries are inserted in the DB after the buffer fills up.
     * <p>
     * This test creates the necessary batch using the database engine.
     */
    @Test
    public void engineBatchInsertFlushBySizeTest() throws Exception {
        final int numTestEntries = 5;

        addTestEntity();
        engine.getProperties().setProperty(PdbProperties.MAXIMUM_TIME_BATCH_SHUTDOWN, "1000000");
        batch = engine.createBatch(numTestEntries, 100000, "batchInsertWithDBConnDownTest");
        // Add entries to batch, no flush needed because #inserted entries = batch size
        for(int i = 0 ; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Check entries are in DB
        checkTestEntriesInDB(numTestEntries);
    }

    /**
     * Checks that batch entries are inserted in the DB after the flush timeout.
     */
    @Test
    public void batchInsertFlushByTimeTest() throws Exception {
        final int numTestEntries = 5;
        final long batchTimeout = 1000;     // Flush after 1 sec

        addTestEntity();
        batch = DefaultBatch.create(engine, "batchInsertWithDBConnDownTest", numTestEntries + 1, batchTimeout, 1000000);
        for(int i = 0 ; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Wait for flush
        Thread.sleep(batchTimeout + 1000);

        // Check entries are in DB
        checkTestEntriesInDB(numTestEntries);
    }

    /**
     * Checks that batch entries are passed to onFlushFailure on DB errors when the buffer fills up.
     */
    @Test
    public void batchInsertFlushBySizeWithDBErrorTest(@Mocked final DatabaseEngine engine) throws Exception {
        final int numTestEntries = 5;

        final List<BatchEntry> failedEntries = new ArrayList<>();
        addTestEntity();
        batch = MockedBatch.create(
                engine,
                "batchInsertWithDBConnDownTest",
                numTestEntries,
                100000,
                1000000,
                failedEvents -> Collections.addAll(failedEntries, failedEvents)
        );

        // Simulate failures in beginTransaction() for flush to fail
        new Expectations() {{
            engine.beginTransaction(); result = new DatabaseEngineRuntimeException("Error !");
        }};

        // Add entries to batch, no flush needed because #inserted entries = batch size
        for(int i = 0 ; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Check that entries were added to onFlushFailure()
        assertEquals("Entries were added to failed", failedEntries.size(), numTestEntries);
    }

    /**
     * Checks that batch entries are passed to onFlushFailure on DB errors when the batch timeout expires.
     */
    @Test
    public void batchInsertFlushByTimeWithDBErrorTest(@Mocked final DatabaseEngine engine) throws Exception {
        final int numTestEntries = 5;
        final long batchTimeout = 1000;     // Flush after 1 sec

        final List<BatchEntry> failedEntries = new ArrayList<>();
        addTestEntity();
        batch = MockedBatch.create(
                engine,
                "batchInsertWithDBConnDownTest",
                numTestEntries + 1,
                batchTimeout,
                1000000,
                failedEvents -> Collections.addAll(failedEntries, failedEvents)
        );

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
        assertEquals("Entries were added to failed", failedEntries.size(), numTestEntries);
    }

    /**
     * Checks that flushing batch entries retries successfully on recoverable DB errors.
     *
     * @since 2.1.12
     */
    @Test
    public void batchInsertFlushRetryAfterDBErrorTest() throws Exception {
        final int numTestEntries = 5;
        final int numRetries = 2;

        final List<BatchEntry> failedEntries = new ArrayList<>();
        addTestEntity();
        batch = DefaultBatch.create(
                engine,
                "batchInsertFlushRetryAfterDBErrorTest",
                numTestEntries + 1,
                10000,
                1000000,
                failedEvents -> Collections.addAll(failedEntries, failedEvents),
                numRetries,
                200
        );

        final AtomicInteger callCounter = new AtomicInteger(0);

        new MockUp<AbstractDatabaseEngine>() {
            // This mock will fail exceptionally until the last retry.
            @Mock
            void beginTransaction(final Invocation inv) throws DatabaseEngineRuntimeException {
                // The following condition is sufficient since there is one extra regular call before any retry.
                if (callCounter.getAndIncrement() < numRetries) {
                    throw new DatabaseEngineRuntimeException("Error! Try again.");
                }
                inv.proceed();
            }
        };

        for(int i = 0 ; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Explicit flush.
        batch.flush();

        // Check that the correct number of retries took place.
        // Note that the number of retries is the number of calls excluding the first (which was not a retry).
        assertEquals("Flush was retried the correct number of times", numRetries, callCounter.get() - 1);

        // Check that entries were not added to onFlushFailure().
        assertTrue("Entries should not be added to failed", failedEntries.isEmpty());

        // Check entries are in DB.
        checkTestEntriesInDB(numTestEntries);
    }

    /**
     * Checks that flushing batch entries fails after exhausting all configured retries.
     *
     * @since 2.1.12
     */
    @Test
    public void batchInsertFlushAbortAfterExhaustingRetriesTest() throws Exception {
        final int numTestEntries = 5;
        final int numRetries = 2;

        final List<BatchEntry> failedEntries = new ArrayList<>();
        addTestEntity();
        batch = DefaultBatch.create(
                engine,
                "batchInsertFlushRetryAfterDBErrorTest",
                numTestEntries + 1,
                10000,
                1000000,
                failedEvents -> Collections.addAll(failedEntries, failedEvents),
                numRetries,
                200
        );

        final AtomicInteger callCounter = new AtomicInteger(0);

        new MockUp<AbstractDatabaseEngine>() {
            // This mock will fail exceptionally for all configured retries (but would succeed if called after that).
            @Mock
            void beginTransaction(final Invocation inv) throws DatabaseEngineRuntimeException {
                // The following condition is necessary since there is one extra regular call before any retry.
                if (callCounter.getAndIncrement() < numRetries + 1) {
                    throw new DatabaseEngineRuntimeException("Error! Try again.");
                }
                inv.proceed();
            }
        };

        for(int i = 0 ; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Explicit flush.
        batch.flush();

        // Check that the correct number of retries took place.
        // Note that the number of retries is the number of calls excluding the first (which was not a retry).
        assertEquals("Flush was retried the correct number of times", numRetries, callCounter.get() - 1);

        // Check that entries were added to onFlushFailure().
        assertEquals("Entries were added to failed", numTestEntries, failedEntries.size());
    }

    /**
     * Ensures that the batch transaction is rolled back when the flush fails.
     *
     * @since 2.1.5
     */
    @Test
    public void flushFreesConnectionOnFailure() throws DatabaseEngineException {
        batch = DefaultBatch.create(engine, "flushFreesConnectionOnFailure", 2, 1000, 1000000);
        batch.add("unknown_table", entry().build()); // This will only fail when flushing
        batch.flush();
        assertFalse("Flush failed but the transaction is still active", engine.isTransactionActive());
    }

    /**
     * Tests that {@link AbstractBatch#flush(boolean)} can be synchronous and waits for previous flush calls.
     *
     * @throws DatabaseEngineException If the operations on the engine fail.
     * @since 2.1.6
     */
    @Test(timeout = 30000)
    public void testFlushBatchSync() throws DatabaseEngineException, InterruptedException {
        final AtomicInteger transactions = new AtomicInteger();
        final CountDownLatch firstFlushStartedLatch = new CountDownLatch(1);
        final CountDownLatch firstFlushFinishedLatch = new CountDownLatch(1);

        // mock the begin transaction to force waiting to cause the first flush to wait for others
        new MockUp<AbstractDatabaseEngine>() {
            @Mock
            void beginTransaction(final Invocation inv) throws DatabaseEngineRuntimeException {
                transactions.incrementAndGet();
                firstFlushStartedLatch.countDown();

                /*
                  on the first invocation, wait for the first flush to complete (only 1 invocation is expected);
                  either the second or the third flush should complete, since this one is blocked and the others
                   shouldn't get here because they don't have data to persist in the DB
                 */
                if (inv.getInvocationCount() == 1) {
                    Uninterruptibles.awaitUninterruptibly(firstFlushFinishedLatch);
                }

                inv.proceed();
            }
        };

        final DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING).build();

        engine.addEntity(entity);

        // create a batch with huge batchTimeout, so that it doesn't automatically flush
        batch = DefaultBatch.create(engine, "testFlushBatchSync", 5, 1000000L, 1000);
        batch.add("TEST", entry().set("COL1", 1).build());

        final List<String> resultOrder = Collections.synchronizedList(new ArrayList<>());
        final ExecutorService pool = Executors.newCachedThreadPool();

        // the first flush should collect the data to flush. will be the second to finish because third will not be blocking and will not have data.
        pool.submit(() -> {
            batch.flush();
            resultOrder.add("first");
        });

        // make sure that second flush doesn't start before the first. Should not start a transaction because the data was cleaned up by first flush.
        firstFlushStartedLatch.await();

        pool.submit(() -> {
            batch.flush(true);
            resultOrder.add("second");
            firstFlushFinishedLatch.countDown();
        });

        // this should be in fact the first to finish because is not blocking and there is no data to flush. Should not even start a transaction.
        pool.submit(() -> {
            batch.flush(false);
            resultOrder.add("third");
            firstFlushFinishedLatch.countDown();
        });

        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        assertThat(transactions)
                .as("only 1 transaction should have been really executed")
                .hasValue(1);

        assertThat(resultOrder)
                .as("all flush operations should have completed")
                .containsExactlyInAnyOrder("third", "first", "second")
                .as("the third flush should have completed before all others, since it was not sync, and didn't have data in the batch")
                .startsWith("third");
    }

    /**
     * Tests that there is no race condition between the {@link AbstractBatch#destroy()} and {@link AbstractBatch#run()}
     * methods.
     * This is a regression test for PULSEDEV-18139, where a race condition was causing the scheduler to attempt to
     * call run while it another thread was already inside `destroy` but had not yet called shutdown on the scheduler.
     * Since those two methods were synchronized, the `run` would not finish while destroy was waiting for all tasks in
     * the Executor to finish.
     * For this test to properly work it is critical that the batch is configured to wait more for the scheduler termination
     * than the test timeout.
     *
     * @since 2.1.10
     * @throws DatabaseEngineException If the operations on the engine fail.
     */
    @Test(timeout = 30000)
    public void testBatchRunDestroyRace() throws DatabaseEngineException {
        final DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING).build();

        engine.addEntity(entity);


        for (int i = 0; i < 40; i++) {
            // The maxAwaitTimeShutdown parameter must be larger than the test timeout.
            final MockedBatch batch = MockedBatch.create(engine, "test", 5, 10L, 50000);
            batch.add("TEST", entry().set("COL1", 1).build());

            // Call `destroy` which will wait, if the data race occurs, for more than the test timeout
            batch.destroy();
        }
    }

    /**
     * Create test table.
     */
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

    /**
     * Creates a test row with values dependent on its position.
     *
     * @param idx  The row position.
     * @return     The test row.
     */
    private EntityEntry getTestEntry(int idx) {
        return entry()
                .set("COL1", 200 + idx)
                .set("COL2", false)
                .set("COL3", 200D + idx)
                .set("COL4", 300L + idx)
                .set("COL5", "ADEUS" + idx)
                .build();
    }

    /**
     * Checks that the test table has a given number of rows and that each row corresponds
     * to the row generated with getTestEntry().
     *
     * @param numEntries  The number of entries
     * @throws DatabaseEngineException If the operations on the engine fail.
     */
    private void checkTestEntriesInDB(int numEntries) throws DatabaseEngineException {
        List<Map<String, ResultColumn>> result = engine.query(select(all()).from(table("TEST")).orderby(column("COL1").asc()));
        assertEquals("Inserted entries not as expected", numEntries, result.size());
        for(int i = 0 ; i < numEntries ; i++) {
            checkTestEntry(i, result.get(i));
        }
    }

    /**
     * Checks that a DB row in a given position matches the test row for that position.
     *
     * @param idx   The position.
     * @param row   The DB row.
     */
    private void checkTestEntry(int idx, Map<String,ResultColumn> row) {
        assertTrue("COL1 exists", row.containsKey("COL1"));
        assertTrue("COL2 exists", row.containsKey("COL2"));
        assertTrue("COL3 exists", row.containsKey("COL3"));
        assertTrue("COL4 exists", row.containsKey("COL4"));
        assertTrue("COL5 exists", row.containsKey("COL5"));

        EntityEntry expectedEntry = getTestEntry(idx);
        assertEquals("COL1 ok?", expectedEntry.get("COL1"), row.get("COL1").toInt());
        assertEquals("COL2 ok?", expectedEntry.get("COL2"), row.get("COL2").toBoolean());
        assertEquals("COL3 ok?", (double) expectedEntry.get("COL3"), row.get("COL3").toDouble(), 0);
        assertEquals("COL4 ok?", expectedEntry.get("COL4"), row.get("COL4").toLong());
        assertEquals("COL5 ok?", expectedEntry.get("COL5"), row.get("COL5").toString());
    }

    /**
     * Concrete abstract batch that just collects the entries passed to onFlushFailure,
     * so it can be checked that onFlushFailure is invoked as expected.
     */
    private static class MockedBatch extends AbstractBatch {

        /**
         * Duration of the sleep in the beginning of the destroy method.
         */
        static final long PRE_DESTROY_SLEEP_DURATION = 500L;

        private MockedBatch(DatabaseEngine de, String name, int batchSize, long batchTimeout, long maxAwaitTimeShutdown, FailureListener listener) {
            super(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown, listener);
        }

        private MockedBatch(DatabaseEngine de, String name, int batchSize, long batchTimeout, long maxAwaitTimeShutdown) {
            super(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown);
        }

        public static MockedBatch create(final DatabaseEngine de, final String name, final int batchSize, final long batchTimeout,
                                         final long maxAwaitTimeShutdown, final FailureListener listener) {
            final MockedBatch b = new MockedBatch(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown, listener);
            b.start();
            return b;
        }

        public static MockedBatch create(final DatabaseEngine de, final String name, final int batchSize, final long batchTimeout,
                                         final long maxAwaitTimeShutdown) {
            final MockedBatch b = new MockedBatch(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown);
            b.start();
            return b;
        }

        @Override
        public synchronized void destroy() {
            Uninterruptibles.sleepUninterruptibly(PRE_DESTROY_SLEEP_DURATION, TimeUnit.MILLISECONDS);
            super.destroy();
        }
    }

}
