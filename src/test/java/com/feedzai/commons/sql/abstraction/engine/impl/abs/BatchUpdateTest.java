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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.assertj.core.api.ObjectAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import com.feedzai.commons.sql.abstraction.batch.AbstractBatch;
import com.feedzai.commons.sql.abstraction.batch.AbstractBatchConfig;
import com.feedzai.commons.sql.abstraction.batch.BatchEntry;
import com.feedzai.commons.sql.abstraction.batch.PdbBatch;
import com.feedzai.commons.sql.abstraction.batch.impl.DefaultBatch;
import com.feedzai.commons.sql.abstraction.batch.impl.DefaultBatchConfig;
import com.feedzai.commons.sql.abstraction.batch.impl.MultithreadedBatchConfig;
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.engine.AbstractDatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.feedzai.commons.sql.abstraction.listeners.BatchListener;
import com.feedzai.commons.sql.abstraction.listeners.MetricsListener;

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
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.MAX_NUMBER_OF_RETRIES;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.RETRY_INTERVAL;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for {@link PdbBatch} implementations.
 *
 * @author Paulo Leitao (paulo.leitao@feedzai.com)
 * @since 2.1.4
 */
@RunWith(Parameterized.class)
public class BatchUpdateTest {

    protected DatabaseEngine engine;
    protected Properties properties;

    /**
     * The {@link PdbBatch} used in each test (extracted to a field so that it can be closed after the test).
     */
    protected PdbBatch batch;

    @Parameterized.Parameters
    public static List<Object[]> data() throws Exception {
        final Set<DatabaseConfiguration> databaseConfigurations = new HashSet<>(DatabaseTestUtil.loadConfigurations());
        final Set<Supplier<AbstractBatchConfig.Builder<?, ?, ?>>> batchConfigs = ImmutableSet.of(
                DefaultBatchConfig::builder,
                MultithreadedBatchConfig::builder,
                () -> MultithreadedBatchConfig.builder().withNumberOfThreads(3)
        );

        return Sets.cartesianProduct(databaseConfigurations, batchConfigs)
                .stream()
                .map(List::toArray)
                .collect(Collectors.toList());
    }

    @Parameterized.Parameter
    public DatabaseConfiguration dbConfig;

    @Parameterized.Parameter(1)
    public Supplier<AbstractBatchConfig.Builder<?, ?, ?>> batchConfigBuilderSupplier;

    @BeforeClass
    public static void initStatic() {
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.TRACE);
    }

    @Before
    public void init() throws DatabaseFactoryException {
        properties = new Properties() {

            {
                setProperty(JDBC, dbConfig.jdbc);
                setProperty(USERNAME, dbConfig.username);
                setProperty(PASSWORD, dbConfig.password);
                setProperty(ENGINE, dbConfig.engine);
                setProperty(SCHEMA_POLICY, "drop-create");
                setProperty(MAX_NUMBER_OF_RETRIES, "5");
                setProperty(RETRY_INTERVAL, "1000");
            }
        };

        engine = DatabaseFactory.getConnection(properties);
    }

    @After
    public void cleanup() throws Exception {
        engine.close();
        if (batch != null) {
            batch.close();
        }
    }

    /**
     * Checks that after creating a batch the connection is still usable.
     * <p>
     * This is a regression test for some issues that were detected previously after batch creation (see test comments).
     */
    @Test
    public void createBatchLeavesUsableConnectionTest() throws Exception {
        final AbstractBatchConfig<?, ?> batchConfig = batchConfigBuilderSupplier.get()
                .withName("createBatchLeavesUsableConnectionTest")
                .withMaxAwaitTimeShutdown(Duration.ofSeconds(10))
                .build();

        batch = engine.createBatch(batchConfig);
        batch.close();

        /*
         Previously AbstractDatabaseEngine was using a child injector to create a new batch.
         The problem with this approach was that the bindings in the child injector would get propagated to the parent,
         resulting in a CreationException due to some instance being bound multiple times (it was already in the parent).
         */
        assertThatCode(() -> batch = engine.createBatch(batchConfig))
                .doesNotThrowAnyException();
        batch.close();

        /*
         Previously AbstractDatabaseEngine was binding "toInstance", which means that Guice would reinject into existing
         instances. The result was that a new AbstractTranslator instance was being created with empty properties, such
         that it wouldn't have DEFAULT_VARCHAR_SIZE, and DbColumns without one defined would get translated as
         VARCHAR(null), failing table creation.
         */
        assertThatCode(this::addTestEntity)
                .doesNotThrowAnyException();
    }

    /**
     * Checks that batch entries are inserted in the DB after an explicit flush.
     */
    @Test
    public void batchInsertExplicitFlushTest() throws Exception {
        final int numTestEntries = 5;

        addTestEntity();

        batch = engine.createBatch(batchConfigBuilderSupplier.get()
                .withName("batchInsertExplicitFlushTest")
                .withBatchSize(numTestEntries + 1)
                .withBatchTimeout(Duration.ofSeconds(100))
                .withMaxAwaitTimeShutdown(Duration.ofSeconds(1000))
                .build()
        );

        // Add entries to batch no flush should take place because numEntries < batch size and batch timeout is huge
        for (int i = 0; i < numTestEntries; i++) {
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
        final TestBatchListener batchListener = new TestBatchListener();

        addTestEntity();

        batch = engine.createBatch(batchConfigBuilderSupplier.get()
                .withName("batchInsertFlushBySizeTest")
                .withBatchSize(numTestEntries)
                .withBatchTimeout(Duration.ofSeconds(100))
                .withMaxAwaitTimeShutdown(Duration.ofSeconds(1000))
                .withBatchListener(batchListener)
                .build()
        );

        // Add entries to batch, no flush needed because #inserted entries = batch size
        for (int i = 0; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // wait for success of flush triggered by batch size, at most 5 seconds
        batchListener.succeeded.poll(5, TimeUnit.SECONDS);

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

        batch = engine.createBatch(batchConfigBuilderSupplier.get()
                .withName("batchInsertFlushByTimeTest")
                .withBatchSize(numTestEntries + 1)
                .withBatchTimeout(Duration.ofMillis(batchTimeout))
                .withMaxAwaitTimeShutdown(Duration.ofSeconds(1000))
                .build()
        );

        for (int i = 0; i < numTestEntries; i++) {
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
    @Test(timeout = 30000)
    public void batchInsertFlushBySizeWithDBErrorTest() throws Exception {
        final int numTestEntries = 5;
        final TestBatchListener batchListener = new TestBatchListener();

        addTestEntity();

        batch = engine.createBatch(batchConfigBuilderSupplier.get()
                .withName("batchInsertFlushBySizeWithDBErrorTest")
                .withBatchSize(numTestEntries)
                .withBatchTimeout(Duration.ofSeconds(100))
                .withMaxAwaitTimeShutdown(Duration.ofSeconds(1000))
                .withBatchListener(batchListener)
                .build()
        );

        // Simulate failures in beginTransaction() for flush to fail
        new MockUp<AbstractDatabaseEngine>(AbstractDatabaseEngine.class) {
            @Mock
            void beginTransaction() throws DatabaseEngineRuntimeException {
                throw new DatabaseEngineRuntimeException("Error !");
            }
        };

        // Add entries to batch, no flush needed because #inserted entries = batch size
        for (int i = 0; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Check that entries were added to onFlushFailure()
        checkFailedEntries(batchListener, numTestEntries);
    }

    /**
     * Checks that batch entries are passed to onFlushFailure on DB errors when the batch timeout expires.
     */
    @Test(timeout = 30000)
    public void batchInsertFlushByTimeWithDBErrorTest() throws Exception {
        final int numTestEntries = 5;
        final long batchTimeout = 1000;
        final TestBatchListener batchListener = new TestBatchListener();

        addTestEntity();

        batch = engine.createBatch(batchConfigBuilderSupplier.get()
                .withName("batchInsertFlushByTimeWithDBErrorTest")
                .withBatchSize(numTestEntries + 1)
                .withBatchTimeout(Duration.ofMillis(batchTimeout))
                .withMaxAwaitTimeShutdown(Duration.ofSeconds(1000))
                .withBatchListener(batchListener)
                .build()
        );

        // Simulate failures in beginTransaction() for flush to fail
        new MockUp<AbstractDatabaseEngine>(AbstractDatabaseEngine.class) {
            @Mock
            void beginTransaction() throws DatabaseEngineRuntimeException {
                throw new DatabaseEngineRuntimeException("Error !");
            }
        };

        for (int i = 0; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Wait for flush
        Thread.sleep(batchTimeout + 1000);

        // Check that entries were added to onFlushFailure()
        checkFailedEntries(batchListener, numTestEntries);
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
        final TestBatchListener batchListener = new TestBatchListener();

        addTestEntity();

        batch = engine.createBatch(batchConfigBuilderSupplier.get()
                .withName("batchInsertFlushRetryAfterDBErrorTest")
                .withBatchSize(numTestEntries + 1)
                .withBatchTimeout(Duration.ofSeconds(10))
                .withMaxAwaitTimeShutdown(Duration.ofSeconds(1000))
                .withBatchListener(batchListener)
                .withMaxFlushRetries(numRetries)
                .withFlushRetryDelay(Duration.ofMillis(200))
                .build()
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

        for (int i = 0; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Explicit flush.
        batch.flush();

        // Check that the correct number of retries took place.
        // Note that the number of retries is the number of calls excluding the first (which was not a retry).
        assertEquals("Flush was retried the correct number of times", numRetries, callCounter.get() - 1);

        // Check that entries were not added to onFlushFailure().
        assertTrue("Entries should not be added to failed", batchListener.failed.isEmpty());

        // Check that all entries succeeded
        assertEquals("Entries should have all succeeded to be persisted", numTestEntries, batchListener.succeeded.size());

        // Check entries are in DB.
        checkTestEntriesInDB(numTestEntries);
    }

    /**
     * Checks that flushing batch entries retries successfully on recoverable DB connection failure.
     */
    @Test
    public void batchInsertFlushRetryAfterDBConnectionErrorTest() throws Exception {
        final TestBatchListener batchListener = new TestBatchListener();
        final AtomicBoolean allowConnection = new AtomicBoolean(true);
        final ThreadLocal<Integer> failedConnectionsCount = ThreadLocal.withInitial(() -> 0);

        addTestEntity();

        // Set a big batch size and timeout, flushes are going to be explicitly triggered in this test
        batch = engine.createBatch(batchConfigBuilderSupplier.get()
                .withName("batchInsertFlushRetryAfterDBConnectionErrorTest")
                .withBatchSize(1000)
                .withBatchTimeout(Duration.ofSeconds(1000))
                .withMaxAwaitTimeShutdown(Duration.ofSeconds(1000))
                .withBatchListener(batchListener)
                .build()
        );


        new MockUp<AbstractDatabaseEngine>() {
            @Mock
            void connect(final Invocation inv) throws Exception {
                if (allowConnection.get()) {
                    failedConnectionsCount.set(0);
                    inv.proceed();
                } else {
                    final int count = failedConnectionsCount.get();
                    if (count == 6) {
                        allowConnection.set(true);
                    }
                    failedConnectionsCount.set(count + 1);
                    throw new SQLException("Could not connect");
                }
            }

            @Mock
            public Connection getConnection(final Invocation inv) throws Exception {
                if (!allowConnection.get() && inv.<AbstractDatabaseEngine>getInvokedInstance().checkConnection(false)) {
                    inv.<Connection>proceed().close();
                }
                return inv.proceed();
            }

            @Mock
            public DatabaseEngine duplicate(final Invocation inv, final Properties mergeProperties, final boolean copyEntities) {
                // Temporarily ignore the test blocking the connection, so that it can duplicate
                final boolean isAllow = allowConnection.get();
                allowConnection.set(true);
                try {
                    return inv.proceed();
                } finally {
                    failedConnectionsCount.set(0);
                    allowConnection.set(isAllow);
                }
            }
        };

        // 1. perform a simple add with a connection ok
        batch.add("TEST", getTestEntry(0));
        batch.flush();
        assertTrue("Check that there were no failure results", batchListener.failed.isEmpty());

        // 2. force an error disallowing the connections to be fetched and disallow new connections to force to exhaust the retry mechanism
        allowConnection.set(false);

        final EntityEntry failTestEntry = getTestEntry(10);
        batch.add("TEST", failTestEntry);
        batch.flush();
        assertEquals("Check that entry was sent to the failure batch", 1, batchListener.failed.size());
        final BatchEntry entry = batchListener.failed.take();
        assertEquals("table name ok?", "TEST", entry.getTableName());
        assertEquals("test entry ok?", failTestEntry, entry.getEntityEntry());

        // 3. restore the connection and make sure that the code tries to reconnect and doesn't fail the batch
        allowConnection.set(true);
        batch.add("TEST", getTestEntry(1));
        batch.flush();
        assertTrue("Check that there were no failure results", batchListener.failed.isEmpty());

        // Check entries in DB.
        checkTestEntriesInDB(2);
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
        final TestBatchListener batchListener = new TestBatchListener();

        addTestEntity();

        batch = engine.createBatch(batchConfigBuilderSupplier.get()
                .withName("batchInsertFlushAbortAfterExhaustingRetriesTest")
                .withBatchSize(numTestEntries + 1)
                .withBatchTimeout(Duration.ofSeconds(10))
                .withMaxAwaitTimeShutdown(Duration.ofSeconds(1000))
                .withBatchListener(batchListener)
                .withMaxFlushRetries(numRetries)
                .withFlushRetryDelay(Duration.ofMillis(200))
                .build()
        );

        final AtomicInteger callCounter = new AtomicInteger(0);

        new MockUp<AbstractDatabaseEngine>(AbstractDatabaseEngine.class) {
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

        for (int i = 0; i < numTestEntries; i++) {
            batch.add("TEST", getTestEntry(i));
        }

        // Explicit flush.
        batch.flush();

        // Check that the correct number of retries took place.
        // Note that the number of retries is the number of calls excluding the first (which was not a retry).
        assertEquals("Flush was retried the correct number of times", numRetries, callCounter.get() - 1);

        // Check that entries were added to onFlushFailure().
        assertEquals("Entries were added to failed", numTestEntries, batchListener.failed.size());
    }

    /**
     * Checks that the {@link MetricsListener} reports metrics events correctly.
     */
    @Test
    public void metricsListenerTest() throws Exception {
        final TestMetricsListener metricsListener = new TestMetricsListener();
        final AtomicBoolean allowFlush = new AtomicBoolean(true);

        addTestEntity();

        // Set a big batch size and timeout, flushes are going to be explicitly triggered in this test
        batch = engine.createBatch(batchConfigBuilderSupplier.get()
                .withName("metricsListenerTest")
                .withBatchSize(1000)
                .withBatchTimeout(Duration.ofSeconds(1000))
                .withMaxAwaitTimeShutdown(Duration.ofSeconds(1000))
                .withMetricsListener(metricsListener)
                .build()
        );

        // Simulate failures in beginTransaction() for flush to fail
        new MockUp<AbstractDatabaseEngine>(AbstractDatabaseEngine.class) {
            @Mock
            void beginTransaction(final Invocation inv) throws DatabaseEngineRuntimeException {
                if (allowFlush.get()) {
                    inv.proceed();
                } else {
                    throw new DatabaseEngineRuntimeException("Error !");
                }
            }
        };

        final List<EntityEntry> successEntries = ImmutableList.of(getTestEntry(0), getTestEntry(1));
        final List<EntityEntry> failEntries = ImmutableList.of(getTestEntry(2), getTestEntry(3));

        // First flush is empty, should only report flush triggered and finished, not started
        batch.flush();

        // Second flush should succeed: should report 2 entries added, flush triggered, started and finished with those entries
        for (final EntityEntry successEntry : successEntries) {
            batch.add("TEST", successEntry);
        }
        batch.flush();

        // Third flush should fail: should report 2 entries added, flush triggered, started and finished with those entries
        allowFlush.set(false);
        for (final EntityEntry failEntry : failEntries) {
            batch.add("TEST", failEntry);
        }
        batch.flush();

        assertEquals("All added entries should be counted", 4, metricsListener.addedCounter.get());

        assertEquals("All flushes should be counted", 3, metricsListener.flushTriggerCounter.get());

        assertThat(metricsListener.startedMetrics)
                .as("All started flushes should be counted")
                .hasSize(2)
                .as("All started flushes should have 2 entries")
                .allMatch(entry -> entry.successfulEntriesCount == 2 && entry.failedEntriesCount == 0);

        assertEquals("All finished flushes should be counted", 3, metricsListener.finishedMetrics.size());
        assertThat(metricsListener.finishedMetrics.take())
                .as("First finished metric should not contain any entries (empty flush).")
                .matches(entry -> entry.successfulEntriesCount == 0 && entry.failedEntriesCount == 0);
        assertThat(metricsListener.finishedMetrics.take())
                .as("Second finished metric should only contain 2 successful entries.")
                .matches(entry -> entry.successfulEntriesCount == 2 && entry.failedEntriesCount == 0);
        assertThat(metricsListener.finishedMetrics.take())
                .as("Third finished metric should only contain 2 failed entries (flush failed on purpose).")
                .matches(entry -> entry.successfulEntriesCount == 0 && entry.failedEntriesCount == 2);
    }

    /**
     * Ensures that the batch transaction is rolled back when the flush fails.
     *
     * @since 2.1.5
     */
    @Test
    public void flushFreesConnectionOnFailure() throws Exception {
        final LinkedBlockingQueue<DatabaseEngine> dbEngines = new LinkedBlockingQueue<>();

        new MockUp<AbstractDatabaseEngine>() {
            @Mock
            void connect(final Invocation inv) {
                dbEngines.add(inv.getInvokedInstance());
                inv.proceed();
            }
        };

        batch = engine.createBatch(batchConfigBuilderSupplier.get()
                .withName("flushFreesConnectionOnFailure")
                .withBatchSize(2)
                .withBatchTimeout(Duration.ofSeconds(1))
                .withMaxAwaitTimeShutdown(Duration.ofSeconds(1000))
                .build()
        );

        batch.add("unknown_table", entry().build()); // This will only fail when flushing
        batch.flush();

        assertFalse("Flush failed but the transaction is still active", engine.isTransactionActive());

        dbEngines.forEach(dbEngine ->
                assertFalse("Flush failed but the transaction is still active in one of the internal engines", dbEngine.isTransactionActive()));
    }

    /**
     * Tests that {@link AbstractBatch#flush(boolean)} can be synchronous and waits for previous flush calls.
     *
     * @throws DatabaseEngineException If the operations on the engine fail.
     * @since 2.1.6
     */
    @Test(timeout = 30000)
    public void testFlushBatchSync() throws Exception {
        assumeTrue("Test only applies to DefaultBatch", batchConfigBuilderSupplier instanceof DefaultBatchConfig.DefaultBatchConfigBuilder);

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

        addTestEntity();

        // create a batch with huge batchTimeout, so that it doesn't automatically flush
        final DefaultBatch batch = engine.createBatch((DefaultBatchConfig) batchConfigBuilderSupplier.get()
                .withName("testFlushBatchSync")
                .withBatchSize(5)
                .withBatchTimeout(Duration.ofSeconds(1000))
                .withMaxAwaitTimeShutdown(Duration.ofSeconds(1))
                .build()
        );
        this.batch = batch;

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
     * Tests that there is no race condition between the {@link AbstractBatch#destroy()} (called by
     * {@link AbstractBatch#close()}) and {@link AbstractBatch#run()} methods.
     * <p>
     * This is a regression test for PULSEDEV-18139, where a race condition was causing the scheduler to attempt to
     * call run while it another thread was already inside `destroy` but had not yet called shutdown on the scheduler.
     * Since those two methods were synchronized, the `run` would not finish while destroy was waiting for all tasks in
     * the Executor to finish.
     * For this test to properly work it is critical that the batch is configured to wait more for the scheduler termination
     * than the test timeout.
     *
     * @throws DatabaseEngineException If the operations on the engine fail.
     * @since 2.1.10
     */
    @Test(timeout = 30000)
    public void testBatchRunDestroyRace() throws Exception {
        addTestEntity();

        for (int i = 0; i < 40; i++) {
            // The maxAwaitTimeShutdown parameter must be larger than the test timeout.
            final PdbBatch batch = engine.createBatch(batchConfigBuilderSupplier.get()
                    .withName("testBatchRunDestroyRace")
                    .withBatchSize(5)
                    .withBatchTimeout(Duration.ofMillis(10))
                    .withMaxAwaitTimeShutdown(Duration.ofSeconds(50))
                    .build()
            );

            batch.add("TEST", entry().set("COL1", 1).build());

            // Call `destroy` which will wait, if the data race occurs, for more than the test timeout
            Thread.sleep(200);
            batch.close();
        }
    }

    /**
     * Tests if a batch with entries having duplicate keys fails when flushing.
     *
     * @throws Exception if any operations on the batch fail.
     */
    @Test
    public void batchInsertDuplicateFlushWithDBErrorTest() throws Exception {
        final TestBatchListener batchListener = new TestBatchListener();
        final int numTestEntries = 2;

        addTestEntityWithPrimaryKey();

        batch = engine.createBatch(batchConfigBuilderSupplier.get()
                                                             .withName("batchInsertDuplicateFlushWithDBErrorTest")
                                                             .withBatchSize(numTestEntries + 1)
                                                             .withBatchTimeout(Duration.ofSeconds(100))
                                                             .withMaxAwaitTimeShutdown(Duration.ofSeconds(1000))
                                                             .withBatchListener(batchListener)
                                                             .build()
        );

        // Add entries to batch, no flush should take place because numTestEntries < batch size and batch timeout is huge
        final int idx = 0;
        final EntityEntry testEntry = getTestEntry(idx);
        batch.add("TEST", testEntry);
        batch.add("TEST", testEntry);

        // Explicit flush
        batch.flush();

        // Check that entries were added to onFlushFailure()
        checkFailedDuplicateEntries(batchListener, numTestEntries, idx);
    }

    /**
     * Tests if a batch with entries having duplicate ignores when there is an error.
     *
     * @throws Exception if any operations on the batch fail.
     */
    @Test
    public void batchInsertOnIgnoreDuplicateFlushTest() throws Exception {
        final TestBatchListener batchListener = new TestBatchListener();
        final int numTestEntries = 2;

        addTestEntityWithPrimaryKey();

        final DefaultBatch batch = engine.createBatch(DefaultBatchConfig.builder()
                                                             .withName("batchInsertOnIgnoreDuplicateFlushTest")
                                                             .withBatchSize(numTestEntries + 1)
                                                             .withBatchTimeout(Duration.ofSeconds(100))
                                                             .withMaxAwaitTimeShutdown(Duration.ofSeconds(1000))
                                                             .withBatchListener(batchListener)
                                                             .build()
        );

        // Add entries to batch, no flush should take place because numTestEntries < batch size and batch timeout is huge
        final int idx = 0;
        final EntityEntry testEntry = getTestEntry(idx);
        batch.add("TEST", testEntry);
        batch.add("TEST", testEntry);

        // Explicit flush, but ignoring the duplicate entries in the batch.
        batch.flushIgnore();

        // Check that entries were not added to onFlushFailure().
        assertTrue("Entries should not be added to failed", batchListener.failed.isEmpty());

        // Check that all entries succeeded
        assertEquals("Entries should have all succeeded to be persisted", numTestEntries, batchListener.succeeded.size());

        // Considering they are the same entry, only one should be inserted in the database.
        checkTestEntriesInDB(1);
    }

    /**
     * Create test table.
     */
    private void addTestEntity() throws DatabaseEngineException {
        final DbEntity entity = dbEntity()
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
     * @param idx The row position.
     * @return The test row.
     */
    private EntityEntry getTestEntry(final int idx) {
        return entry()
                .set("COL1", 200 + idx)
                .set("COL2", false)
                .set("COL3", 200D + idx)
                .set("COL4", 300L + idx)
                .set("COL5", "ADEUS" + idx)
                .build();
    }

    /**
     * Create test table with primary key defined.
     */
    private void addTestEntityWithPrimaryKey() throws DatabaseEngineException {
        final DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1")
                .build();

        engine.addEntity(entity);
    }

    /**
     * Helper method to check that the given number of entries appear on the {@link TestBatchListener} failed list and
     * correspond to the test entries originally added to the batch.
     *
     * @param batchListener The mocked batch listener used in the batch.
     * @param numEntries    The expected number of test entries in the failed list.
     * @throws InterruptedException If this check is interrupted while waiting.
     */
    private void checkFailedEntries(final TestBatchListener batchListener, final int numEntries) throws InterruptedException {
        final List<BatchEntry> failedEntries = new ArrayList<>();

        while (true) {
            final BatchEntry entry = batchListener.failed.poll(5, TimeUnit.SECONDS);
            if (entry == null) {
                break;
            }

            failedEntries.add(entry);
        }

        assertEquals("The total number of failed entries should match the entries added to the batch.", numEntries, failedEntries.size());

        failedEntries.sort(Comparator.comparingInt(entry -> (int) entry.getEntityEntry().get("COL1")));

        for (int i = 0; i < failedEntries.size(); i++) {
            final ObjectAssert<BatchEntry> batchEntryAssert = assertThat(failedEntries.get(i));

            batchEntryAssert.extracting(BatchEntry::getTableName)
                    .as("Failed entry '%s' should have the correct table name.", i)
                    .isEqualTo("TEST");

            batchEntryAssert.extracting(BatchEntry::getEntityEntry)
                    .as("Failed entry '%s' should match the entry added to the batch.", i)
                    .isEqualTo(getTestEntry(i));
        }
    }

    /**
     * Checks that the test table has a given number of rows and that each row corresponds
     * to the row generated with getTestEntry().
     *
     * @param numEntries The number of entries
     * @throws DatabaseEngineException If the operations on the engine fail.
     */
    private void checkTestEntriesInDB(int numEntries) throws DatabaseEngineException {
        final List<Map<String, ResultColumn>> result = engine.query(select(all()).from(table("TEST")).orderby(column("COL1").asc()));

        assertEquals("Inserted entries not as expected", numEntries, result.size());

        for (int i = 0; i < numEntries; i++) {
            checkTestEntry(i, result.get(i));
        }
    }

    /**
     * Checks that a DB row in a given position matches the test row for that position.
     *
     * @param idx The position.
     * @param row The DB row.
     */
    private void checkTestEntry(int idx, Map<String, ResultColumn> row) {
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
     * Helper method to check that the given number of entries appear on the {@link TestBatchListener} failed list and
     * correspond to the duplicate test entries originally added to the batch.
     *
     * @param batchListener         The mocked batch listener used in the batch.
     * @param numEntries            The expected number of test entries in the failed list.
     * @param idx                   The duplicated id.
     * @throws InterruptedException If this check is interrupted while waiting.
     */
    private void checkFailedDuplicateEntries(final TestBatchListener batchListener, final int numEntries, final int idx) throws InterruptedException {
        final List<BatchEntry> failedEntries = new ArrayList<>();

        while (true) {
            final BatchEntry entry = batchListener.failed.poll(5, TimeUnit.SECONDS);
            if (entry == null) {
                break;
            }

            failedEntries.add(entry);
        }

        assertEquals("The total number of failed entries should match the entries added to the batch.", numEntries, failedEntries.size());
        for (int i = 0; i < failedEntries.size(); i++) {
            final ObjectAssert<BatchEntry> batchEntryAssert = assertThat(failedEntries.get(i));

            batchEntryAssert.extracting(BatchEntry::getTableName)
                            .as("Failed entry '%s' should have the correct table name.", i)
                            .isEqualTo("TEST");

            batchEntryAssert.extracting(BatchEntry::getEntityEntry)
                            .as("Failed entry '%s' should match the entry added to the batch.", i)
                            .isEqualTo(getTestEntry(idx));
        }
    }

    /**
     * A {@link BatchListener} for the tests.
     */
    private static class TestBatchListener implements BatchListener {

        /**
         * The entries that succeeded to be persisted.
         */
        final BlockingQueue<BatchEntry> succeeded = new LinkedBlockingQueue<>();

        /**
         * The entries that failed to be persisted.
         */
        final BlockingQueue<BatchEntry> failed = new LinkedBlockingQueue<>();

        @Override
        public void onFailure(final BatchEntry[] rowsFailed) {
            Collections.addAll(this.failed, rowsFailed);
        }

        @Override
        public void onSuccess(final BatchEntry[] rowsSucceeded) {
            Collections.addAll(this.succeeded, rowsSucceeded);
        }
    }

    /**
     * A {@link MetricsListener} for the tests.
     */
    private static class TestMetricsListener implements MetricsListener {

        /**
         * Class to hold flush metrics values.
         */
        static class FlushMetricsEntry {

            /**
             * Number of entries successfully flushed/added.
             */
            int successfulEntriesCount;

            /**
             * Number of entries that failed to be flushed.
             */
            int failedEntriesCount;

            /**
             * Constructor for a new metric entry.
             *
             * @param successfulEntriesCount Number of entries successfully flushed/added.
             * @param failedEntriesCount     Number of entries that failed to be flushed.
             */
            public FlushMetricsEntry(final int successfulEntriesCount, final int failedEntriesCount) {
                this.successfulEntriesCount = successfulEntriesCount;
                this.failedEntriesCount = failedEntriesCount;
            }
        }

        /**
         * The counter for "entry added" events.
         */
        final AtomicInteger addedCounter = new AtomicInteger();

        /**
         * The counter for "flush triggered" events.
         */
        final AtomicInteger flushTriggerCounter = new AtomicInteger();

        /**
         * A queue to hold metrics for "flush started" events.
         */
        final BlockingQueue<FlushMetricsEntry> startedMetrics = new LinkedBlockingQueue<>();

        /**
         * A queue to hold metrics for "flush finished" events.
         */
        final BlockingQueue<FlushMetricsEntry> finishedMetrics = new LinkedBlockingQueue<>();

        @Override
        public void onEntryAdded() {
            addedCounter.getAndIncrement();
        }

        @Override
        public void onFlushTriggered() {
            flushTriggerCounter.getAndIncrement();
        }

        @Override
        public void onFlushStarted(final long elapsed, final int flushEntriesCount) {
            startedMetrics.add(new FlushMetricsEntry(flushEntriesCount, 0));
        }

        @Override
        public void onFlushFinished(final long elapsed, final int successfulEntriesCount, final int failedEntriesCount) {
            finishedMetrics.add(new FlushMetricsEntry(successfulEntriesCount, failedEntriesCount));
        }
    }
}
