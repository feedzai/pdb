/*
 * Copyright 2022 Feedzai
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

package com.feedzai.commons.sql.abstraction.batch.impl;

import com.feedzai.commons.sql.abstraction.batch.AbstractBatch;
import com.feedzai.commons.sql.abstraction.batch.BatchEntry;
import com.feedzai.commons.sql.abstraction.batch.PdbBatch;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.feedzai.commons.sql.abstraction.listeners.BatchListener;
import com.feedzai.commons.sql.abstraction.listeners.MetricsListener;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A Batch that periodically flushes pending insertions to the database using multiple threads/connections.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
public class MultithreadedBatch implements PdbBatch, AutoCloseable {

    /**
     * The logger.
     */
    private static final Logger logger = LoggerFactory.getLogger(MultithreadedBatch.class);

    /**
     * The confidential logger.
     */
    private final Logger confidentialLogger;

    /**
     * The dev Marker.
     */
    private static final Marker DEV = MarkerFactory.getMarker("DEV");

    /**
     * Salt to avoid erroneous flushes.
     */
    private static final int SALT = 100;

    private final Map<Long, DatabaseEngine> dbEnginesMap = new ConcurrentHashMap<>();

    private final Supplier<DatabaseEngine> dbEngineSupplier;

    /**
     * The Timer that runs this task.
     */
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private final ExecutorService flusher;

    /**
     * The maximum await time to wait for the batch to shutdown.
     */
    private final long maxAwaitTimeShutdown;

    /**
     * The batchSize.
     */
    protected final int batchSize;

    /**
     * The batch timeout.
     */
    protected final long batchTimeout;

    /**
     * Timestamp of the last flush.
     */
    protected volatile long lastFlush;

    /**
     * EntityEntry buffer.
     */
    protected BlockingQueue<BatchEntry> buffer;

    /**
     * The name of the batch.
     */
    protected String name;

    /**
     * The listener for customized behavior when this batch succeeds or fails to persist data.
     *
     * @since 2.8.1
     */
    protected final BatchListener batchListener;

    private final MetricsListener metricsListener;

    /**
     * The number of times to retry a batch flush upon failure.
     * <p>
     * Defaults to {@value NO_RETRY}.
     */
    protected final int maxFlushRetries;

    /**
     * The time interval (milliseconds) to wait between batch flush retries.
     * <p>
     * Defaults to {@value DEFAULT_RETRY_INTERVAL}.
     */
    protected final long flushRetryDelay;

    /**
     * Creates a new instance of {@link AbstractBatch} with a {@link BatchListener}.
     *
     * @param dbEngine             The database engine.
     * @param name                 The batch name (null or empty names are allowed, falling back to "Anonymous Batch").
     * @param batchSize            The batch size.
     * @param batchTimeout         The batch timeout.
     * @param maxAwaitTimeShutdown The maximum await time for the batch to shutdown.
     * @param batchListener        The listener that will be invoked whenever some batch operation fail or succeeds to persist.
     * @param maxFlushRetries      The number of times to retry a batch flush upon failure. When set to 0, no retries
     *                             will be attempted.
     * @param flushRetryDelay      The time interval (milliseconds) to wait between batch flush retries.
     * @param confidentialLogger   The confidential logger.
     * @since 2.8.8
     */
    @Inject
    public MultithreadedBatch(final DatabaseEngine dbEngine, final MultithreadedBatchConfig batchConfig) {
        Objects.requireNonNull(dbEngine, "dbEngine can't be null.");
        Objects.requireNonNull(batchConfig, "batchConfig can't be null.");

        final int numberOfThreads = batchConfig.getNumberOfThreads();
        logger.info("Running MultithreadedBatch with {} threads.", numberOfThreads);

        final Properties properties = new Properties();
        properties.setProperty(PdbProperties.SCHEMA_POLICY, "none"); // TODO shema policies should be in an enum
        this.dbEngineSupplier = () -> {
            try {
                return dbEngine.duplicate(properties, true);
            } catch (final Exception e) {
                throw new IllegalStateException(e);
            }
        };


        this.batchSize = batchConfig.getBatchSize();
        this.buffer = new LinkedBlockingQueue<>(this.batchSize);
        this.batchTimeout = batchConfig.getBatchTimeoutMs();
        this.lastFlush = System.currentTimeMillis();
        this.name = batchConfig.getName();
        this.maxAwaitTimeShutdown = batchConfig.getMaxAwaitTimeShutdownMsOpt()
                .orElse(dbEngine.getProperties().getMaximumAwaitTimeBatchShutdown());
        this.batchListener = batchConfig.getBatchListener();
        this.metricsListener = batchConfig.getMetricsListener();
        this.maxFlushRetries = batchConfig.getMaxFlushRetries();
        this.flushRetryDelay = batchConfig.getFlushRetryDelayMs();
        this.confidentialLogger = batchConfig.getConfidentialLogger().orElse(logger);

        this.flusher = new ThreadPoolExecutor(
                numberOfThreads, numberOfThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(batchConfig.getExecutorCapacity()),
                new ThreadFactoryBuilder().setNameFormat("asyncBatch-" + name + "-%d").build()
        );

        scheduler.scheduleAtFixedRate(periodicFlushTask(), 0, batchTimeout + SALT, TimeUnit.MILLISECONDS);

        logger.info("{} - MultithreadedBatch started", name);
    }

    /**
     * Destroys this batch.
     */
    public void close() throws Exception {
        logger.info("{} - Destroy called on Batch", name);

        long remainingTimeout = this.maxAwaitTimeShutdown;
        final long start = System.currentTimeMillis();

        orderlyShutdownExecutor(scheduler, remainingTimeout);

        remainingTimeout = Math.max(this.maxAwaitTimeShutdown - (System.currentTimeMillis() - start), 1);
        try {
            flush().get(remainingTimeout, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            // ignore, continue shutdown of executors and connections
        }

        remainingTimeout = Math.max(this.maxAwaitTimeShutdown - (System.currentTimeMillis() - start), 1);
        orderlyShutdownExecutor(flusher, remainingTimeout);

        logger.trace("Closing internal database connections");
        dbEnginesMap.values().forEach(DatabaseEngine::close);
    }

    private void orderlyShutdownExecutor(final ExecutorService executor, final long remainingTimeout) {
        executor.shutdown();

        try {
            if (!executor.awaitTermination(remainingTimeout, TimeUnit.MILLISECONDS)) {
                logger.warn("Could not terminate batch within {}. Forcing shutdown.",
                        DurationFormatUtils.formatDurationWords(remainingTimeout, true, true));
                executor.shutdownNow();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("Interrupted while waiting.", e);
        }
    }

    /**
     * Adds the fields to the batch.
     *
     * @param entityName The table name.
     * @param ee         The entity entry.
     * @throws DatabaseEngineException If an error with the database occurs.
     */
    public void add(final String entityName, final EntityEntry ee) throws InterruptedException {
        add(new BatchEntry(entityName, ee));
    }

    /**
     * Adds the fields to the batch.
     *
     * @param batchEntry The batch entry.
     */
    public void add(final BatchEntry batchEntry) throws InterruptedException {
        this.buffer.put(batchEntry);

        this.metricsListener.onEntryAdded();

        if (this.buffer.size() == this.batchSize) {
            flush();
        }
    }

    /**
     * Flushes the pending batches asynchronously.
     * @return
     */
    public CompletableFuture<Void> flush() {
        // No-op if batch is empty
        if (buffer.isEmpty()) {
            logger.trace("[{}] Batch empty, not flushing", name);
            return CompletableFuture.completedFuture(null);
        }

        final List<BatchEntry> temp = new LinkedList<>();
        buffer.drainTo(temp);

        if (temp.isEmpty()) {
            logger.trace("[{}] Batch empty, not flushing", name);
            return CompletableFuture.completedFuture(null);
        }

        this.metricsListener.onFlush(temp.size());

        try {
            return CompletableFuture.runAsync(() -> {
                flush(temp);
                this.metricsListener.onFlushed(temp.size());
            }, this.flusher);
        } catch (final RejectedExecutionException e) {
            logger.trace("[{}] Rejected execution while flushing batch", name);
            onFlushFailure(temp);
            return CompletableFuture.completedFuture(null);
        } finally {
            this.metricsListener.onFlushed(temp.size());
        }
    }

    /**
     * Flushes the given list batch entries to {@link DatabaseEngine} immediately.
     *
     * @param temp List of batch entries to be written to the data base.
     */
    private void flush(final List<BatchEntry> temp) {
        final long start = System.currentTimeMillis();
        this.lastFlush = start;
        final DatabaseEngine de = dbEnginesMap.computeIfAbsent(Thread.currentThread().getId(), ignored -> dbEngineSupplier.get());

        try {
            processBatch(de, temp);

            final long elapsed = System.currentTimeMillis() - start;
            onFlushSuccess(temp, elapsed);
            logger.trace("[{}] Batch flushed. Took {} ms, {} rows.", name, elapsed, temp.size());

        } catch (final Exception e) {
            if (this.maxFlushRetries > 0) {
                final String msg = "[{}] Error occurred while flushing. Retrying.";
                confidentialLogger.warn(DEV, msg, name, e);
                if (confidentialLogger != logger) {
                    logger.warn(DEV, msg, name);
                }
            }

            boolean success = false;
            int retryCount;

            for (retryCount = 0; retryCount < this.maxFlushRetries && !success; retryCount++) {
                try {
                    Thread.sleep(this.flushRetryDelay);

                    // If the connection was established, we might need a rollback.
                    if (de.checkConnection() && de.isTransactionActive()) {
                        de.rollback();
                    }

                    processBatch(de, temp);

                    success = true;

                } catch (final InterruptedException ex) {
                    logger.debug("Interrupted while trying to flush batch. Stopping retries.");
                    Thread.currentThread().interrupt();
                    break;

                } catch (final Exception ex) {
                    final String msg = "[{}] Error occurred while flushing (retry attempt {}).";
                    confidentialLogger.warn(DEV, msg, name, retryCount + 1, ex);
                    if (confidentialLogger != logger) {
                        logger.warn(DEV, msg, name, retryCount + 1);
                    }
                }
            }

            if (!success) {
                try {
                    if (de.isTransactionActive()) {
                        de.rollback();
                    }
                } catch (final Exception ee) {
                    ee.addSuppressed(e);
                    final String msg = "[{}] Batch failed to check the flush transaction state";
                    confidentialLogger.trace(msg, name, ee);
                    if (confidentialLogger != logger) {
                        logger.trace(msg, name);
                    }
                }

                onFlushFailure(temp);
                final String msg = "[{}] Error occurred while flushing. Aborting batch flush.";
                confidentialLogger.error(DEV, msg, name, e);
                if (confidentialLogger != logger) {
                    logger.error(DEV, msg, name);
                }
            } else {
                final long elapsed = System.currentTimeMillis() - start;
                onFlushSuccess(temp, elapsed);
                logger.trace("[{}] Batch flushed. Took {} ms, {} retries, {} rows.", name, elapsed, retryCount, temp.size());
            }
        } finally {
            try {
                if (de.isTransactionActive()) {
                    de.rollback();
                }
            } catch (final Exception e) {
                final String msg = "[{}] Batch failed to check the flush transaction state";
                confidentialLogger.trace(msg, name, e);
                if (confidentialLogger != logger) {
                    logger.trace(msg, name);
                }
            }
        }
    }

    private void onFlushFailure(final List<BatchEntry> temp) {
        this.metricsListener.onFlushFailure();
        this.batchListener.onFailure(temp.toArray(new BatchEntry[0]));
    }

    private void onFlushSuccess(final List<BatchEntry> temp, final long elapsed) {
        this.metricsListener.onFlushSuccess(elapsed);
        this.batchListener.onSuccess(temp.toArray(new BatchEntry[0]));
    }

    /**
     * Processes all batch entries.
     * <p>
     * This is done by creating a transaction (by disabling auto-commit), adding all {@link BatchEntry batch entries} to
     * their respective prepared statements, flush them and finally perform a commit on the transaction (which will
     * enable auto-commit again afterwards).
     *
     * @param de
     * @param batchEntries The list of batch entries to be flush on the DB
     * @throws DatabaseEngineException If the operation failed
     */
    private void processBatch(final DatabaseEngine de, final List<BatchEntry> batchEntries) throws DatabaseEngineException {
        // begin the transaction before the addBatch calls in order to force the retry
        // of the connection if the same was lost during or since the last batch. Otherwise
        // the addBatch call that uses a prepared statement will fail
        de.beginTransaction();

        for (final BatchEntry entry : batchEntries) {
            de.addBatch(entry.getTableName(), entry.getEntityEntry());
        }

        de.flush();
        de.commit(); // automatically ends transaction
    }

    /**
     * FIXME javadoc
     *         // if a periodic execution throws an exception, future executions are suspended,
     *         // this task wraps the call in a try-catch block to prevent that. Errors are still propagated.
     *
     * @return
     */
    private Runnable periodicFlushTask() {
        return () -> {
            try {
                if (System.currentTimeMillis() - lastFlush >= batchTimeout) {
                    logger.trace("[{}] Flush timeout occurred", name);
                    flush();
                }
            } catch (final Exception e) {
                logger.error("[{}] Error during timeout-initiated flush", name, e);
            }
        };
    }
}
