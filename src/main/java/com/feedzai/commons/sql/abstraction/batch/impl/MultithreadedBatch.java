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

import com.feedzai.commons.sql.abstraction.batch.AbstractPdbBatch;
import com.feedzai.commons.sql.abstraction.batch.BatchEntry;
import com.feedzai.commons.sql.abstraction.batch.PdbBatch;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.listeners.BatchListener;
import com.feedzai.commons.sql.abstraction.listeners.MetricsListener;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
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
public class MultithreadedBatch extends AbstractPdbBatch implements PdbBatch {

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
     * A delay to make sure that when the next periodic flush is triggered, it occurs after the batch timeout period
     * has passed since the last flush occurred (unless the flush was triggered by batch size).
     */
    private static final int SALT = 100;

    /**
     * A map containing the {@link DatabaseEngine database engines} used by each tread (where the key is the thread id).
     */
    private final Map<Long, DatabaseEngine> dbEnginesMap = new ConcurrentHashMap<>();

    /**
     * A {@link Supplier} of {@link DatabaseEngine} for the various threads.
     */
    private final Supplier<DatabaseEngine> dbEngineSupplier;

    /**
     * The executor used to schedule periodic batch flushes.
     */
    private final ScheduledExecutorService scheduler;

    /**
     * The executor used to run the various flush tasks concurrently.
     */
    private final ExecutorService flusher;

    /**
     * The maximum time in milliseconds to wait for the batch to shutdown.
     */
    private final long maxAwaitTimeShutdownMs;

    /**
     * The batch size.
     */
    protected final int batchSize;

    /**
     * The batch timeout in milliseconds.
     */
    protected final long batchTimeoutMs;

    /**
     * Timestamp of the last flush.
     */
    protected volatile long lastFlush;

    /**
     * A buffer of {@link BatchEntry batch entries}.
     */
    protected BlockingQueue<BatchEntry> buffer;

    /**
     * The name of the batch.
     */
    protected String name;

    /**
     * The listener for customized behavior when this batch succeeds or fails to persist data.
     */
    protected final BatchListener batchListener;

    /**
     * The listener for events that can be used to collect metrics.
     */
    private final MetricsListener metricsListener;

    /**
     * The number of times to retry a batch flush upon failure.
     */
    protected final int maxFlushRetries;

    /**
     * The time interval in milliseconds to wait between batch flush retries.
     */
    protected final long flushRetryDelayMs;

    /**
     * A set of {@link CompletableFuture} corresponding to flush operations currently running.
     */
    private final Set<CompletableFuture<Void>> pendingFlushFutures = ConcurrentHashMap.newKeySet();

    /**
     * Creates a new instance of {@link MultithreadedBatch}.
     *
     * @param dbEngine    The database engine.
     * @param batchConfig The batch configuration.
     * @implNote The internal timer task for periodic flushes is started.
     */
    @Inject
    public MultithreadedBatch(final DatabaseEngine dbEngine, final MultithreadedBatchConfig batchConfig) {
        Objects.requireNonNull(dbEngine, "dbEngine can't be null.");
        Objects.requireNonNull(batchConfig, "batchConfig can't be null.");

        final int numberOfThreads = batchConfig.getNumberOfThreads();
        logger.info("Running MultithreadedBatch with {} threads.", numberOfThreads);

        final Properties properties = new Properties();
        properties.setProperty(PdbProperties.SCHEMA_POLICY, "none");
        this.dbEngineSupplier = () -> {
            try {
                return dbEngine.duplicate(properties, true);
            } catch (final Exception e) {
                throw new IllegalStateException(e);
            }
        };

        this.batchSize = batchConfig.getBatchSize();
        this.buffer = new LinkedBlockingQueue<>(this.batchSize);
        this.batchTimeoutMs = batchConfig.getBatchTimeout().toMillis();
        this.lastFlush = System.currentTimeMillis();
        this.name = batchConfig.getName();
        this.maxAwaitTimeShutdownMs = Optional.ofNullable(batchConfig.getMaxAwaitTimeShutdown())
                .map(Duration::toMillis)
                .orElse(dbEngine.getProperties().getMaximumAwaitTimeBatchShutdown());
        this.batchListener = batchConfig.getBatchListener();
        this.metricsListener = batchConfig.getMetricsListener();
        this.maxFlushRetries = batchConfig.getMaxFlushRetries();
        this.flushRetryDelayMs = batchConfig.getFlushRetryDelay().toMillis();
        this.confidentialLogger = batchConfig.getConfidentialLogger().orElse(logger);

        this.scheduler = Executors.newScheduledThreadPool(
                1,
                new ThreadFactoryBuilder()
                        .setNameFormat("MultiThreadedBatch-scheduler-" + name + "-%d")
                        .setUncaughtExceptionHandler((thread, throwable) ->
                                logger.error("Uncaught exception in scheduler worker thread.", throwable))
                        .build()
        );

        this.flusher = new ThreadPoolExecutor(
                numberOfThreads, numberOfThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(batchConfig.getExecutorCapacity()),
                new ThreadFactoryBuilder()
                        .setNameFormat("MultiThreadedBatch-" + name + "-%d")
                        .setUncaughtExceptionHandler((thread, throwable) ->
                                logger.error("Uncaught exception in flusher worker thread.", throwable))
                        .build()
        );

        scheduler.scheduleAtFixedRate(periodicFlushTask(), 0, batchTimeoutMs + SALT, TimeUnit.MILLISECONDS);

        logger.info("{} - MultithreadedBatch started", name);
    }

    /**
     * Closes this batch.
     */
    public void close() {
        logger.info("{} - MultithreadedBatch closing", name);

        long remainingTimeout = this.maxAwaitTimeShutdownMs;
        final long start = System.currentTimeMillis();

        orderlyShutdownExecutor(scheduler, remainingTimeout);

        remainingTimeout = Math.max(this.maxAwaitTimeShutdownMs - (System.currentTimeMillis() - start), 1);
        try {
            flushAsync().get(remainingTimeout, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            // ignore, continue shutdown of executors and connections
        }

        remainingTimeout = Math.max(this.maxAwaitTimeShutdownMs - (System.currentTimeMillis() - start), 1);
        orderlyShutdownExecutor(flusher, remainingTimeout);

        try {
            this.metricsListener.close();
        } catch (final Exception e) {
            // ignore, continue closing DB connections
        }

        logger.trace("Closing internal database connections");
        dbEnginesMap.values().forEach(DatabaseEngine::close);
    }

    /**
     * Performs a shutdown of the provided executor.
     * <p>
     * This helper method first tries to perform an orderly shutdown of the executor, waiting the specified amount of
     * time. If after the time the executor hasn't terminated yet, performs a forceful shutdown.
     *
     * @param executor        The executor to shutdown.
     * @param shutdownTimeout The maximum time to wait for an orderly shutdown of the executor.
     */
    private void orderlyShutdownExecutor(final ExecutorService executor, final long shutdownTimeout) {
        executor.shutdown();

        try {
            if (!executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
                logger.warn("Could not terminate batch within {}. Forcing shutdown.",
                        DurationFormatUtils.formatDurationWords(shutdownTimeout, true, true));
                executor.shutdownNow();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("Interrupted while waiting.", e);
        }
    }

    @Override
    public void add(final BatchEntry batchEntry) throws InterruptedException {
        this.buffer.put(batchEntry);

        this.metricsListener.onEntryAdded();

        if (this.buffer.size() == this.batchSize) {
            flushAsync();
        }
    }

    @Override
    public void flush() throws ExecutionException, InterruptedException {
        flushAsync();
        CompletableFuture.allOf(pendingFlushFutures.toArray(new CompletableFuture[0])).get();
    }

    @Override
    public CompletableFuture<Void> flushAsync() {
        this.metricsListener.onFlushTriggered();
        final long flushTriggeredMs = System.currentTimeMillis();
        // Reset the last flush timestamp, even if the batch is empty or flush fails
        lastFlush = flushTriggeredMs;

        // No-op if batch is empty
        if (buffer.isEmpty()) {
            onFlushFinished(flushTriggeredMs, Collections.emptyList(), Collections.emptyList());
            logger.trace("[{}] Batch empty, not flushing", name);
            return CompletableFuture.completedFuture(null);
        }

        final List<BatchEntry> temp = new LinkedList<>();
        buffer.drainTo(temp);

        if (temp.isEmpty()) {
            onFlushFinished(flushTriggeredMs, Collections.emptyList(), Collections.emptyList());
            logger.trace("[{}] Batch empty, not flushing", name);
            return CompletableFuture.completedFuture(null);
        }

        try {
            final CompletableFuture<Void> flushAsyncFuture = CompletableFuture.runAsync(() -> flush(flushTriggeredMs, temp), this.flusher);

            if (!flushAsyncFuture.isDone()) {
                /*
                 Add the future to the set of pending futures, so that the blocking flush() can wait for all of them.
                 When done, the future removes itself (if done already, all this can be skipped).
                 */
                this.pendingFlushFutures.add(flushAsyncFuture);
                flushAsyncFuture.whenComplete((unused, throwable) -> this.pendingFlushFutures.remove(flushAsyncFuture));
            }

            return flushAsyncFuture;

        } catch (final RejectedExecutionException e) {
            logger.trace("[{}] Rejected execution while flushing batch", name);

            onFlushFinished(flushTriggeredMs, Collections.emptyList(), temp);

            final CompletableFuture<Void> flushFuture = new CompletableFuture<>();
            flushFuture.completeExceptionally(e);
            return flushFuture;
        }
    }

    @Override
    public void flushIgnore() {
        logger.trace("Flush ignoring not available for MultithreadedBatch. Skipping ...");
    }

    /**
     * Flushes the given list batch entries to {@link DatabaseEngine} immediately.
     *
     * @param flushTriggeredMs The timestamp (in milliseconds since Unix epoch) when batch flush was triggered.
     * @param batchEntries     List of batch entries to be written to the database.
     */
    private void flush(final long flushTriggeredMs, final List<BatchEntry> batchEntries) {
        final DatabaseEngine de = dbEnginesMap.computeIfAbsent(Thread.currentThread().getId(), ignored -> dbEngineSupplier.get());
        this.metricsListener.onFlushStarted(flushTriggeredMs, batchEntries.size());

        try {
            processBatch(de, batchEntries);

            onFlushFinished(flushTriggeredMs, batchEntries, Collections.emptyList());

            logger.trace("[{}] Batch flushed. Took {} ms, {} rows.",
                    name, System.currentTimeMillis() - flushTriggeredMs, batchEntries.size());

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
                    Thread.sleep(this.flushRetryDelayMs);

                    // If the connection was established, we might need a rollback.
                    if (de.checkConnection() && de.isTransactionActive()) {
                        de.rollback();
                    }

                    processBatch(de, batchEntries);

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

                onFlushFinished(flushTriggeredMs, Collections.emptyList(), batchEntries);

                final String msg = "[{}] Error occurred while flushing. Aborting batch flush.";
                confidentialLogger.error(DEV, msg, name, e);
                if (confidentialLogger != logger) {
                    logger.error(DEV, msg, name);
                }
            } else {
                onFlushFinished(flushTriggeredMs, batchEntries, Collections.emptyList());
                logger.trace("[{}] Batch flushed. Took {} ms, {} retries, {} rows.",
                        name, System.currentTimeMillis() - flushTriggeredMs, retryCount, batchEntries.size());
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

    /**
     * Notifies the listeners when the flush finishes.
     *
     * @param flushTriggeredMs  The timestamp (in milliseconds since Unix epoch) when batch flush was triggered.
     * @param successfulEntries The entries that were part of the batch that succeeded.
     * @param failedEntries     The entries that were part of the batch that failed.
     */
    private void onFlushFinished(final long flushTriggeredMs,
                                 final List<BatchEntry> successfulEntries,
                                 final List<BatchEntry> failedEntries) {

        final long elapsed = System.currentTimeMillis() - flushTriggeredMs;
        this.metricsListener.onFlushFinished(elapsed, successfulEntries.size(), failedEntries.size());

        if (!failedEntries.isEmpty()) {
            this.batchListener.onFailure(failedEntries.toArray(new BatchEntry[0]));
        }

        if (!successfulEntries.isEmpty()) {
            this.batchListener.onSuccess(successfulEntries.toArray(new BatchEntry[0]));
        }
    }

    /**
     * Gets a {@link Runnable} task to execute flushes.
     * <p>
     * This is meant to be executed periodically by the {@link #scheduler}, so that periodic flushes occur when they
     * aren't otherwise triggered by the number of batch entries.
     * Future executions of this task would be suspended if it were to throw an exception. For that reason, this task
     * wraps the flush call in a try-catch block to prevent that; errors are still propagated.
     *
     * @return The task to execute flushes.
     */
    private Runnable periodicFlushTask() {
        return () -> {
            try {
                if (System.currentTimeMillis() - lastFlush >= batchTimeoutMs) {
                    logger.trace("[{}] Flush timeout occurred", name);
                    flushAsync();
                }
            } catch (final Exception e) {
                logger.error("[{}] Error during timeout-initiated flush", name, e);
            }
        };
    }
}
