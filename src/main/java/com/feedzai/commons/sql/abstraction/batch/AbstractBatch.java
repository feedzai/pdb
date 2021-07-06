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
package com.feedzai.commons.sql.abstraction.batch;

import com.feedzai.commons.sql.abstraction.FailureListener;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.feedzai.commons.sql.abstraction.listeners.BatchListener;
import com.google.common.base.Strings;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import javax.annotation.Nullable;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Batch that periodically flushes pending insertions to the database.
 * <p/>
 * Extending classes that want to be notified when a flush could not be performed after the timeout has been reached,
 * must override the {@link #onFlushFailure(BatchEntry[])}.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public abstract class AbstractBatch implements Runnable {
    /**
     * The logger.
     */
    protected final Logger logger = LoggerFactory.getLogger(AbstractBatch.class);

    /**
     * Constant representing that no retries should be attempted on batch flush failures.
     */
    public final static int NO_RETRY = 0;
    /**
     * Constant representing the default time interval (milliseconds) to wait between batch flush retries.
     */
    public final static long DEFAULT_RETRY_INTERVAL = 300L;

    /**
     * The dev Marker.
     */
    protected final static Marker dev = MarkerFactory.getMarker("DEV");
    /**
     * Salt to avoid erroneous flushes.
     */
    protected static final int salt = 100;

    /**
     * Lock used for concurrent access to the flush buffer.
     *
     * @since 2.1.4
     */
    private final Lock bufferLock = new ReentrantLock();

    /**
     * Lock used for concurrent access to the flush transaction.
     * <p>
     * We are not using the {@link #bufferLock} because the
     * {@link #add(BatchEntry)} calls would block while a flush
     * transaction is in progress.
     *
     * @since 2.1.5
     */
    private final Lock flushTransactionLock = new ReentrantLock();

    /**
     * The database engine.
     */
    protected final DatabaseEngine de;
    /**
     * The maximum await time to wait for the batch to shutdown.
     */
    protected final long maxAwaitTimeShutdown;
    /**
     * The Timer that runs this task.
     */
    protected ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    /**
     * The batchSize.
     */
    protected final int batchSize;
    /**
     * The batch timeout.
     */
    protected final long batchTimeout;
    /**
     * The batch at the present moment.
     */
    protected int batch;
    /**
     * Timestamp of the last flush.
     */
    protected volatile long lastFlush;
    /**
     * EntityEntry buffer.
     */
    protected LinkedList<BatchEntry> buffer = new LinkedList<>();
    /**
     * The name of the batch.
     */
    protected String name;

    /**
     * The listener for customized behavior when this batch succeeds or fails to persist data.
     *
     * @since 2.8.1
     */
    protected Optional<BatchListener> batchListener = Optional.empty();

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
     * @param de                   The database engine.
     * @param name                 The batch name (null or empty names are allowed, falling back to "Anonymous Batch").
     * @param batchSize            The batch size.
     * @param batchTimeout         The batch timeout.
     * @param maxAwaitTimeShutdown The maximum await time for the batch to shutdown.
     * @param batchListener        The listener that will be invoked whenever some batch operation fail or succeeds to persist.
     * @param maxFlushRetries      The number of times to retry a batch flush upon failure. When set to 0, no retries
     *                             will be attempted.
     * @param flushRetryDelay      The time interval (milliseconds) to wait between batch flush retries.
     *
     * @since 2.8.1
     */
    protected AbstractBatch(
        final DatabaseEngine de,
        final String name,
        final int batchSize,
        final long batchTimeout,
        final long maxAwaitTimeShutdown,
        @Nullable final BatchListener batchListener,
        final int maxFlushRetries,
        final long flushRetryDelay) {
        Objects.requireNonNull(de, "The provided database engine is null.");

        this.de = de;
        this.batchSize = batchSize;
        this.batch = batchSize;
        this.batchTimeout = batchTimeout;
        this.lastFlush = System.currentTimeMillis();
        this.name = Strings.isNullOrEmpty(name) ? "Anonymous Batch" : name;
        this.maxAwaitTimeShutdown = maxAwaitTimeShutdown;
        this.batchListener = Optional.ofNullable(batchListener);
        this.maxFlushRetries = maxFlushRetries;
        this.flushRetryDelay = flushRetryDelay;
    }

    /**
     * Creates a new instance of {@link AbstractBatch} with a {@link BatchListener}.
     *
     * @param de                   The database engine.
     * @param name                 The batch name (null or empty names are allowed, falling back to "Anonymous Batch").
     * @param batchSize            The batch size.
     * @param batchTimeout         The batch timeout.
     * @param maxAwaitTimeShutdown The maximum await time for the batch to shutdown.
     * @param batchListener        The listener that will be invoked whenever some batch operation fail or succeeds to persist.
     *
     * @since 2.8.1
     */
    protected AbstractBatch(
        final DatabaseEngine de,
        final String name,
        final int batchSize,
        final long batchTimeout,
        final long maxAwaitTimeShutdown,
        @Nullable final BatchListener batchListener) {
        this(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown, batchListener, NO_RETRY, DEFAULT_RETRY_INTERVAL);
    }

    /**
     * Creates a new instance of {@link AbstractBatch} with a {@link FailureListener}.
     *
     * @param de                   The database engine.
     * @param name                 The batch name (null or empty names are allowed, falling back to "Anonymous Batch").
     * @param batchSize            The batch size.
     * @param batchTimeout         The batch timeout.
     * @param maxAwaitTimeShutdown The maximum await time for the batch to shutdown.
     * @param failureListener      The listener that will be invoked whenever some batch operation fail to persist.
     * @param maxFlushRetries      The number of times to retry a batch flush upon failure. When set to 0, no retries
     *                             will be attempted.
     * @param flushRetryDelay      The time interval (milliseconds) to wait between batch flush retries.
     *
     * @since 2.1.12
     * @deprecated Use {@link #AbstractBatch(DatabaseEngine, String, int, long, long, BatchListener, int, long)} instead.
     */
    @Deprecated
    protected AbstractBatch(
            final DatabaseEngine de,
            final String name,
            final int batchSize,
            final long batchTimeout,
            final long maxAwaitTimeShutdown,
            final FailureListener failureListener,
            final int maxFlushRetries,
            final long flushRetryDelay) {
        this(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown, convertToBatchListener(failureListener), maxFlushRetries, flushRetryDelay);
    }

    /**
     * Creates a new instance of {@link AbstractBatch} with a {@link FailureListener}.
     *
     * @param de                   The database engine.
     * @param name                 The batch name (null or empty names are allowed, falling back to "Anonymous Batch").
     * @param batchSize            The batch size.
     * @param batchTimeout         The batch timeout.
     * @param maxAwaitTimeShutdown The maximum await time for the batch to shutdown.
     * @param failureListener      The listener that will be invoked whenever some batch operation fail to persist.
     *
     * @since 2.1.11
     * @deprecated Use {@link #AbstractBatch(DatabaseEngine, String, int, long, long, BatchListener)} instead.
     */
    @Deprecated
    protected AbstractBatch(
            final DatabaseEngine de,
            final String name,
            final int batchSize,
            final long batchTimeout,
            final long maxAwaitTimeShutdown,
            final FailureListener failureListener) {
        this(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown, convertToBatchListener(failureListener), NO_RETRY, DEFAULT_RETRY_INTERVAL);
    }

    /**
     * Creates a new instance of {@link AbstractBatch}.
     *
     * @param de                   The database engine.
     * @param name                 The batch name (null or empty names are allowed, falling back to "Anonymous Batch").
     * @param batchSize            The batch size.
     * @param batchTimeout         The batch timeout.
     * @param maxAwaitTimeShutdown The maximum await time for the batch to shutdown.
     */
    protected AbstractBatch(final DatabaseEngine de, String name, final int batchSize, final long batchTimeout, final long maxAwaitTimeShutdown) {
        this(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown, (BatchListener) null);
    }

    /**
     * Creates a new instance of {@link AbstractBatch}.
     *
     * @param de           The database engine.
     * @param batchSize    The batch size.
     * @param batchTimeout The batch timeout.
     */
    protected AbstractBatch(final DatabaseEngine de, final int batchSize, final long batchTimeout, final long maxAwaitTimeShutdown) {
        this(de, null, batchSize, batchTimeout, maxAwaitTimeShutdown);
    }


    /**
     * Starts the timer task.
     */
    protected void start() {
        // if a periodic execution throws an exception, future executions are suspended,
        // this task wraps the call in a try-catch block to prevent that. Errors are still propagated.
        final Runnable resilientTask = () -> {
            try {
                run();
            } catch (final Exception e) {
                logger.error("[{}] Error during timeout-initiated flush", name, e);
            }
        };

        scheduler.scheduleAtFixedRate(resilientTask, 0, batchTimeout + salt, TimeUnit.MILLISECONDS);
    }

    /**
     * Destroys this batch.
     */
    public void destroy() {
        logger.trace("{} - Destroy called on Batch", name);
        scheduler.shutdown();

        try {
            if (!scheduler.awaitTermination(maxAwaitTimeShutdown, TimeUnit.MILLISECONDS)) {
                logger.warn(
                        "Could not terminate batch within {}. Forcing shutdown.",
                        DurationFormatUtils.formatDurationWords(
                                maxAwaitTimeShutdown,
                                true,
                                true
                        )
                );
                scheduler.shutdownNow();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("Interrupted while waiting.", e);
        }

        flush();
    }

    /**
     * Adds the fields to the batch.
     *
     * @param batchEntry The batch entry.
     * @throws DatabaseEngineException If an error with the database occurs.
     */
    public void add(BatchEntry batchEntry) throws DatabaseEngineException {
        bufferLock.lock();
        try {
            buffer.add(batchEntry);
            batch--;
        } finally {
            bufferLock.unlock();
        }
        if (batch <= 0) {
            flush();
        }
    }

    /**
     * Adds the fields to the batch.
     *
     * @param entityName The table name.
     * @param ee         The entity entry.
     * @throws DatabaseEngineException If an error with the database occurs.
     */
    public void add(final String entityName, final EntityEntry ee) throws DatabaseEngineException {
        add(new BatchEntry(entityName, ee));
    }

    /**
     * Flushes the pending batches.
     *
     * @implSpec Same as {@link #flush(boolean)} with {@link false}.
     */
    public void flush() {
        List<BatchEntry> temp;

        bufferLock.lock();
        try {
            // Reset the last flush timestamp, even if the batch is empty or flush fails
            lastFlush = System.currentTimeMillis();

            // No-op if batch is empty
            if (batch == batchSize) {
                logger.trace("[{}] Batch empty, not flushing", name);
                return;
            }

            // Declare the batch empty, regardless of flush success/failure
            batch = batchSize;

            // If something goes wrong we still have a copy to recover.
            temp = buffer;
            buffer = new LinkedList<>();

        } finally {
            bufferLock.unlock();
        }

        long start = System.currentTimeMillis();
        try {
            flushTransactionLock.lock();
            start = System.currentTimeMillis();

            processBatch(temp);

            onFlushSuccess(temp.toArray(new BatchEntry[0]));
            logger.trace("[{}] Batch flushed. Took {} ms, {} rows.", name, (System.currentTimeMillis() - start), temp.size());
        } catch (final Exception e) {
            if (this.maxFlushRetries > 0) {
                logger.warn(dev, "[{}] Error occurred while flushing. Retrying.", name, e);
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

                    processBatch(temp);

                    success = true;
                } catch (final InterruptedException ex) {
                    logger.debug("Interrupted while trying to flush batch. Stopping retries.");
                    Thread.currentThread().interrupt();
                    break;
                } catch (final Exception ex) {
                    logger.warn(dev, "[{}] Error occurred while flushing (retry attempt {}).", name, retryCount + 1, ex);
                }
            }

            if (!success) {
                try {
                    if (de.isTransactionActive()) {
                        de.rollback();
                    }
                } catch (final Exception ee) {
                    ee.addSuppressed(e);
                    logger.trace("[{}] Batch failed to check the flush transaction state", name, ee);
                }

                onFlushFailure(temp.toArray(new BatchEntry[0]));
                logger.error(dev, "[{}] Error occurred while flushing. Aborting batch flush.", name, e);
            } else {
                onFlushSuccess(temp.toArray(new BatchEntry[0]));
                logger.trace("[{}] Batch flushed. Took {} ms, {} retries, {} rows.", name,
                        (System.currentTimeMillis() - start), retryCount, temp.size());
            }
        } finally {
            try {
                if (de.isTransactionActive()) {
                    de.rollback();
                }
            } catch (final Exception e) {
                logger.trace("[{}] Batch failed to check the flush transaction state", name, e);
            } finally {
                flushTransactionLock.unlock();
            }
        }
    }

    /**
     * Flushes the pending messages.
     * <p>
     * If {@code sync} is {@code true} it waits for other pending flush operations.
     * If it is {@code false} it can return directly if the buffer in the batch is empty.
     *
     * @param sync {@code true} if it should wait for other flush operations (from here, or those started by calling
     *             {@link #flush()}).
     * @implNote It is possible that two threads might execute in competing order and sync execution acquires the
     * {@link #flushTransactionLock} before a non synchronous one, leading to a non serial execution.
     * <p>
     * Since the flushes on the DB connection are also guarded by the same {@link #flushTransactionLock}, the result is
     * that if the batch is not empty, the thread calling this method will wait for an ongoing flush either before or
     * after draining the batch buffer to a local temporary buffer (respectively, when {@code sync} is {@code true} or
     * {@code false}).
     * If the batch is empty, then the thread calling this method will either wait for an ongoing flush or it will
     * return from the method, depending on whether {@code sync} is {@code true} or {@code false}, respectively.
     * @since 2.1.6
     */
    public void flush(final boolean sync) {
        if (!sync) {
            flush();
        } else {
            try {
                flushTransactionLock.lock();
                flush();
            } finally {
                flushTransactionLock.unlock();
            }
        }
    }

    /**
     * Notifies about the pending entries on flush failure.
     *
     * @param entries The entries that are pending to be persisted.
     */
    public void onFlushFailure(final BatchEntry[] entries) {
        this.batchListener.ifPresent(batchListener -> batchListener.onFailure(entries));
    }

    /**
     * Notifies about succeeded entries on flush success.
     *
     * @param entries The entries that were persisted.
     *
     * @since 2.8.1
     */
    public void onFlushSuccess(final BatchEntry[] entries) {
        this.batchListener.ifPresent(batchListener -> batchListener.onSuccess(entries));
    }

    @Override
    public void run() {
        if (System.currentTimeMillis() - lastFlush >= batchTimeout) {
            logger.trace("[{}] Flush timeout occurred", name);
            flush();
        }
    }

    /**
     * Processes all batch entries.
     * <p>
     * This is done by creating a transaction (by disabling auto-commit), adding all {@link BatchEntry batch entries} to
     * their respective prepared statements, flush them and finally perform a commit on the transaction (which will
     * enable auto-commit again afterwards).
     *
     * @param batchEntries The list of batch entries to be flush on the DB
     * @throws DatabaseEngineException If the operation failed
     */
    private void processBatch(final List<BatchEntry> batchEntries) throws DatabaseEngineException {
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
     * Converts a {@link FailureListener} to {@link BatchListener}.
     *
     * @param failureListener The {@link FailureListener} to be converted.
     * @return A {@link BatchListener} that calls a {@link FailureListener} on failure.
     *
     * @since 2.8.1
     * @deprecated The {@link FailureListener} is deprecated and this method will be removed once it is removed.
     */
    public static BatchListener convertToBatchListener(final FailureListener failureListener) {
        return new BatchListener() {
            @Override
            public void onFailure(final BatchEntry[] rowsFailed) {
                failureListener.onFailure(rowsFailed);
            }

            @Override
            public void onSuccess(final BatchEntry[] rowsSucceeded) {
                // Do nothing.
            }
        };
    }
}
