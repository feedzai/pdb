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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
     * Constant {@link FailureListener} representing the NO OP operation.
     */
    public final static FailureListener NO_OP = rowsFailed -> {};

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
     * The failure listener for customized behavior when this batch fails to persist data.
     */
    protected Optional<FailureListener> failureListener = Optional.empty();

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
     */
    protected AbstractBatch(
            final DatabaseEngine de,
            final String name,
            final int batchSize,
            final long batchTimeout,
            final long maxAwaitTimeShutdown,
            final FailureListener failureListener) {
        Preconditions.checkNotNull(de, "The provided database engine is null.");
        Preconditions.checkNotNull(failureListener, "The provided failure listener is null");

        this.de = de;
        this.batchSize = batchSize;
        this.batch = batchSize;
        this.batchTimeout = batchTimeout;
        this.lastFlush = System.currentTimeMillis();
        this.name = Strings.isNullOrEmpty(name) ? "Anonymous Batch" : name;
        this.maxAwaitTimeShutdown = maxAwaitTimeShutdown;
        this.failureListener = Optional.of(failureListener);
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
        this(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown, NO_OP);
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
        scheduler.scheduleAtFixedRate(this, 0, batchTimeout + salt, TimeUnit.MILLISECONDS);
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

        try {
            flushTransactionLock.lock();
            final long start = System.currentTimeMillis();

            // begin the transaction before the addBatch calls in order to force the retry
            // of the connection if the same was lost during or since the last batch. Otherwise
            // the addBatch call that uses a prepared statement will fail
            de.beginTransaction();

            for (BatchEntry entry : temp) {
                de.addBatch(entry.getTableName(), entry.getEntityEntry());
            }

            de.flush();
            de.commit();
            logger.trace("[{}] Batch flushed. Took {} ms, {} rows.", name, (System.currentTimeMillis() - start), temp.size());
        } catch (Exception e) {
            logger.error(dev, "[{}] Error occurred while flushing.", name, e);
            /*
             * We cannot try any recovery here because we don't know why it failed. If it failed by a Constraint
             * violation for instance, we cannot try it again and again because it will always fail.
             *
             * The idea here is to hand the entries that could not be added and continue right where we were.
             *
             * So, temp will only have the entries until it failed, this can be all the entries or until de.addBatch() failed;
             * and buffer will have the events right after the failure.
             */
            onFlushFailure(temp.toArray(new BatchEntry[temp.size()]));
        } finally {
            try {
                if (de.isTransactionActive()) {
                    de.rollback();
                }
            } catch (Exception e) {
                logger.trace("[{}] Batch failed to check the flush transaction state", name);
            }
            flushTransactionLock.unlock();
        }
    }

    /**
     * Flushes the pending messages.
     * <p>
     * If {@param sync} is {@literal true} it waits for other pending flush operations.
     * <p>
     * If {@param sync} is {@literal false} it can return directly if the buffer if the batch is empty.
     *
     * @param sync {@literal true} if it should wait for other {@link #flush()} operations.
     * @implSpec It is possible that two threads might execute in competing order and sync execution acquires the flushTransactionLock before a non synchronous one leading
     * to a non serial execution.
     * @since 2.1.6
     */
    public void flush(boolean sync) {
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
    public void onFlushFailure(BatchEntry[] entries) {
        if (!this.failureListener.isPresent()) {
            return;
        }

        this.failureListener.get().onFailure(entries);
    }

    @Override
    public void run() {
        if (System.currentTimeMillis() - lastFlush >= batchTimeout) {
            logger.trace("[{}] Flush timeout occurred", name);
            flush();
        }
    }
}
