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

/**
 * The default batch implementation.
 * <p/>
 * Behind the scenes, it will periodically flush pending statements. It has auto recovery
 * and will never thrown any exception.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @see AbstractBatch
 * @since 2.0.0
 */
public class DefaultBatch extends AbstractBatch {

    /**
     * Creates a new instance of {@link DefaultBatch}.
     *
     * @param de                   The database engine reference.
     * @param name                 The batch name.
     * @param batchSize            The batch size.
     * @param batchTimeout         The timeout.
     * @param maxAwaitTimeShutdown The maximum await time for the batch to shutdown.
     */
    protected DefaultBatch(final DatabaseEngine de, final String name, final int batchSize, final long batchTimeout,
                           final long maxAwaitTimeShutdown) {
        super(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown);
    }

    /**
     * Creates a new instance of {@link DefaultBatch}.
     *
     * @param de                   The database engine reference.
     * @param name                 The batch name.
     * @param batchSize            The batch size.
     * @param batchTimeout         The timeout.
     * @param maxAwaitTimeShutdown The maximum await time for the batch to shutdown.
     * @param listener             The listener that will be invoked when batch fails to persist at least one data row.
     */
    protected DefaultBatch(final DatabaseEngine de, final String name, final int batchSize, final long batchTimeout,
                           final long maxAwaitTimeShutdown, final FailureListener listener) {
        super(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown, listener);
    }

    /**
     * Creates a new instance of {@link DefaultBatch}.
     *
     * @param de                   The database engine reference.
     * @param name                 The batch name.
     * @param batchSize            The batch size.
     * @param batchTimeout         The timeout.
     * @param maxAwaitTimeShutdown The maximum await time for the batch to shutdown.
     * @param listener             The listener that will be invoked when batch fails to persist at least one data row.
     * @param maxFlushRetries      The number of times to retry a batch flush upon failure. Defaults to
     *                             {@value NO_RETRY}. When set to 0, no retries will be attempted.
     * @param flushRetryDelay      The time interval (milliseconds) to wait between batch flush retries. Defaults to
     *                             {@value DEFAULT_RETRY_INTERVAL}.
     *
     * @since 2.1.12
     */
    protected DefaultBatch(final DatabaseEngine de, final String name, final int batchSize, final long batchTimeout,
                           final long maxAwaitTimeShutdown, final FailureListener listener, final int maxFlushRetries,
                           final long flushRetryDelay) {
        super(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown, listener, maxFlushRetries, flushRetryDelay);
    }

    /**
     * <p>Creates a new instance of {@link DefaultBatch}.</p>
     * <p>Starts the timertask.</p>
     *
     * @param de                   The database engine.
     * @param name                 The batch name.
     * @param batchSize            The batch size.
     * @param batchTimeout         The batch timeout.
     * @param maxAwaitTimeShutdown The maximum await time for the batch to shutdown.
     * @return The Batch.
     */
    public static DefaultBatch create(final DatabaseEngine de, final String name, final int batchSize,
                                      final long batchTimeout, final long maxAwaitTimeShutdown) {
        final DefaultBatch b = new DefaultBatch(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown);
        b.start();

        return b;
    }

    /**
     * <p>Creates a new instance of {@link DefaultBatch} with a {@link FailureListener}.</p>
     * <p>Starts the timertask.</p>
     *
     * @param de                   The database engine.
     * @param name                 The batch name.
     * @param batchSize            The batch size.
     * @param batchTimeout         The batch timeout.
     * @param maxAwaitTimeShutdown The maximum await time for the batch to shutdown.
     * @param listener             The listener that will be invoked when batch fails to persist at least one data row.
     * @return The Batch.
     *
     * @since 2.1.11
     */
    public static DefaultBatch create(final DatabaseEngine de, final String name, final int batchSize, final long batchTimeout,
                                      final long maxAwaitTimeShutdown, final FailureListener listener) {
        final DefaultBatch b = new DefaultBatch(de, name, batchSize, batchTimeout, maxAwaitTimeShutdown, listener);
        b.start();

        return b;
    }

    /**
     * <p>Creates a new instance of {@link DefaultBatch} with a {@link FailureListener}.</p>
     * <p>Starts the timertask.</p>
     *
     * @param de                   The database engine.
     * @param name                 The batch name.
     * @param batchSize            The batch size.
     * @param batchTimeout         The batch timeout.
     * @param maxAwaitTimeShutdown The maximum await time for the batch to shutdown.
     * @param listener             The listener that will be invoked when batch fails to persist at least one data row.
     * @param maxFlushRetries      The number of times to retry a batch flush upon failure. Defaults to
     *                             {@value NO_RETRY}. When set to 0, no retries will be attempted.
     * @param flushRetryDelay      The time interval (milliseconds) to wait between batch flush retries. Defaults to
     *                             {@value DEFAULT_RETRY_INTERVAL}.
     * @return The Batch.
     *
     * @since 2.1.12
     */
    public static DefaultBatch create(final DatabaseEngine de, final String name, final int batchSize,
                                      final long batchTimeout, final long maxAwaitTimeShutdown,
                                      final FailureListener listener, final int maxFlushRetries,
                                      final long flushRetryDelay) {
        final DefaultBatch b = new DefaultBatch(
                de,
                name,
                batchSize,
                batchTimeout,
                maxAwaitTimeShutdown,
                listener,
                maxFlushRetries,
                flushRetryDelay
        );
        b.start();

        return b;
    }
}
