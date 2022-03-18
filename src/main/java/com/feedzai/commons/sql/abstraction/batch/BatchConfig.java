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

package com.feedzai.commons.sql.abstraction.batch;

import com.feedzai.commons.sql.abstraction.listeners.BatchListener;
import com.feedzai.commons.sql.abstraction.listeners.MetricsListener;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Optional;

/**
 * Interface specifying a configuration for a {@link PdbBatch}.
 *
 * @param <C> The type of {@link PdbBatch} that this config class applies to.
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
public interface BatchConfig<C extends PdbBatch> {

    /**
     * Gets the concrete class of the {@link PdbBatch} implementation that this config belongs to.
     *
     * @return The batch class.
     */
    @Nonnull
    Class<C> getBatchClass();

    /**
     * Gets the name to use for the batch.
     *
     * @return The batch name.
     */
    @Nonnull
    String getName();

    /**
     * Gets the size of the batch; when the number of pending entries reaches this number, the batch should flush.
     *
     * @return The batch size.
     */
    int getBatchSize();

    /**
     * Gets the batch timeout.
     * <p>
     * Each time there aren't enough pending entries as specified in the {@link #getBatchSize()} to trigger a flush,
     * then a flush should be triggered by timeout, when the time specified by this setting has elapsed since the last
     * flush.
     *
     * @return The batch timeout.
     */
    @Nonnull
    Duration getBatchTimeout();

    /**
     * Gets the maximum time to wait for the batch to shutdown.
     *
     * @return The maximum await time for batch shutdown.
     * @implNote This can be {@code null}, in which case the batch implementation should use the value specified in the
     * PDB property {@link com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties#MAXIMUM_TIME_BATCH_SHUTDOWN}.
     */
    @Nullable
    Duration getMaxAwaitTimeShutdown();

    /**
     * Gets the number of times to retry a batch flush upon failure. When set to 0, no retries will be attempted.
     *
     * @return The maximum number of batch flush retries.
     */
    int getMaxFlushRetries();

    /**
     * Gets the time interval to wait between batch flush retries.
     *
     * @return The time delay between flush retries.
     */
    @Nonnull
    Duration getFlushRetryDelay();

    /**
     * The listener that will be invoked whenever some batch operation fail or succeeds to persist.
     *
     * @return The {@link BatchListener}.
     */
    @Nonnull
    BatchListener getBatchListener();

    /**
     * The listener that will be invoked whenever some measurable event occurs, so that it can be captured and reported
     * in monitoring metrics.
     *
     * @return The {@link MetricsListener}.
     */
    @Nonnull
    MetricsListener getMetricsListener();

    /**
     * Gets an optional logger to log messages potentially containing sensitive data.
     *
     * @return The {@link Optional} confidential logger.
     */
    @Nonnull
    Optional<Logger> getConfidentialLogger();
}
