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
import com.feedzai.commons.sql.abstraction.listeners.impl.NoopBatchListener;
import com.feedzai.commons.sql.abstraction.listeners.impl.NoopMetricsListener;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Optional;

/**
 * Abstract configuration for {@link PdbBatch} implementations, containing common properties.
 *
 * @param <PB>   The type of {@link PdbBatch} that this config class applies to.
 * @param <SELF> The concrete type of concrete {@link BatchConfig} overriding this abstract class.
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
public abstract class AbstractBatchConfig<PB extends PdbBatch, SELF extends AbstractBatchConfig<PB, SELF>> implements BatchConfig<PB> {

    /**
     * The default name of the batch, when a name isn't explicitly provided in the config builder.
     */
    public final String defaultBatchName = "Anonymous " + this.getClass().getSimpleName();

    /**
     * Constant representing the default batch size.
     *
     * @see #getBatchSize()
     */
    public static final int DEFAULT_BATCH_SIZE = 1000;

    /**
     * Constant representing the default batch timeout.
     *
     * @see #getBatchTimeout()
     */
    public static final Duration DEFAULT_BATCH_TIMEOUT = Duration.ofSeconds(1);

    /**
     * Constant representing that no retries should be attempted on batch flush failures.
     *
     * @see #getMaxFlushRetries()
     */
    public static final int NO_RETRY = 0;

    /**
     * Constant representing the default time interval to wait between batch flush retries.
     *
     * @see #getFlushRetryDelay()
     */
    public static final Duration DEFAULT_RETRY_INTERVAL = Duration.ofMillis(300);

    /**
     * @see #getName()
     */
    private final String name;

    /**
     * @see #getBatchSize()
     */
    private final int batchSize;

    /**
     * @see #getBatchTimeout()
     */
    private final Duration batchTimeout;

    /**
     * @see #getMaxAwaitTimeShutdown()
     */
    private final Duration maxAwaitTimeShutdown;

    /**
     * @see #getMaxFlushRetries()
     */
    private final int maxFlushRetries;

    /**
     * @see #getFlushRetryDelay()
     */
    private final Duration flushRetryDelay;

    /**
     * @see #getBatchListener()
     */
    private final BatchListener batchListener;

    /**
     * @see #getMetricsListener()
     */
    private final MetricsListener metricsListener;

    /**
     * @see #getConfidentialLogger()
     */
    private final Logger confidentialLogger;

    /**
     * Constructor for this {@link AbstractBatchConfig}.
     *
     * @param builder The {@link Builder} for this config.
     */
    protected AbstractBatchConfig(final AbstractBatchConfig.Builder<PB, SELF, ?> builder) {
        this.name = StringUtils.defaultIfBlank(builder.name, defaultBatchName);

        this.batchSize = Optional.ofNullable(builder.batchSize).orElse(DEFAULT_BATCH_SIZE);
        checkArgument(this.batchSize > 0, "batchSize must be > 0 (was set to %s)", this.batchSize);

        this.batchTimeout = Optional.ofNullable(builder.batchTimeout).orElse(DEFAULT_BATCH_TIMEOUT);
        checkArgument(!this.batchTimeout.isNegative(), "batchTimeout must be >= 0 (was set to %s)", builder.batchTimeout);

        this.maxAwaitTimeShutdown = builder.maxAwaitTimeShutdown;
        Optional.ofNullable(getMaxAwaitTimeShutdown()).ifPresent(maxAwaitTimeShutdown ->
                checkArgument(!this.maxAwaitTimeShutdown.isNegative(),
                        "When maxAwaitTimeShutdown is defined it must be >= 0 (was set to %s)", this.maxAwaitTimeShutdown)
        );

        this.maxFlushRetries = Optional.ofNullable(builder.maxFlushRetries).orElse(NO_RETRY);
        checkArgument(this.maxFlushRetries >= 0, "maxFlushRetries must be >= 0 (was set to %s)", this.maxFlushRetries);

        this.flushRetryDelay = Optional.ofNullable(builder.flushRetryDelay).orElse(DEFAULT_RETRY_INTERVAL);
        checkArgument(!this.flushRetryDelay.isNegative(), "flushRetryDelay must be >= 0 (was set to %s)", this.flushRetryDelay);

        this.batchListener = Optional.ofNullable(builder.batchListener).orElse(NoopBatchListener.INSTANCE);
        this.metricsListener = Optional.ofNullable(builder.metricsListener).orElse(NoopMetricsListener.INSTANCE);
        this.confidentialLogger = builder.confidentialLogger;
    }

    /**
     * Checks that configuration arguments pass the verification specified in the condition expression.
     *
     * @param condition          A boolean expression for verification of the configuration arguments.
     * @param errorMessageFormat An error message to include in the exception when the verification fails (supports
     *                           {@link String#format} parameters).
     * @param errorMessageArg    Arguments for the error message template specified in {@code errorMessageFormat}.
     * @throws IllegalArgumentException if {@code expression} is false
     */
    protected static void checkArgument(final boolean condition, final String errorMessageFormat, final Object errorMessageArg) {
        if (!condition) {
            throw new IllegalArgumentException(String.format(errorMessageFormat, errorMessageArg));
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public Duration getBatchTimeout() {
        return batchTimeout;
    }

    @Override
    public Duration getMaxAwaitTimeShutdown() {
        return maxAwaitTimeShutdown;
    }

    @Override
    public int getMaxFlushRetries() {
        return maxFlushRetries;
    }

    @Override
    public Duration getFlushRetryDelay() {
        return flushRetryDelay;
    }

    @Override
    public BatchListener getBatchListener() {
        return batchListener;
    }

    @Override
    public MetricsListener getMetricsListener() {
        return metricsListener;
    }

    @Override
    public Optional<Logger> getConfidentialLogger() {
        return Optional.ofNullable(confidentialLogger);
    }

    /**
     * A builder for the {@link AbstractBatchConfig}.
     *
     * @param <PB>   The type of {@link PdbBatch} that the config built from this builder applies to.
     * @param <BC>   The type of {@link BatchConfig} that this builder generates.
     * @param <SELF> The concrete type of concrete {@link Builder} overriding this abstract class.
     */
    public abstract static class Builder<
            PB extends PdbBatch,
            BC extends AbstractBatchConfig<PB, ?>,
            SELF extends Builder<PB, BC, SELF>
            > {

        /**
         * @see #getName()
         */
        protected String name;

        /**
         * @see #getBatchSize()
         */
        protected Integer batchSize;

        /**
         * @see #getBatchTimeout()
         */
        protected Duration batchTimeout;

        /**
         * @see #getMaxAwaitTimeShutdown()
         */
        protected Duration maxAwaitTimeShutdown;

        /**
         * @see #getMaxFlushRetries()
         */
        protected Integer maxFlushRetries;

        /**
         * @see #getFlushRetryDelay()
         */
        protected Duration flushRetryDelay;

        /**
         * @see #getBatchListener()
         */
        protected BatchListener batchListener;

        /**
         * @see #getMetricsListener()
         */
        protected MetricsListener metricsListener;

        /**
         * @see #getConfidentialLogger()
         */
        protected Logger confidentialLogger;

        /**
         * Sets a name for the batch.
         *
         * @param name The name.
         * @return This instance.
         * @see #getName()
         */
        public final SELF withName(@Nullable final String name) {
            this.name = name;
            return self();
        }

        /**
         * Sets the batch size.
         *
         * @param batchSize The batch size.
         * @return This instance.
         * @see #getBatchSize()
         */
        public final SELF withBatchSize(final int batchSize) {
            this.batchSize = batchSize;
            return self();
        }

        /**
         * Sets the batch timeout.
         *
         * @param batchTimeout The batch timeout.
         * @return This instance.
         * @see #getBatchTimeout()
         */
        public final SELF withBatchTimeout(final Duration batchTimeout) {
            this.batchTimeout = batchTimeout;
            return self();
        }

        /**
         * Sets the maximum time to wait for shutdown.
         *
         * @param maxAwaitTimeShutdown The maximum time to wait for shutdown.
         * @return This instance.
         * @see #getMaxAwaitTimeShutdown()
         */
        public final SELF withMaxAwaitTimeShutdown(final Duration maxAwaitTimeShutdown) {
            this.maxAwaitTimeShutdown = maxAwaitTimeShutdown;
            return self();
        }

        /**
         * Sets the maximum number of flush retries.
         *
         * @param maxFlushRetries The maximum flush retries.
         * @return This instance.
         * @see #getMaxFlushRetries()
         */
        public final SELF withMaxFlushRetries(final int maxFlushRetries) {
            this.maxFlushRetries = maxFlushRetries;
            return self();
        }

        /**
         * Sets the flush retry delay.
         *
         * @param flushRetryDelay The flush retry delay.
         * @return This instance.
         * @see #getFlushRetryDelay()
         */
        public final SELF withFlushRetryDelay(final Duration flushRetryDelay) {
            this.flushRetryDelay = flushRetryDelay;
            return self();
        }

        /**
         * Sets the batch listener.
         *
         * @param batchListener The batch listener.
         * @return This instance.
         * @see #getBatchListener()
         */
        public final SELF withBatchListener(final BatchListener batchListener) {
            this.batchListener = batchListener;
            return self();
        }

        /**
         * Sets the metrics listener.
         *
         * @param metricsListener The metrics listener.
         * @return This instance.
         * @see #getMetricsListener()
         */
        public final SELF withMetricsListener(final MetricsListener metricsListener) {
            this.metricsListener = metricsListener;
            return self();
        }

        /**
         * Sets the confidential logger.
         *
         * @param confidentialLogger The confidential logger.
         * @return This instance.
         * @see #getConfidentialLogger()
         */
        public final SELF withConfidentialLogger(final Logger confidentialLogger) {
            this.confidentialLogger = confidentialLogger;
            return self();
        }

        /**
         * Helper method to return the current instance, with the correct type.
         *
         * @return This instance.
         */
        protected abstract SELF self();

        /**
         * Builds the config.
         *
         * @return The concrete config.
         */
        public abstract BC build();
    }
}
