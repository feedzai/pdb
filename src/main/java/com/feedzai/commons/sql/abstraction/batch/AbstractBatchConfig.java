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
import com.google.common.base.Strings;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * FIXME
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
public abstract class AbstractBatchConfig<PB extends PdbBatch, SELF extends AbstractBatchConfig<PB, SELF>> implements BatchConfig<PB> {

    public static final String DEFAULT_BATCH_NAME = "Anonymous Batch";

    /**
     * Constant representing that no retries should be attempted on batch flush failures.
     */
    public static final int DEFAULT_BATCH_SIZE = 1000;

    /**
     * Constant representing that no retries should be attempted on batch flush failures.
     */
    public static final long DEFAULT_BATCH_TIMEOUT_MS = 1000;

    /**
     * Constant representing that no retries should be attempted on batch flush failures.
     */
    public static final int NO_RETRY = 0;

    /**
     * Constant representing the default time interval (milliseconds) to wait between batch flush retries.
     */
    public static final long DEFAULT_RETRY_INTERVAL_MS = 300;

    private final String name;
    private final int batchSize;
    private final long batchTimeoutMs;
    private final Long maxAwaitTimeShutdownMs;
    private final int maxFlushRetries;
    private final long flushRetryDelayMs;
    private final BatchListener batchListener;
    private final MetricsListener metricsListener;
    private final Logger confidentialLogger;

    protected AbstractBatchConfig(final AbstractBatchConfig.Builder<PB, SELF, ?> builder) {
        this.name = Strings.isNullOrEmpty(builder.name) ? DEFAULT_BATCH_NAME : builder.name;

        this.batchSize = Optional.ofNullable(builder.batchSize).orElse(DEFAULT_BATCH_SIZE);
        checkArgument(this.batchSize > 0, "batchSize must be > 0 (was set to %s)", this.batchSize);

        this.batchTimeoutMs = Optional.ofNullable(builder.batchTimeoutMs).orElse(DEFAULT_BATCH_TIMEOUT_MS);
        checkArgument(this.batchTimeoutMs >= 0, "batchTimeoutMs must be >= 0 (was set to %s)", this.batchTimeoutMs);

        this.maxAwaitTimeShutdownMs = builder.maxAwaitTimeShutdownMs;
        getMaxAwaitTimeShutdownMsOpt().ifPresent(maxAwaitTimeShutdown ->
                checkArgument(maxAwaitTimeShutdown >= 0,
                        "When maxAwaitTimeShutdownMs is defined it must be >= 0 (was set to %s)", this.maxAwaitTimeShutdownMs)
        );

        this.maxFlushRetries = Optional.ofNullable(builder.maxFlushRetries).orElse(NO_RETRY);
        checkArgument(this.maxFlushRetries >= 0, "maxFlushRetries must be >= 0 (was set to %s)", this.maxFlushRetries);

        this.flushRetryDelayMs = Optional.ofNullable(builder.flushRetryDelayMs).orElse(DEFAULT_RETRY_INTERVAL_MS);
        checkArgument(this.flushRetryDelayMs >= 0, "flushRetryDelayMs must be >= 0 (was set to %s)", this.flushRetryDelayMs);

        this.batchListener = Optional.ofNullable(builder.batchListener).orElse(NoopBatchListener.INSTANCE);
        this.metricsListener = Optional.ofNullable(builder.metricsListener).orElse(NoopMetricsListener.INSTANCE);
        this.confidentialLogger = builder.confidentialLogger;
    }

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
    public long getBatchTimeoutMs() {
        return batchTimeoutMs;
    }

    @Override
    public Optional<Long> getMaxAwaitTimeShutdownMsOpt() {
        return Optional.ofNullable(maxAwaitTimeShutdownMs);
    }

    @Override
    public int getMaxFlushRetries() {
        return maxFlushRetries;
    }

    @Override
    public long getFlushRetryDelayMs() {
        return flushRetryDelayMs;
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

    public abstract static class Builder<PB extends PdbBatch, BC extends AbstractBatchConfig<PB, ?>, SELF extends Builder<PB, BC, SELF>> {

        protected String name;
        protected Integer batchSize;
        protected Long batchTimeoutMs;
        protected Long maxAwaitTimeShutdownMs;
        protected Integer maxFlushRetries;
        protected Long flushRetryDelayMs;
        protected BatchListener batchListener;
        protected MetricsListener metricsListener;
        protected Logger confidentialLogger;

        public final SELF withName(@Nullable final String name) {
            this.name = name;
            return self();
        }

        public final SELF withBatchSize(final int batchSize) {
            this.batchSize = batchSize;
            return self();
        }

        public final SELF withBatchTimeoutMs(final long batchTimeoutMs) {
            this.batchTimeoutMs = batchTimeoutMs;
            return self();
        }

        public final SELF withMaxAwaitTimeShutdownMs(final long maxAwaitTimeShutdownMs) {
            this.maxAwaitTimeShutdownMs = maxAwaitTimeShutdownMs;
            return self();
        }

        public final SELF withMaxFlushRetries(final int maxFlushRetries) {
            this.maxFlushRetries = maxFlushRetries;
            return self();
        }

        public final SELF withFlushRetryDelayMs(final long flushRetryDelayMs) {
            this.flushRetryDelayMs = flushRetryDelayMs;
            return self();
        }

        public final SELF withBatchListener(final BatchListener batchListener) {
            this.batchListener = batchListener;
            return self();
        }

        public final SELF withMetricsListener(final MetricsListener metricsListener) {
            this.metricsListener = metricsListener;
            return self();
        }

        public final SELF withConfidentialLogger(final Logger confidentialLogger) {
            this.confidentialLogger = confidentialLogger;
            return self();
        }

        /**
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
