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

import com.feedzai.commons.sql.abstraction.batch.AbstractBatchConfig;

import java.util.Optional;

/**
 * Configuration for {@link MultithreadedBatch}.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
public class MultithreadedBatchConfig extends AbstractBatchConfig<MultithreadedBatch, MultithreadedBatchConfig> {

    /**
     * Constant representing the default number of threads.
     *
     * @see #getNumberOfThreads()
     */
    public static final int DEFAULT_NUMBER_OF_THREADS = 1;

    /**
     * Constant representing the default capacity of the batch flusher executor queue.
     *
     * @see #getExecutorCapacity()
     */
    public static final int DEFAULT_EXECUTOR_CAPACITY = 1000;

    /**
     * @see #getNumberOfThreads()
     */
    private final int numberOfThreads;

    /**
     * @see #getExecutorCapacity()
     */
    private final int executorCapacity;

    /**
     * Constructor for this {@link MultithreadedBatchConfig}.
     *
     * @param builder The {@link Builder} for this config.
     */
    protected MultithreadedBatchConfig(final MultithreadedBatchConfigBuilder builder) {
        super(builder);

        this.numberOfThreads = Optional.ofNullable(builder.numberOfThreads).orElse(DEFAULT_NUMBER_OF_THREADS);
        checkArgument(this.numberOfThreads > 0, "numberOfThreads must be > 0 (was set to %s)", this.numberOfThreads);

        this.executorCapacity = Optional.ofNullable(builder.executorCapacity).orElse(DEFAULT_EXECUTOR_CAPACITY);
        checkArgument(this.executorCapacity > 0, "executorCapacity must be > 0 (was set to %s)", this.executorCapacity);
    }

    @Override
    public Class<MultithreadedBatch> getBatchClass() {
        return MultithreadedBatch.class;
    }

    /**
     * Gets the number of threads to use for the batch flusher executor.
     * <p>
     * Each thread creates and uses its own connection to the database.
     *
     * @return The number of threads of the executor.
     */
    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    /**
     * Gets the capacity of the batch flusher executor queue.
     *
     * @return The batch flusher executor capacity.
     */
    public int getExecutorCapacity() {
        return executorCapacity;
    }

    /**
     * Returns the {@link MultithreadedBatchConfigBuilder builder} to create an instance of this class.
     *
     * @return The {@link MultithreadedBatchConfigBuilder builder}.
     */
    public static MultithreadedBatchConfigBuilder builder() {
        return new MultithreadedBatchConfigBuilder();
    }

    /**
     * The builder for the {@link MultithreadedBatchConfig}.
     */
    public static class MultithreadedBatchConfigBuilder
            extends AbstractBatchConfig.Builder<MultithreadedBatch, MultithreadedBatchConfig, MultithreadedBatchConfigBuilder> {

        /**
         * @see #getNumberOfThreads()
         */
        private Integer numberOfThreads;

        /**
         * @see #getExecutorCapacity()
         */
        private Integer executorCapacity;

        /**
         * Sets the number of threads for the executor.
         *
         * @param numberOfThreads The number of threads.
         * @return This instance.
         * @see #getNumberOfThreads()
         */
        public MultithreadedBatchConfigBuilder withNumberOfThreads(final int numberOfThreads) {
            this.numberOfThreads = numberOfThreads;
            return self();
        }

        /**
         * Sets the executor queue capacity.
         *
         * @param executorCapacity The executor capacity.
         * @return This instance.
         * @see #getExecutorCapacity()
         */
        public MultithreadedBatchConfigBuilder withExecutorCapacity(final int executorCapacity) {
            this.executorCapacity = executorCapacity;
            return self();
        }

        @Override
        protected MultithreadedBatchConfigBuilder self() {
            return this;
        }

        @Override
        public MultithreadedBatchConfig build() {
            return new MultithreadedBatchConfig(this);
        }
    }
}
