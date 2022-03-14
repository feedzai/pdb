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
 * FIXME
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
public class MultithreadedBatchConfig extends AbstractBatchConfig<MultithreadedBatch, MultithreadedBatchConfig> {

    public static final int DEFAULT_NUMBER_OF_THREADS = 1;
    public static final int DEFAULT_EXECUTOR_CAPACITY = 1000;

    private final int numberOfThreads;

    private final int executorCapacity;

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

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public int getExecutorCapacity() {
        return executorCapacity;
    }

    public static MultithreadedBatchConfigBuilder builder() {
        return new MultithreadedBatchConfigBuilder();
    }
    public static class MultithreadedBatchConfigBuilder
            extends AbstractBatchConfig.Builder<MultithreadedBatch, MultithreadedBatchConfig, MultithreadedBatchConfigBuilder> {


        private Integer numberOfThreads;
        private Integer executorCapacity;

        public MultithreadedBatchConfigBuilder withNumberOfThreads(final int numberOfThreads) {
            this.numberOfThreads = numberOfThreads;
            return self();
        }

        public MultithreadedBatchConfigBuilder withExecutorCapacity(final int executorCapacity) {
            this.executorCapacity = executorCapacity;
            return self();
        }

        /**
         * @return This instance.
         */
        protected MultithreadedBatchConfigBuilder self() {
            return this;
        }

        @Override
        public MultithreadedBatchConfig build() {
            return new MultithreadedBatchConfig(this);
        }
    }
}
