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

/**
 * Configuration for {@link DefaultBatch}.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
public class DefaultBatchConfig extends AbstractBatchConfig<DefaultBatch, DefaultBatchConfig> {

    /**
     * Constructor for this {@link DefaultBatchConfig}.
     *
     * @param builder The {@link Builder} for this config.
     */
    protected DefaultBatchConfig(final DefaultBatchConfigBuilder builder) {
        super(builder);
    }

    @Override
    public Class<DefaultBatch> getBatchClass() {
        return DefaultBatch.class;
    }

    /**
     * Returns the {@link DefaultBatchConfigBuilder builder} to create an instance of this class.
     *
     * @return The {@link DefaultBatchConfigBuilder builder}.
     */
    public static DefaultBatchConfigBuilder builder() {
        return new DefaultBatchConfigBuilder();
    }

    /**
     * The builder for the {@link DefaultBatchConfig}.
     */
    public static class DefaultBatchConfigBuilder extends Builder<DefaultBatch, DefaultBatchConfig, DefaultBatchConfigBuilder> {

        @Override
        protected DefaultBatchConfigBuilder self() {
            return this;
        }

        @Override
        public DefaultBatchConfig build() {
            return new DefaultBatchConfig(this);
        }
    }
}
