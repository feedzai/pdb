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

import com.feedzai.commons.sql.abstraction.batch.AbstractBatch;
import com.feedzai.commons.sql.abstraction.batch.PdbBatch;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;

import javax.inject.Inject;
import java.time.Duration;
import java.util.Optional;

/**
 * The default batch implementation.
 * <p/>
 * Behind the scenes, it will periodically flush pending entries; it has auto recovery.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 * @see AbstractBatch
 */
public class DefaultBatch extends AbstractBatch implements PdbBatch {

    /**
     * Creates a new instance of {@link DefaultBatch}.
     *
     * @param de     The database engine reference.
     * @param config The batch configuration.
     * @implNote The internal timer task for periodic flushes is started.
     */
    @Inject
    public DefaultBatch(final DatabaseEngine de, final DefaultBatchConfig config) {
        super(
                de,
                config.getName(),
                config.getBatchSize(),
                config.getBatchTimeout().toMillis(),
                Optional.ofNullable(config.getMaxAwaitTimeShutdown())
                        .map(Duration::toMillis)
                        .orElse(de.getProperties().getMaximumAwaitTimeBatchShutdown()),
                config.getBatchListener(),
                config.getMaxFlushRetries(),
                config.getFlushRetryDelay().toMillis(),
                config.getConfidentialLogger().orElse(logger)
        );

        start();
    }
}
