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

import java.util.Optional;

/**
 * FIXME
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
public interface BatchConfig<C extends PdbBatch> {

    Class<C> getBatchClass();

    String getName();

    int getBatchSize();

    long getBatchTimeoutMs();

    Optional<Long> getMaxAwaitTimeShutdownMsOpt();

    int getMaxFlushRetries();

    long getFlushRetryDelayMs();

    BatchListener getBatchListener();

    MetricsListener getMetricsListener();

    Optional<Logger> getConfidentialLogger();
}
