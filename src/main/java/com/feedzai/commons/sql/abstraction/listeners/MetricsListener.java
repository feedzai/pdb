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

package com.feedzai.commons.sql.abstraction.listeners;

/**
 * Listener interface to report actions for metrics.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
public interface MetricsListener {

    /**
     * Called when an entry is added to the batch.
     */
    void onEntryAdded();

    /**
     * Called when a batch flush is triggered and is not empty.
     *
     * @param flushEntriesCount The number of entries to be flushed.
     */
    void onFlush(int flushEntriesCount);

    /**
     * Called when a batch flush is finished.
     *
     * This may happen if the flush was rejected due to capacity constraints, or if it finished (either with success or
     * failure).
     *
     * @param flushEntriesCount The number of entries flushed.
     */
    void onFlushed(int flushEntriesCount);

    /**
     * Called upon batch flush failure.
     */
    void onFlushFailure();

    /**
     * Called upon batch flush success.
     *
     * @param elapsed The time elapsed (in milliseconds) since the batch flush was triggered.
     */
    void onFlushSuccess(final long elapsed);
}
