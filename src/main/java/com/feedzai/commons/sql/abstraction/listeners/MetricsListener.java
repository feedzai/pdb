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
 * @implSpec The method calls on the listener should not block nor throw any exceptions.
 */
public interface MetricsListener {

    /**
     * Called when an entry is added to the batch.
     */
    void onEntryAdded();

    /**
     * Called when a batch flush is triggered.
     * <p>
     * The batch may be empty, in which case it will skip the persistence to database.
     */
    void onFlushTriggered();

    /**
     * Called when a batch flush is not empty and starts the process to persist entries in the database, eventually
     * after waiting for other flush operations to finish.
     *
     * @param elapsed           The time elapsed (in milliseconds) since the batch flush was triggered.
     * @param flushEntriesCount The number of entries to be flushed.
     */
    void onFlushStarted(long elapsed, int flushEntriesCount);

    /**
     * Called when a batch flush is finished.
     * <p>
     * This should be called for all cases:
     * <ul>
     *     <li>flush finished immediately because it was empty</li>
     *     <li>flush was rejected due to executor capacity constraints</li>
     *     <li>flush finished after all entries were sent to the database (either all or some succeeded, or all failed)</li>
     * </ul>
     *
     * @param elapsed                The time elapsed (in milliseconds) since the batch flush was triggered.
     * @param successfulEntriesCount The number of entries flushed successfully.
     * @param failedEntriesCount     The number of entries that failed to be flushed.
     */
    void onFlushFinished(long elapsed, int successfulEntriesCount, int failedEntriesCount);
}
