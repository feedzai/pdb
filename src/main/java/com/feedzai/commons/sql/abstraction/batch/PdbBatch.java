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

import com.feedzai.commons.sql.abstraction.entry.EntityEntry;

import java.util.concurrent.CompletableFuture;

/**
 * Interface specifying a batch that periodically flushes pending insertions to the database.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 * @implSpec Implementations are expected to have a constructor annotated with {@link javax.inject.Inject}, so that
 * instances can be generated from config using Guice injection.
 */
public interface PdbBatch extends AutoCloseable {

    /**
     * Adds the entry to the batch.
     *
     * @param entityName The table name.
     * @param ee         The entity entry.
     * @throws Exception If an error occurs while adding to the batch.
     */
    void add(String entityName, EntityEntry ee) throws Exception;

    /**
     * Adds the entry to the batch.
     *
     * @param batchEntry The batch entry.
     * @throws Exception If an error occurs while adding to the batch.
     */
    void add(final BatchEntry batchEntry) throws Exception;

    /**
     * Flushes the pending batches.
     *
     * @throws Exception If an error occurs while flushing.
     */
    void flush() throws Exception;

    /**
     * Flushes the pending batches asynchronously, if the implementation allows it; otherwise, by default, this should
     * simply call {@link #flush()} and return the future when done.
     *
     * @return A void {@link CompletableFuture} that completes when the flush action finishes.
     */
    CompletableFuture<Void> flushAsync() throws Exception;

    /**
     * Flushes the pending batches upserting entries to avoid duplicated key violations.
     *
     * @throws Exception If an error occurs while flushing.
     */
    void flushUpsert() throws Exception;

}
