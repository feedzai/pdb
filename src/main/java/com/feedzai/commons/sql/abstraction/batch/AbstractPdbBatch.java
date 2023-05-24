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

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;

/**
 * A abstract {@link PdbBatch} with useful default base methods for concrete implementations.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
public abstract class AbstractPdbBatch implements PdbBatch {

    @Override
    public void add(final String entityName, final EntityEntry ee) throws Exception {
        add(new BatchEntry(entityName, ee));
    }

    @Override
    public CompletableFuture<Void> flushAsync() {
        final CompletableFuture<Void> flushFuture = new CompletableFuture<>();
        try {
            flush();
            flushFuture.complete(null);
        } catch (final Exception e) {
            flushFuture.completeExceptionally(e);
        }

        return flushFuture;
    }

    /**
     * Processes all batch entries.
     * <p>
     * This is done by starting a transaction, adding all {@link BatchEntry batch entries} to their respective prepared
     * statements, flushing them and finally performing a commit on the transaction (which will finish that transaction).
     *
     * @param de           The {@link DatabaseEngine} on which to perform the flush.
     * @param batchEntries The list of batch entries to be flushed.
     * @throws DatabaseEngineException If the operation failed.
     */
    protected void processBatch(final DatabaseEngine de, final List<BatchEntry> batchEntries) throws DatabaseEngineException {
        /*
         Begin transaction before the addBatch calls, in order to force the retry of the connection if it was lost during
         or since the last batch. Otherwise, the addBatch call that uses a prepared statement will fail.
         */
        de.beginTransaction();

        for (final BatchEntry entry : batchEntries) {
            de.addBatch(entry.getTableName(), entry.getEntityEntry());
        }

        de.flush();
        de.commit();
    }

    /**
     * Processes all batch entries ignoring duplicate entries.
     *
     * @implSpec Same as {@link #processBatch(DatabaseEngine, List)}}.
     *
     * @param de                        The {@link DatabaseEngine} on which to perform the flush.
     * @param batchEntries              The list of batch entries to be flushed.
     * @throws DatabaseEngineException  If the operation failed.
     */
    protected void processBatchIgnoring(final DatabaseEngine de, final List<BatchEntry> batchEntries) throws DatabaseEngineException {
        /*
         Begin transaction before the addBatch calls, in order to force the retry of the connection if it was lost during
         or since the last batch. Otherwise, the addBatch call that uses a prepared statement will fail.
         */
        de.beginTransaction();

        for (final BatchEntry entry : batchEntries) {
            de.addBatchIgnore(entry.getTableName(), entry.getEntityEntry());
        }

        de.flushIgnore();
        de.commit();
    }

}
