/*
 * Copyright 2017 Feedzai
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

import com.feedzai.commons.sql.abstraction.OnFailureListener;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link AbstractBatch batch} which delegates error handling to a {@link OnFailureListener}.
 *
 * @author Helder Martins (helder.martins@feedzai.com)
 * @since 2.1.11
 */
public class FailureHandlerBatch extends AbstractBatch {

    /**
     * Creates a new instance of {@link FailureHandlerBatch}.
     *
     * @param connection            The database engine reference.
     * @param name                  The batch name.
     * @param batchSize             The batch size.
     * @param batchTimeout          The timeout.
     * @param maxAwaitTimeShutdown  The maximum await time for the batch to shutdown.
     * @param failureListener       The listener to be called when rows fail to be persisted.
     */
    protected FailureHandlerBatch(
            final DatabaseEngine connection,
            final String name,
            final int batchSize,
            final long batchTimeout,
            final long maxAwaitTimeShutdown,
            final OnFailureListener failureListener) {
        super(connection, name, batchSize, batchTimeout, maxAwaitTimeShutdown, failureListener);
    }

    @Override
    public void onFlushFailure(final BatchEntry[] entries) {
        final Set<Map<String, Serializable>> failedEvents = new HashSet<>();
        for (final BatchEntry entry : entries) {
            final Map<String, Object> eventMap = entry.getEntityEntry().getMap();
            final Map<String, Serializable> event = Maps.newHashMapWithExpectedSize(eventMap.size());

            // do not change this to collect(Collectors.toMap()) since it throws an NPE when the value is null
            eventMap.keySet().
                    forEach(name -> event.put(name, (Serializable) eventMap.get(name)));
            failedEvents.add(event);
        }

        failureListener.onFailure(failedEvents);
    }

    /**
     * Creates a new instance of {@link FailureHandlerBatch}.
     * <p>
     * Starts the timertask.
     *
     * @param connection            The database engine.
     * @param name                  The batch name.
     * @param batchSize             The batch size.
     * @param batchTimeout          The batch timeout.
     * @param maxAwaitTimeShutdown  The maximum await time for the batch to shutdown.
     * @param failureListener       The listener to be called when events fail to be persisted.
     * @return The Batch.
     */
    public static FailureHandlerBatch create(
            final DatabaseEngine connection,
            final String name,
            final int batchSize,
            final long batchTimeout,
            final long maxAwaitTimeShutdown,
            final OnFailureListener failureListener) {

        final FailureHandlerBatch batch = new FailureHandlerBatch(connection, name, batchSize, batchTimeout, maxAwaitTimeShutdown, failureListener);
        batch.start();

        return batch;
    }
}
