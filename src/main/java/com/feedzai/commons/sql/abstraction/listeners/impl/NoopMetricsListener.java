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

package com.feedzai.commons.sql.abstraction.listeners.impl;

import com.feedzai.commons.sql.abstraction.listeners.MetricsListener;

/**
 * A {@link NoopMetricsListener} that does nothing on the callbacks.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
public class NoopMetricsListener implements MetricsListener {

    /**
     * A singleton instance of this class.
     */
    public static final NoopMetricsListener INSTANCE = new NoopMetricsListener();

    /**
     * Private constructor to prevent direct instantiation.
     */
    private NoopMetricsListener() {
    }

    @Override
    public void onEntryAdded() {
        // do nothing
    }

    @Override
    public void onFlush(final int flushEntriesCount) {
        // do nothing
    }

    @Override
    public void onFlushed(final int flushEntriesCount) {
        // do nothing
    }

    @Override
    public void onFlushFailure() {
        // do nothing
    }

    @Override
    public void onFlushSuccess(final long elapsed) {
        // do nothing
    }
}
