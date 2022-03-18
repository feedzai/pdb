/*
 * Copyright 2021 Feedzai
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

import com.feedzai.commons.sql.abstraction.batch.BatchEntry;

/**
 * Listener interface to add behavior after executing batch operations on databases (e.g. write rows to file).
 *
 * @author João Fernandes (joao.fernandes@feedzai.com)
 * @implSpec The method calls on the listener should not block nor throw any exceptions.
 * @since 2.8.1
 */
public interface BatchListener {

    /**
     * Callback indicating that one or more rows have failed to be persisted.
     *
     * @param rowsFailed An array of {@link BatchEntry entries} with the row or rows that failed to be persisted.
     */
    void onFailure(BatchEntry[] rowsFailed);

    /**
     * Callback indicating that one or more rows have succeeded to be persisted.
     *
     * @param rowsSucceeded An array of {@link BatchEntry entries} with the row or rows that succeeded to be persisted.
     */
    void onSuccess(BatchEntry[] rowsSucceeded);
}
