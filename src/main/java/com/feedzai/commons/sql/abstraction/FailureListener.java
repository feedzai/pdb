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
package com.feedzai.commons.sql.abstraction;

import com.feedzai.commons.sql.abstraction.batch.BatchEntry;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Listener interface to add behavior when there is some failure executing batch
 * operations on databases (e.g. write rows to file).
 *
 * @author Helder Martins (helder.martins@feedzai.com).
 * @since 2.1.11
 */
@FunctionalInterface
public interface FailureListener {

    /**
     * Callback indicating that one or more rows have failed to be persisted.
     *
     * @param rowsFailed An array of {@link BatchEntry entries} with the row or rows that failed to be persisted.
     */
    void onFailure(BatchEntry[] rowsFailed);
}
