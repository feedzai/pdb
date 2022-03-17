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
        return CompletableFuture.runAsync(() -> {
            try {
                flush();
            } catch (final RuntimeException e) {
                throw e;
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
