/*
 * Copyright 2014 Feedzai
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
package com.feedzai.commons.sql.abstraction.ddl;

import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


/**
 * Represents a database index.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class DbIndex implements Serializable {
    /**
     * The columns that are part of the index.
     */
    private final List<String> columns;
    /**
     * Specifies if the index is unique or not.
     */
    private final boolean unique;

    /**
     * Creates a new instance of {@link DbIndex}.
     *
     * @param columns The column names that are part of the index.
     * @param unique  {@code true} if the index is unique, {@code false} otherwise.
     */
    private DbIndex(List<String> columns, boolean unique) {
        this.columns = columns;
        this.unique = unique;
    }

    /**
     * Gets the list of columns that are part of the index,
     *
     * @return The list of columns that are part of the index.
     */
    public List<String> getColumns() {
        return columns;
    }

    /**
     * Checks if the index is unique.
     *
     * @return {@code true} if the index is unique, {@code false} otherwise.
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * Builder to create immutable {@link DbIndex} objects.
     */
    public static class Builder implements com.feedzai.commons.sql.abstraction.util.Builder<DbIndex>, Serializable {
        private final List<String> columns = new ArrayList<>();
        private boolean unique = false;

        /**
         * Add the columns that are part of the index.
         *
         * @param columns The columns.
         * @return This builder.
         */
        public Builder columns(String... columns) {
            if (columns == null) {
                return this;
            }

            return columns(Arrays.asList(columns));
        }

        /**
         * Add the columns that are part of the index.
         *
         * @param collection The columns.
         * @return This builder.
         */
        public Builder columns(final Collection<String> collection) {
            this.columns.addAll(collection);

            return this;
        }

        /**
         * Indicates if this index is unique or not.
         *
         * @param unique True if it is unique, false otherwise.
         * @return This builder.
         */
        public Builder unique(final boolean unique) {
            this.unique = unique;

            return this;
        }

        @Override
        public DbIndex build() {
            return new DbIndex(ImmutableList.copyOf(columns), unique);
        }
    }
}
