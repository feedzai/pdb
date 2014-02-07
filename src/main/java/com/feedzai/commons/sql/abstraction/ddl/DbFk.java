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
 * Represents a database foreign key hard link
 * between two tables.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class DbFk implements Serializable {
    /**
     * The local column's names.
     */
    private final List<String> localColumns;
    /**
     * The foreign column's names.
     */
    private final List<String> foreignColumns;
    /**
     * The reference table.
     */
    private final String foreignTable;

    /**
     * Creates a new instance of {@link DbFk}.
     *
     * @param localColumns   The local columns.
     * @param foreignColumns The foreign columns.
     * @param foreignTable   The foreign table.
     */
    private DbFk(List<String> localColumns, List<String> foreignColumns, String foreignTable) {
        this.localColumns = localColumns;
        this.foreignColumns = foreignColumns;
        this.foreignTable = foreignTable;
    }

    /**
     * Gets the local columns names.
     *
     * @return The local columns names.
     */
    public List<String> getLocalColumns() {
        return localColumns;
    }

    /**
     * Gets he list of foreign column names.
     *
     * @return The list of foreign column names.
     */
    public List<String> getForeignColumns() {
        return foreignColumns;
    }

    /**
     * Gets the name of the foreign table name.
     *
     * @return The name of the foreign table name.
     */
    public String getForeignTable() {
        return foreignTable;
    }

    /**
     * Builder to create immutable {@link DbFk} objects.
     */
    public static class Builder implements com.feedzai.commons.sql.abstraction.util.Builder<DbFk>, Serializable {
        private final List<String> localColumns = new ArrayList<>();
        private final List<String> foreignColumns = new ArrayList<>();
        private String foreignTable = null;

        /**
         * Sets the foreign table name.
         *
         * @param foreignTable The foreign table name.
         * @return This builder.
         */
        public Builder foreignTable(final String foreignTable) {
            this.foreignTable = foreignTable;

            return this;
        }

        /**
         * Adds local columns to match the foreign.
         *
         * @param columns The columns.
         * @return This builder.
         */
        public Builder addColumn(final String... columns) {
            return addColumns(Arrays.asList(columns));
        }

        /**
         * Adds local columns to match the foreign.
         *
         * @param columns The columns.
         * @return This builder.
         */
        public Builder addColumns(final Collection<String> columns) {
            this.localColumns.addAll(columns);

            return this;
        }

        /**
         * Adds foreign columns to match the local ones.
         *
         * @param foreignColumns The columns.
         * @return This builder.
         */
        public Builder addForeignColumn(final String... foreignColumns) {
            return addForeignColumns(Arrays.asList(foreignColumns));
        }

        /**
         * Adds foreign columns to match the local ones.
         *
         * @param columns The columns.
         * @return This builder.
         */
        public Builder addForeignColumns(final Collection<String> columns) {
            this.foreignColumns.addAll(columns);

            return this;
        }

        @Override
        public DbFk build() {
            return new DbFk(ImmutableList.copyOf(localColumns), ImmutableList.copyOf(foreignColumns), foreignTable);
        }
    }
}
