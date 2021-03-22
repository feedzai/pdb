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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a database foreign key hard link
 * between two tables.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class DbFk implements Serializable {
    /**
     * The local column's names (which will compose the foreign key).
     */
    private final List<String> localColumns;

    /**
     * The referenced table's column names.
     */
    private final List<String> referencedColumns;

    /**
     * The referenced table.
     */
    private final String referencedTable;

    /**
     * Cached hashcode.
     */
    private final int hashCode;

    /**
     * Creates a new instance of {@link DbFk}.
     *
     * @param builder The builder from this class.
     */
    private DbFk(final Builder builder) {
        this.localColumns = ImmutableList.copyOf(builder.localColumns);
        this.referencedColumns = ImmutableList.copyOf(builder.referencedColumns);
        this.referencedTable = builder.referencedTable;
        this.hashCode = Objects.hash(this.localColumns, this.referencedColumns, this.referencedTable);
    }

    /**
     * Gets the local columns names.
     *
     * These columns are from the local table where the foreign key is defined (the child table); this group of columns
     * is what composes the foreign key.
     *
     * @return The local columns names.
     */
    public List<String> getLocalColumns() {
        return localColumns;
    }

    /**
     * Gets the referenced columns names.
     *
     * These columns are from the table that the foreign key constraint references (the parent table); the values in
     * these columns need to match the values in the local columns that define the foreign key.
     *
     * @return The list of referenced columns names.
     * @deprecated a "foreign key" is actually composed from the local columns of the table where it is defined (the
     * child table); this name is wrong and will eventually be removed ─ use {@link #getReferencedColumns()} instead.
     */
    @Deprecated
    public List<String> getForeignColumns() {
        return referencedColumns;
    }

    /**
     * Gets the referenced columns names.
     *
     * These columns are from the table that the foreign key constraint references (the parent table); the values in
     * these columns need to match the values in the local columns that define the foreign key.
     *
     * @return The list of referenced columns names.
     */
    public List<String> getReferencedColumns() {
        return referencedColumns;
    }

    /**
     * Gets the name of the referenced table (parent table).
     *
     * @return The name of the referenced table.
     * @deprecated a "foreign key" is actually composed from the local columns of the table where it is defined (the
     * child table); this method was used to get the name of the referenced table, which is the parent table, thus the
     * name of the method is wrong and it will eventually be removed ─ use {@link #getReferencedTable()} instead.
     */
    @Deprecated
    public String getForeignTable() {
        return referencedTable;
    }

    /**
     * Gets the name of the referenced table (parent table).
     *
     * @return The name of the referenced table.
     */
    public String getReferencedTable() {
        return referencedTable;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        final DbFk dbFk = (DbFk) obj;
        return this.localColumns.equals(dbFk.localColumns)
                && this.referencedColumns.equals(dbFk.referencedColumns)
                && this.referencedTable.equals(dbFk.referencedTable);
    }

    @Override
    public int hashCode() {
        return this.hashCode;
    }

    /**
     * Builder to create immutable {@link DbFk} objects.
     */
    public static class Builder implements com.feedzai.commons.sql.abstraction.util.Builder<DbFk>, Serializable {
        private final List<String> localColumns = new LinkedList<>();
        private final List<String> referencedColumns = new LinkedList<>();
        private String referencedTable = null;

        /**
         * Sets the name of the referenced table (parent table).
         *
         * @param referencedTable The referenced table name.
         * @return This builder.
         * @deprecated a "foreign key" is actually composed from the local columns of the table where it is defined (the
         * child table); this name is wrong and will eventually be removed ─ use {@link #referencedTable(String)}
         * instead.
         */
        @Deprecated
        public Builder foreignTable(final String referencedTable) {
            return referencedTable(referencedTable);
        }

        /**
         * Sets the name of the referenced table (parent table).
         *
         * @param referencedTable The referenced table name.
         * @return This builder.
         */
        public Builder referencedTable(final String referencedTable) {
            this.referencedTable = referencedTable;
            return this;
        }

        /**
         * Adds local columns (from the child table, where the foreign key is defined) to match the ones from the
         * referenced table (parent table).
         *
         * These columns will compose the foreign key.
         *
         * @param columns The columns.
         * @return This builder.
         */
        public Builder addColumn(final String... columns) {
            return addColumns(Arrays.asList(columns));
        }

        /**
         * Adds local columns (from the child table, where the foreign key is defined) to match the ones from the
         * referenced table (parent table).
         *
         * These columns will compose the foreign key.
         *
         * @param columns The columns.
         * @return This builder.
         */
        public Builder addColumns(final Collection<String> columns) {
            this.localColumns.addAll(columns);
            return this;
        }

        /**
         * Adds columns from the referenced table (parent table) to match the local ones.
         *
         * @param columns The columns.
         * @return This builder.
         * @deprecated a "foreign key" is actually composed from the local columns of the table where it is defined (the
         * child table); this name is wrong and will eventually be removed ─ use {@link #addReferencedColumn(String...)}
         * instead.
         */
        @Deprecated
        public Builder addForeignColumn(final String... columns) {
            return addReferencedColumn(columns);
        }

        /**
         * Adds columns from the referenced table (parent table) to match the local ones.
         *
         * @param columns The columns.
         * @return This builder.
         * @deprecated a "foreign key" is actually composed from the local columns of the table where it is defined (the
         * child table); this name is wrong and will eventually be removed ─ use {@link #addReferencedColumns(Collection)}
         * instead.
         */
        @Deprecated
        public Builder addForeignColumns(final Collection<String> columns) {
            return addReferencedColumns(columns);
        }

        /**
         * Adds columns from the referenced table (parent table) to match the local ones.
         *
         * @param columns The columns.
         * @return This builder.
         */
        public Builder addReferencedColumn(final String... columns) {
            return addReferencedColumns(Arrays.asList(columns));
        }

        /**
         * Adds columns from the referenced table (parent table) to match the local ones.
         *
         * @param columns The columns.
         * @return This builder.
         */
        public Builder addReferencedColumns(final Collection<String> columns) {
            this.referencedColumns.addAll(columns);
            return this;
        }

        @Override
        public DbFk build() {
            Preconditions.checkNotNull(referencedTable, "The referenced table can't be null.");
            Preconditions.checkArgument(localColumns.size() == referencedColumns.size(),
                    "The number of local columns and referenced columns must be the same. local: %s ; referenced: %s",
                    localColumns, referencedColumns
            );
            return new DbFk(this);
        }
    }
}
