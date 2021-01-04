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

import com.feedzai.commons.sql.abstraction.dml.Expression;
import com.feedzai.commons.sql.abstraction.dml.K;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a database entity.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class DbEntity extends Expression {
    /**
     * The entity name.
     */
    private final String name;
    /**
     * The columns that compose the entity.
     */
    private final List<DbColumn> columns;
    /**
     * The list of foreign keys.
     */
    private final List<DbFk> fks;
    /**
     * The list of fields that compose the primary key.
     */
    private final List<String> pkFields;
    /**
     * The list of indexes in the entity.
     */
    private final List<DbIndex> indexes;

    /**
     * Creates a new instance of {@link DbEntity}.
     *
     * @param name     The name of the entity.
     * @param columns  The column that compose the entity.
     * @param fks      The list of foreign keys.
     * @param pkFields The list of fields that compose the primary key.
     * @param indexes  The list of indexes in the entity.
     */
    protected DbEntity(String name, List<DbColumn> columns, List<DbFk> fks, List<String> pkFields, List<DbIndex> indexes) {
        this.name = name;
        this.columns = columns;
        this.fks = fks;
        this.pkFields = pkFields;
        this.indexes = indexes;
    }

    /**
     * Gets the name of the entity.
     *
     * @return The name of the entity.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the list of columns of the entity.
     *
     * @return The list of columns of the entity.
     */
    public List<DbColumn> getColumns() {
        return columns;
    }

    /**
     * Gets the immutable list of foreign keys.
     *
     * @return The immutable list of foreign keys.
     */
    public List<DbFk> getFks() {
        return fks;
    }

    /**
     * Gets the immutable list of fields that compose the primary key.
     *
     * @return The immutable list of keys that compose the primary key.
     */
    public List<String> getPkFields() {
        return pkFields;
    }

    /**
     * Gets the immutable list of indexes.
     *
     * @return The immutable list of indexes.
     */
    public List<DbIndex> getIndexes() {
        return indexes;
    }

    /**
     * Checks if the given column is present in the list of columns.
     *
     * @param columnName The column name to check.
     * @return {@code true} if the column is present in the list of columns, {@code false} otherwise.
     */
    public boolean containsColumn(String columnName) {
        return columns.stream()
            .map(DbColumn::getName)
            .anyMatch(listColName -> listColName.equals(columnName));
    }

    /**
     * Returns a new builder out of the configuration.
     *
     * @return A new builder out of the configuration.
     */
    public Builder newBuilder() {
        return new Builder()
                .name(name)
                .addColumn(columns)
                .addFk(fks)
                .pkFields(pkFields)
                .addIndexes(indexes);
    }

    @Override
    public String translate() {
        return translator.translateCreateTable(this);
    }

    /**
     * Builder to create immutable {@link DbEntity} objects.
     */
    public static class Builder implements com.feedzai.commons.sql.abstraction.util.Builder<DbEntity>, Serializable {
        protected String name;
        protected final List<DbColumn> columns = new ArrayList<>();
        protected final List<DbFk> fks = new ArrayList<>();
        protected final List<String> pkFields = new ArrayList<>();
        protected final List<DbIndex> indexes = new ArrayList<>();

        /**
         * Sets the entity name.
         *
         * @param name The entity name.
         * @return This builder.
         */
        public Builder name(final String name) {
            this.name = name;

            return this;
        }

        /**
         * Adds a column to the entity.
         *
         * @param dbColumn The column to add.
         * @return This builder.
         */
        public Builder addColumn(final DbColumn dbColumn) {
            this.columns.add(dbColumn);

            return this;
        }

        /**
         * Removes the column with the given name.
         *
         * @param name The column name to remove.
         * @return This builder.
         */
        public Builder removeColumn(final String name) {
            final Iterator<DbColumn> iterator = columns.iterator();

            while (iterator.hasNext()) {
                final DbColumn next = iterator.next();
                if (next.getName().equals(name)) {
                    iterator.remove();
                    return this;
                }
            }

            return this;
        }

        /**
         * Adds the columns to the entity.
         *
         * @param dbColumn The columns to add.
         * @return This builder.
         */
        public Builder addColumn(final Collection<DbColumn> dbColumn) {
            this.columns.addAll(dbColumn);

            return this;
        }

        /**
         * Adds a column to the entity.
         *
         * @param name        The entity name.
         * @param type        The entity type.
         * @param constraints The list of constraints.
         * @return This builder.
         */
        public Builder addColumn(final String name, final DbColumnType type, final DbColumnConstraint... constraints) {
            addColumn(new DbColumn.Builder()
                    .name(name)
                    .addConstraints(constraints)
                    .type(type)
                    .build());

            return this;
        }

        /**
         * Adds a column to the entity.
         *
         * @param name        The entity name.
         * @param type        The entity type.
         * @param size        The type size.
         * @param constraints The list of constraints.
         * @return This builder.
         */
        public Builder addColumn(final String name, final DbColumnType type, final Integer size, final DbColumnConstraint... constraints) {
            addColumn(new DbColumn.Builder()
                    .name(name)
                    .addConstraints(constraints)
                    .type(type)
                    .size(size)
                    .build());

            return this;
        }

        /**
         * Adds a column to the entity.
         *
         * @param name        The entity name.
         * @param type        The entity type.
         * @param autoInc     {@code true} if the column is to autoincrement, {@code false} otherwise.
         * @param constraints The list of constraints.
         * @return This builder.
         */
        public Builder addColumn(final String name, final DbColumnType type, final boolean autoInc, final DbColumnConstraint... constraints) {
            addColumn(new DbColumn.Builder()
                    .name(name)
                    .addConstraints(constraints)
                    .type(type)
                    .autoInc(autoInc)
                    .build());

            return this;
        }

        /**
         * Adds a column to the entity.
         *
         * @param name         The entity name.
         * @param type         The entity type.
         * @param defaultValue The defaultValue for the column.
         * @param constraints  The list of constraints.
         * @return This builder.
         */
        public Builder addColumn(final String name, final DbColumnType type, final K defaultValue, final DbColumnConstraint... constraints) {
            addColumn(new DbColumn.Builder()
                    .name(name)
                    .addConstraints(constraints)
                    .type(type)
                    .defaultValue(defaultValue)
                    .build());

            return this;
        }

        /**
         * Adds a column to the entity.
         *
         * @param name        The entity name.
         * @param type        The entity type.
         * @param size        The type size.
         * @param autoInc     True if the column is to autoincrement, false otherwise.
         * @param constraints The list of constraints.
         * @return This builder.
         */
        public Builder addColumn(final String name, final DbColumnType type, final Integer size, final boolean autoInc, final DbColumnConstraint... constraints) {
            addColumn(new DbColumn.Builder()
                    .name(name)
                    .addConstraints(constraints)
                    .type(type).autoInc(autoInc)
                    .size(size)
                    .build());

            return this;
        }

        /**
         * Sets the PK fields.
         *
         * @param pkFields The PK fields.
         * @return This builder.
         */
        public Builder pkFields(final String... pkFields) {
            return pkFields(Arrays.asList(pkFields));
        }

        /**
         * Sets the PK fields.
         *
         * @param pkFields The PK fields.
         * @return This builder.
         */
        public Builder pkFields(final Collection<String> pkFields) {
            this.pkFields.addAll(pkFields);

            return this;
        }

        /**
         * Adds an index.
         *
         * @param index The index.
         * @return This builder.
         */
        public Builder addIndex(final DbIndex index) {
            this.indexes.add(index);

            return this;
        }

        /**
         * Adds an index.
         *
         * @param indexes The index.
         * @return This builder.
         */
        public Builder addIndexes(final Collection<DbIndex> indexes) {
            this.indexes.addAll(indexes);

            return this;
        }

        /**
         * Adds an index.
         *
         * @param unique  {@code true} if the index is unique, {@code false} otherwise.
         * @param columns The columns that are part of the index.
         * @return This builder.
         */
        public Builder addIndex(final boolean unique, final String... columns) {
            return addIndex(new DbIndex.Builder().columns(columns).unique(unique).build());
        }

        /**
         * Adds an index.
         *
         * @param columns The columns that are part of the index.
         * @return This builder.
         */
        public Builder addIndex(final String... columns) {
            return addIndex(false, columns);
        }

        /**
         * Adds an index.
         *
         * @param columns The columns that are part of the index.
         * @return This builder.
         */
        public Builder addIndex(final Collection<String> columns) {
            addIndex(new DbIndex.Builder().columns(columns).build());

            return this;
        }

        /**
         * Adds the FKs.
         *
         * @param fks The list of FKs.
         * @return This builder.
         */
        public Builder addFk(final DbFk... fks) {
            return addFk(Arrays.asList(fks));
        }

        /**
         * Adds the FKs.
         *
         * @param fks The list FKs builders..
         * @return This builder.
         */
        public Builder addFk(final DbFk.Builder... fks) {
            for (DbFk.Builder fk : fks) {
                addFk(fk.build());
            }

            return this;
        }

        /**
         * Adds the FKs.
         *
         * @param fks The list of FKs.
         * @return This builder.
         */
        public Builder addFk(final Collection<DbFk> fks) {
            this.fks.addAll(fks);

            return this;
        }

        /**
         * Clears all the FKs in this builder.
         *
         * @return This builder.
         */
        public Builder clearFks() {
            this.fks.clear();

            return this;
        }

        /**
         * Adds the FKs.
         *
         * @param fks The list of FKs.
         * @return This object.
         */
        public Builder addFks(final Collection<DbFk> fks) {
            this.fks.addAll(fks);

            return this;
        }

        @Override
        public DbEntity build() {
            return new DbEntity(
                    name,
                    ImmutableList.copyOf(columns),
                    ImmutableList.copyOf(fks),
                    ImmutableList.copyOf(pkFields),
                    ImmutableList.copyOf(indexes));
        }
    }
}
