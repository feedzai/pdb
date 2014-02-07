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

import com.feedzai.commons.sql.abstraction.dml.K;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Represents a database column definition.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class DbColumn implements Serializable {
    /**
     * The column name.
     */
    private final String name;
    /**
     * The column type.
     */
    private final DbColumnType dbColumnType;
    /**
     * The size if applicable (e.g. for var char).
     */
    private final Integer size;
    /**
     * The list of constraints.
     */
    private final List<DbColumnConstraint> columnConstraints;
    /**
     * {@code true} if the column is auto incremental.
     */
    private final boolean autoInc;
    /**
     * The default value if applicable.
     */
    private final K defaultValue;

    /**
     * Creates a new instance of {@link DbColumn}.
     *
     * @param name              The column name.
     * @param dbColumnType      The column type.
     * @param size              The size if applicable.
     * @param columnConstraints The list of constraints.
     * @param autoInc           True to use auto incrementation, false otherwise.
     * @param defaultValue      The default value if applicable.
     */
    private DbColumn(String name, DbColumnType dbColumnType, Integer size, List<DbColumnConstraint> columnConstraints, boolean autoInc, K defaultValue) {
        this.name = name;
        this.dbColumnType = dbColumnType;
        this.size = size;
        this.columnConstraints = columnConstraints;
        this.autoInc = autoInc;
        this.defaultValue = defaultValue;
    }

    /**
     * Gets column name.
     *
     * @return The column name.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the column type.
     *
     * @return The column type.
     */
    public DbColumnType getDbColumnType() {
        return dbColumnType;
    }

    /**
     * Gets the size of the column if applicable (e.g. VARCHAR), {@code null} is returned
     * when not applicable.
     *
     * @return The size or {@code null} if not applicable.
     */
    public Integer getSize() {
        return size;
    }

    /**
     * Gets the immutable list of constraints in the column.
     *
     * @return The immutable list of column constraints.
     */
    public List<DbColumnConstraint> getColumnConstraints() {
        return columnConstraints;
    }

    /**
     * Signals whether this column is auto incremental or not.
     *
     * @return {@code true} if the column is auto incremental, {@code false} otherwise.
     */
    public boolean isAutoInc() {
        return autoInc;
    }

    /**
     * Gets the default value, {@code null} when not applicable.
     *
     * @return The default value.
     */
    public K getDefaultValue() {
        return defaultValue;
    }

    /**
     * Signals if the default value is set or not.
     *
     * @return {@code true} if the default value is set, {@code false} otherwise.
     */
    public boolean isDefaultValueSet() {
        return defaultValue != null;
    }

    /**
     * Signals if the size is set or not.
     *
     * @return {@code true} if the size is set, {@code false} otherwise.
     */
    public boolean isSizeSet() {
        return size != null;
    }

    /**
     * Returns a new builder out of this configuration.
     *
     * @return a new builder out of this configuration.
     */
    public Builder newBuilder() {
        return new Builder()
                .name(name)
                .type(dbColumnType)
                .size(size)
                .addConstraints(columnConstraints)
                .autoInc(autoInc)
                .defaultValue(defaultValue);
    }

    /**
     * Builder for creating immutable {@link DbColumn} objects.
     */
    public static class Builder implements com.feedzai.commons.sql.abstraction.util.Builder<DbColumn>, Serializable {
        private String name;
        private DbColumnType dbColumnType;
        private Integer size = null;
        private final List<DbColumnConstraint> columnConstraints = new ArrayList<>();
        private boolean autoInc = false;
        private K defaultValue = null;

        /**
         * Sets the column name.
         *
         * @param name The column name.
         * @return This builder.
         */
        public Builder name(final String name) {
            this.name = name;

            return this;
        }

        /**
         * Sets the size of the type if applicable (e.g. VARCHAR).
         *
         * @param size The size.
         * @return This builder.
         */
        public Builder size(final Integer size) {
            this.size = size;

            return this;
        }

        /**
         * Sets the column type.
         *
         * @param dbColumnType The column type.
         * @return This builder.
         */
        public Builder type(final DbColumnType dbColumnType) {
            this.dbColumnType = dbColumnType;

            return this;
        }

        /**
         * Adds a new constraint to this column.
         *
         * @param dbColumnConstraint The new constraint.
         * @return This builder.
         */
        public Builder addConstraint(final DbColumnConstraint dbColumnConstraint) {
            this.columnConstraints.add(dbColumnConstraint);

            return this;
        }

        /**
         * Adds constraints.
         *
         * @param constraints The column constraints.
         * @return This builder.
         */
        public Builder addConstraints(final DbColumnConstraint... constraints) {
            if (constraints == null) {
                return this;
            }

            return addConstraints(Arrays.asList(constraints));
        }

        /**
         * Adds constraints.
         *
         * @param constraints A collection of column constraints.
         * @return This builder.
         */
        public Builder addConstraints(final Collection<DbColumnConstraint> constraints) {
            this.columnConstraints.addAll(constraints);

            return this;
        }

        /**
         * Sets this field to use auto incrementation techniques.
         *
         * @param autoInc {@code true} to use auto incrementation, {@code false} otherwise.
         * @return This builder.
         */
        public Builder autoInc(final boolean autoInc) {
            this.autoInc = autoInc;

            return this;
        }

        /**
         * Sets the default value.
         *
         * @return The builder.
         */
        public Builder defaultValue(K defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        @Override
        public DbColumn build() {
            return new DbColumn(
                    name,
                    dbColumnType,
                    size,
                    ImmutableList.copyOf(columnConstraints),
                    autoInc,
                    defaultValue);
        }
    }
}
