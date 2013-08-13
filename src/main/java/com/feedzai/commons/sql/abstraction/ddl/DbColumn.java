/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.ddl;

import com.feedzai.commons.sql.abstraction.dml.K;
import com.google.common.base.Objects;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The database column.
 */
public class DbColumn implements Serializable {
    /**
     * The column name.
     */
    private String name;
    /**
     * The column type.
     */
    private DbColumnType dbColumnType;
    /**
     * The size (if applicable).
     */
    private Integer size = null;
    /**
     * The list of constraints.
     */
    private final List<DbColumnConstraint> columnConstraints = new ArrayList<DbColumnConstraint>();
    /**
     * Indicates if the column is to auto increment or not.
     */
    private boolean autoInc = false;
    /**
     * The default value of the column.
     */
    private K defaultValue;

    /**
     * Creates a new instance of {@link DbColumn}.
     */
    public DbColumn() {
    }

    /**
     * Creates a new instance of {@link DbColumn}.
     *
     * @param name         The column name.
     * @param dbColumnType The column type.
     */
    public DbColumn(String name, DbColumnType dbColumnType) {
        this(name, dbColumnType, false);
    }

    /**
     * Creates a new instance of {@link DbColumn}.
     *
     * @param name         The column name.
     * @param dbColumnType The column type.
     * @param autoInc      True to use auto incrementation, false otherwise.
     */
    public DbColumn(String name, DbColumnType dbColumnType, boolean autoInc) {
        this.name = name;
        this.dbColumnType = dbColumnType;
        this.autoInc = autoInc;
    }

    /**
     * Creates a new instance of {@link DbColumn}.
     *
     * @param name         The column name.
     * @param dbColumnType The column type.
     * @param size         The size.
     */
    public DbColumn(String name, DbColumnType dbColumnType, Integer size) {
        this.name = name;
        this.dbColumnType = dbColumnType;
        this.size = size;
    }

    /**
     * Creates a new instance of {@link DbColumn}.
     *
     * @param name         The column name.
     * @param dbColumnType The column type.
     * @param defaultValue The default value.
     */
    public DbColumn(String name, DbColumnType dbColumnType, K defaultValue) {
        this.name = name;
        this.dbColumnType = dbColumnType;
        this.defaultValue = defaultValue;
    }

    /**
     * Sets the column name.
     *
     * @param name The column name.
     * @return This object.
     */
    public DbColumn setName(final String name) {
        this.name = name;

        return this;
    }

    /**
     * Sets the size of the type (if applicable).
     *
     * @param size The size.
     * @return This object.
     */
    public DbColumn setSize(final Integer size) {
        this.size = size;

        return this;
    }

    /**
     * Sets the column type.
     *
     * @param dbColumnType The column type.
     * @return This object.
     */
    public DbColumn setType(final DbColumnType dbColumnType) {
        this.dbColumnType = dbColumnType;

        return this;
    }

    /**
     * Adds a new constraint to this column.
     *
     * @param dbColumnConstraint The new constraint.
     * @return This object.
     */
    public DbColumn addConstraint(final DbColumnConstraint dbColumnConstraint) {
        this.columnConstraints.add(dbColumnConstraint);

        return this;
    }

    /**
     * Adds constraints.
     *
     * @param constraints An array of constraints.
     * @return This object.
     */
    public DbColumn addConstraints(final DbColumnConstraint... constraints) {
        if (constraints == null) {
            return this;
        }

        this.columnConstraints.addAll(Arrays.asList(constraints));

        return this;
    }

    /**
     * Sets this field to use auto incrementation techniques.
     *
     * @param autoInc True to use auto incrementation, false otherwise.
     * @return This object.
     */
    public DbColumn setAutoInc(final boolean autoInc) {
        this.autoInc = autoInc;

        return this;
    }

    /**
     * @return True if it is auto inc, false otherwise.
     */
    public boolean isAutoInc() {
        return autoInc;
    }

    /**
     * @return The column constraints.
     */
    public List<DbColumnConstraint> getColumnConstraints() {
        return columnConstraints;
    }

    /**
     * @return The column type.
     */
    public DbColumnType getDbColumnType() {
        return dbColumnType;
    }

    /**
     * @return The column name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return The size.
     */
    public Integer getSize() {
        return size;
    }

    /**
     * @return True if the size is set, false otherwise.
     */
    public boolean isSizeSet() {
        return size != null;
    }

    /**
     * @return The default value of the column.
     */
    public K getDefaultValue() {
        return defaultValue;
    }

    /**
     * Sets The default value of the column.
     *
     * @param defaultValue The default value of the column.
     */
    public DbColumn setDefaultValue(K defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    /**
     * @return {@code true} if the default value of the column is set, {@code false} otherwise.
     */
    public boolean isDefaultValueSet() {
        return defaultValue != null;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, dbColumnType, size, columnConstraints, autoInc);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final DbColumn other = (DbColumn) obj;
        return Objects.equal(this.name, other.name) && Objects.equal(this.dbColumnType, other.dbColumnType) && Objects.equal(this.size, other.size) && Objects.equal(this.columnConstraints, other.columnConstraints) && Objects.equal(this.autoInc, other.autoInc);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("dbColumnType", dbColumnType)
                .add("size", size)
                .add("columnConstraints", columnConstraints)
                .add("autoInc", autoInc)
                .add("defaultValue", defaultValue)
                .toString();
    }
}
