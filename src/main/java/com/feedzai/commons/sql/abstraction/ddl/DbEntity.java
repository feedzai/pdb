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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Builds a new database entity abstraction.
 */
public class DbEntity implements Serializable {

    /**
     * The entity name.
     */
    private String name = null;
    /**
     * The list of {@link DbColumn}.
     */
    private final List<DbColumn> columns = new ArrayList<DbColumn>();
    /**
     * The list of {@link DbFk}.
     */
    private final List<DbFk> fks = new ArrayList<DbFk>();
    /**
     * The list of fields that make the primary key.
     */
    private String[] pkFields = new String[0];
    /**
     * The indexes.
     */
    private final List<DbIndex> indexes = new ArrayList<DbIndex>();

    /**
     * Creates a new instance of {@link DbEntity}.
     */
    public DbEntity() {
    }

    /**
     * Sets the entity name.
     *
     * @param name The name.
     * @return This object.
     */
    public DbEntity setName(final String name) {

        this.name = name;

        return this;
    }

    /**
     * Adds a column to the entity.
     *
     * @param dbColumn The column.
     * @return This object.
     */
    public DbEntity addColumn(final DbColumn dbColumn) {
        this.columns.add(dbColumn);

        return this;
    }

    /**
     * Adds the columns to the entity.
     *
     * @param dbColumn The columns.
     * @return This object.
     */
    public DbEntity addColumn(final Collection<DbColumn> dbColumn) {
        this.columns.addAll(dbColumn);

        return this;
    }

    /**
     * Adds a column to the entity.
     *
     * @param name        The entity name.
     * @param type        The entity type.
     * @param constraints The list of constraints.
     * @return This object.
     */
    public DbEntity addColumn(final String name, final DbColumnType type, final DbColumnConstraint... constraints) {
        addColumn(new DbColumn().setName(name).addConstraints(constraints).setType(type));

        return this;
    }

    /**
     * Adds a column to the entity.
     *
     * @param name        The entity name.
     * @param type        The entity type.
     * @param size        The type size.
     * @param constraints The list of constraints.
     * @return This object.
     */
    public DbEntity addColumn(final String name, final DbColumnType type, final Integer size, final DbColumnConstraint... constraints) {
        addColumn(new DbColumn().setName(name).addConstraints(constraints).setType(type).setSize(size));

        return this;
    }

    /**
     * Adds a column to the entity.
     *
     * @param name        The entity name.
     * @param type        The entity type.
     * @param autoInc     True if the column is to autoincrement, false otherwise.
     * @param constraints The list of constraints.
     * @return This object.
     */
    public DbEntity addColumn(final String name, final DbColumnType type, final boolean autoInc, final DbColumnConstraint... constraints) {
        addColumn(new DbColumn().setName(name).addConstraints(constraints).setType(type).setAutoInc(autoInc));

        return this;
    }

    /**
     * Adds a column to the entity.
     *
     * @param name         The entity name.
     * @param type         The entity type.
     * @param defaultValue The defaultValue for the column.
     * @param constraints  The list of constraints.
     * @return This object.
     */
    public DbEntity addColumn(final String name, final DbColumnType type, final K defaultValue, final DbColumnConstraint... constraints) {
        addColumn(new DbColumn().setName(name).addConstraints(constraints).setType(type).setDefaultValue(defaultValue));

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
     * @return This object.
     */
    public DbEntity addColumn(final String name, final DbColumnType type, final Integer size, final boolean autoInc, final DbColumnConstraint... constraints) {
        addColumn(new DbColumn().setName(name).addConstraints(constraints).setType(type).setAutoInc(autoInc).setSize(size));

        return this;
    }

    /**
     * Removes the column form the list of columns.
     *
     * @param name The column name.
     * @since 12.1.0
     */
    public void removeColumn(final String name) {
        for (int x = 0; x < columns.size(); x++) {
            if (columns.get(x).getName().equals(name)) {
                columns.remove(x);
                return;
            }
        }
    }

    /**
     * Sets the PK fields.
     *
     * @param pkFields The PK fields.
     * @return This object.
     */
    public DbEntity setPkFields(final String... pkFields) {
        this.pkFields = pkFields;

        return this;
    }

    /**
     * Adds an index.
     *
     * @param index The index.
     * @return This object.
     */
    public DbEntity addIndex(final DbIndex index) {
        indexes.add(index);

        return this;
    }

    /**
     * Adds an index.
     *
     * @param unique  True if the index is unique, false otherwise.
     * @param columns The columns that are part of the index.
     * @return This object.
     */
    public DbEntity addIndex(final boolean unique, final String... columns) {
        return addIndex(new DbIndex().columns(columns).setUnique(unique));
    }

    /**
     * Adds an index.
     *
     * @param columns The columns that are part of the index.
     * @return This object.
     */
    public DbEntity addIndex(final String... columns) {
        return addIndex(false, columns);
    }

    /**
     * Adds an index.
     *
     * @param columns The columns that are part of the index.
     * @return This object.
     */
    public DbEntity addIndex(final Collection<String> columns) {
        addIndex(new DbIndex().columns(columns));

        return this;
    }

    /**
     * Adds the FKs.
     *
     * @param fks The list of FKs.
     * @return This object.
     */
    public DbEntity addFk(final DbFk... fks) {
        this.fks.addAll(Arrays.asList(fks));

        return this;
    }

    /**
     * Adds the FKs.
     *
     * @param fks The list of FKs.
     * @return This object.
     */
    public DbEntity addFks(final Collection<DbFk> fks) {
        this.fks.addAll(fks);

        return this;
    }

    /**
     * @return The entity columns.
     */
    public List<DbColumn> getColumns() {
        return columns;
    }

    /**
     * Checks if the entity has a column defined with the specified name.
     *
     * @param columnName The column name.
     * @return True if contains the column.
     * @since 12.1.0
     */
    public boolean containsColumn(String columnName) {
        for (DbColumn column : columns) {
            if (column.getName().equals(columnName))
                return true;
        }
        return false;
    }

    /**
     * @return The entity name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return The PK fields.
     */
    public String[] getPkFields() {
        return pkFields;
    }

    /**
     * @return The indexes to this entity.
     */
    public List<DbIndex> getIndexes() {
        return indexes;
    }

    /**
     * @return The FKs to this entity.
     */
    public List<DbFk> getFks() {
        return fks;
    }
}
