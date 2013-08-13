/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.ddl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * References another column in a table.
 */
public class DbFk implements Serializable {
    /** The local column's names. */
    private List<String> localColumns = new ArrayList<String>();
    /** The foreign column's names. */
    private List<String> foreignColumns = new ArrayList<String>();
    /** The reference table. */
    private String foreignTable = null;
    

    /**
     * Creates a new instance of {@link DbFk}.
     */
    public DbFk() { }

    /**
     * Sets the foreign table.
     * @param foreignTable The foreign table.
     * @return This object.
     */
    public DbFk setForeignTable(final String foreignTable) {
        this.foreignTable = foreignTable;

        return this;
    }

    /**
     * Adds local columns to match the foreign.
     * @param columns The columns.
     * @return This object.
     */
    public DbFk addColumn(final String... columns) {
        this.localColumns.addAll(Arrays.asList(columns));

        return this;
    }

    /**
     * Adds local columns to match the foreign.
     * @param columns The columns.
     * @return This object.
     */
    public DbFk addColumns(final Collection<String> columns) {
        this.localColumns.addAll(columns);

        return this;
    }

    /**
     * Adds foreign columns to match the local ones.
     * @param foreignColumns The columns.
     * @return This object.
     */
    public DbFk addForeignColumn(final String... foreignColumns) {
        this.foreignColumns.addAll(Arrays.asList(foreignColumns));

        return this;
    }

    /**
     * Adds foreign columns to match the local ones.
     * @param columns The columns.
     * @return This object.
     */
    public DbFk addForeignColumns(final Collection<String> columns) {
        this.foreignColumns.addAll(columns);

        return this;
    }

    /**
     * @return The foreign columns.
     */
    public List<String> getForeignColumns() {
        return foreignColumns;
    }

    /**
     * @return The local columns.
     */
    public List<String> getLocalColumns() {
        return localColumns;
    }

    /**
     * @return The foreign table.
     */
    public String getForeignTable() {
        return foreignTable;
    }
}
