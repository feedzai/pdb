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
 * A database index.
 */
public class DbIndex implements Serializable {
    /** The columns part of the index. */
    private final List<String> columns = new ArrayList<String>();
    /** Specifies if the index is unique or not. */
    private boolean unique = false;

    /**
     * Creates a new instance of {@link DbIndex}.
     */
    public DbIndex() { }

    /**
     * Add the columns that are part of the index.
     * @param columns The columns.
     * @return This object.
     */
    public DbIndex columns(String... columns) {
        if (columns == null) {
            return this;
        }

        this.columns.addAll(Arrays.asList(columns));

        return this;
    }

    /**
     * Add the columns that are part of the index.
     * @param collection The columns.
     * @return This object.
     */
    public DbIndex columns(final Collection<String> collection) {
        this.columns.addAll(collection);

        return this;
    }

    /**
     * Indicates if this index is unique or not.
     * @param unique True if it is unique, false otherwise.
     * @return This object.
     */
    public DbIndex setUnique(final boolean unique) {
        this.unique = unique;

        return this;
    }

    /**
     * @return The index columns.
     */
    public List<String> getColumns() {
        return columns;
    }

    /**
     * @return True if it is unique, false otherwise.
     */
    public boolean isUnique() {
        return unique;
    }
}
