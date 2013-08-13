/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.batch;

import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.google.common.base.Objects;

/**
 * Represents a batch entry that contains the data of the entry and the table name.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 13.1.0
 */
public class BatchEntry {
    /**
     * the table name
     */
    protected String tableName;
    /**
     * the entity data
     */
    protected EntityEntry entityEntry;

    public BatchEntry(String tableName, EntityEntry entityEntry) {
        this.tableName = tableName;
        this.entityEntry = entityEntry;
    }

    /**
     * Sets new the table name.
     *
     * @param tableName New value of the table name.
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Sets new the entity data.
     *
     * @param entityEntry New value of the entity data.
     */
    public void setEntityEntry(EntityEntry entityEntry) {
        this.entityEntry = entityEntry;
    }

    /**
     * Gets the entity data.
     *
     * @return Value of the entity data.
     */
    public EntityEntry getEntityEntry() {
        return entityEntry;
    }

    /**
     * Gets the table name.
     *
     * @return Value of the table name.
     */
    public String getTableName() {
        return tableName;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableName, entityEntry);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final BatchEntry other = (BatchEntry) obj;
        return Objects.equal(this.tableName, other.tableName) && Objects.equal(this.entityEntry, other.entityEntry);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("tableName", tableName)
                .add("entityEntry", entityEntry)
                .toString();
    }
}
