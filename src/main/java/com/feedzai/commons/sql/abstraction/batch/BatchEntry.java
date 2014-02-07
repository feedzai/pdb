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
package com.feedzai.commons.sql.abstraction.batch;

import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.google.common.base.Objects;

/**
 * Represents a batch entry that contains the data of the entry and the table name.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
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

    /**
     * Creates a new instance of {@link BatchEntry}.
     *
     * @param tableName   The table name.
     * @param entityEntry The entity entry.
     */
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
