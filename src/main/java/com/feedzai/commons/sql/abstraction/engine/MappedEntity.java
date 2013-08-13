/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine;

import com.feedzai.commons.sql.abstraction.ddl.DbEntity;

import java.sql.PreparedStatement;

/**
 * A Mapped Entity.
 */
public class MappedEntity {
    /**
     * The entity object.
     */
    private DbEntity entity = null;
    /**
     * The prepared statement to insert new values.
     */
    private PreparedStatement insert = null;
    /**
     * The prepared statement to insert new values including the auto inc columns.
     */
    private PreparedStatement insertWithAutoInc = null;
    /**
     * The prepared statement to insert new values.
     */
    private PreparedStatement insertReturning = null;
    /**
     * The auto increment column if exists;
     */
    private String autoIncColumn = null;

    /**
     * Creates a new instance of {@link MappedEntity}.
     */
    public MappedEntity() {
    }

    /**
     * Sets the entity.
     *
     * @param entity The entity.
     * @return This object.
     */
    public MappedEntity setEntity(final DbEntity entity) {
        this.entity = entity;

        return this;
    }

    /**
     * Sets the insert statement.
     *
     * @param insert The insert statement.
     * @return This object.
     */
    public MappedEntity setInsert(final PreparedStatement insert) {
        this.insert = insert;

        return this;
    }

    /**
     * @return The entity.
     */
    public DbEntity getEntity() {
        return entity;
    }

    /**
     * @return The insert statement.
     */
    public PreparedStatement getInsert() {
        return insert;
    }

    /**
     * @return The insert statement that allows returning the generated keys.
     */
    public PreparedStatement getInsertReturning() {
        return insertReturning;
    }

    /**
     * Sets the insert statement that allows returning the generated keys.
     *
     * @param insertReturning The insert statement that allows returning the generated keys
     * @return This object.
     */
    public MappedEntity setInsertReturning(PreparedStatement insertReturning) {
        this.insertReturning = insertReturning;

        return this;
    }

    /**
     * @return The insert statement with auto increment columns.
     * @see DatabaseEngine#persist(String, com.feedzai.commons.sql.abstraction.entry.EntityEntry, boolean)
     */
    public PreparedStatement getInsertWithAutoInc() {
        return insertWithAutoInc;
    }

    /**
     * Sets the insert statement auto inc columns.
     *
     * @param insertWithAutoInc The insert statement with auto inc columns.
     * @return the MappedEntiry with the autoincrement behavior active
     * @see DatabaseEngine#persist(String, com.feedzai.commons.sql.abstraction.entry.EntityEntry, boolean)
     */
    public MappedEntity setInsertWithAutoInc(PreparedStatement insertWithAutoInc) {
        this.insertWithAutoInc = insertWithAutoInc;
        return this;
    }

    public String getAutoIncColumn() {
        return autoIncColumn;
    }

    public MappedEntity setAutoIncColumn(String autoIncColumn) {
        this.autoIncColumn = autoIncColumn;
        return this;
    }
}
