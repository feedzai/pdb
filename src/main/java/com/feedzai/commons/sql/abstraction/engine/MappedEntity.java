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
package com.feedzai.commons.sql.abstraction.engine;

import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Mapped entity contains information about an entity that has been mapped using the engine.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class MappedEntity implements AutoCloseable {
    /**
     * The logger.
     */
    private static Logger logger = LoggerFactory.getLogger(MappedEntity.class);
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
     * The prepared statement to insert new values ignoring duplicated keys.
     */
    private PreparedStatement insertIgnoring = null;
    /**
     * The auto increment column if exists;
     */
    private String autoIncColumn = null;
    /**
     * Flag to signal if a sequence is dirty due to a non-autoinc insert.
     */
    private boolean sequenceDirty = true;

    /**
     * Creates a new instance of {@link MappedEntity}.
     */
    public MappedEntity() {
    }

    /**
     * Sets the entity.
     *
     * @param entity The entity.
     * @return This mapped entity
     */
    public MappedEntity setEntity(final DbEntity entity) {
        this.entity = entity;

        return this;
    }

    /**
     * Sets the insert statement.
     *
     * @param insert The insert statement.
     * @return This mapped entity
     */
    public MappedEntity setInsert(final PreparedStatement insert) {
        closeQuietly(this.insert);
        this.insert = insert;

        return this;
    }

    /**
     * Gets the entity.
     *
     * @return The entity.
     */
    public DbEntity getEntity() {
        return entity;
    }

    /**
     * Gets the prepared statement for inserts.
     *
     * @return The prepared statement for inserts.
     */
    public PreparedStatement getInsert() {
        return insert;
    }

    /**
     * Gets the prepared statement for inserts that retrieve the generated keys using the
     * insert statement.
     *
     * @return The insert statement that allows returning the generated keys.
     */
    public PreparedStatement getInsertReturning() {
        return insertReturning;
    }

    /**
     * Sets the insert statement that allows returning the generated keys.
     *
     * @param insertReturning The insert statement that allows returning the generated keys
     * @return This mapped entity
     */
    public MappedEntity setInsertReturning(final PreparedStatement insertReturning) {
        closeQuietly(this.insertReturning);
        this.insertReturning = insertReturning;

        return this;
    }

    /**
     * Gets the insert statement with auto increment columns.
     *
     * @return The insert statement with auto increment columns.
     * @see DatabaseEngine#persist(String, EntityEntry, boolean)
     */
    public PreparedStatement getInsertWithAutoInc() {
        return insertWithAutoInc;
    }

    /**
     * Sets the insert statement auto inc columns.
     *
     * @param insertWithAutoInc The insert statement with auto inc columns.
     * @return This mapped entity;
     * @see DatabaseEngine#persist(String, EntityEntry, boolean)
     */
    public MappedEntity setInsertWithAutoInc(final PreparedStatement insertWithAutoInc) {
        closeQuietly(this.insertWithAutoInc);
        this.insertWithAutoInc = insertWithAutoInc;

        return this;
    }

    /**
     * Gets the prepared statement for inserts ignoring duplicated keys.
     *
     * @return The insert statement that allows ignoring duplicated keys.
     */
    public PreparedStatement getInsertIgnoring() {
        return insertIgnoring;
    }

    /**
     * Sets the insert that allows ignoring duplicated keys.
     *
     * @param insertIgnoring The insert statement that allows ignoring duplicated keys
     * @return This mapped entity
     */
    public MappedEntity setInsertIgnoring(final PreparedStatement insertIgnoring) {
        closeQuietly(this.insertIgnoring);
        this.insertIgnoring = insertIgnoring;

        return this;
    }

    /**
     * Gets the auto increment column.
     *
     * @return The auto increment column.
     */
    public String getAutoIncColumn() {
        return autoIncColumn;
    }

    /**
     * Sets the auto increment column.
     *
     * @param autoIncColumn the auto increment column.
     * @return This mapped entity.
     */
    public MappedEntity setAutoIncColumn(String autoIncColumn) {
        this.autoIncColumn = autoIncColumn;

        return this;
    }

    /**
     * Checks if a sequence is dirty. A sequence is dirty only if it was potentially marked as so and there's an auto increment column set.
     *
     * @return {@code true} if the sequence is dirty, {@code false} otherwise.
     */
    public boolean isSequenceDirty() {
        return autoIncColumn != null && sequenceDirty;
    }

    /**
     * Sets a sequence as dirty or not.
     *
     * @param sequenceDirty The flag signaling the status of the sequence.
     */
    public void setSequenceDirty(boolean sequenceDirty) {
        this.sequenceDirty = sequenceDirty;
    }

    /**
     * Closes a {@link PreparedStatement}, logging a warning if {@link SQLException} is thrown.
     *
     * @param preparedStatement The prepared statement to close.
     * @since 2.1.8
     */
    private void closeQuietly(final PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (final SQLException e) {
                logger.trace("Could not close prepared statement.", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        closeQuietly(this.insert);
        closeQuietly(this.insertWithAutoInc);
        closeQuietly(this.insertReturning);
        closeQuietly(this.insertIgnoring);
    }
}
