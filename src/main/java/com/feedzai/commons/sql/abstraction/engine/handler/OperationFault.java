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
package com.feedzai.commons.sql.abstraction.engine.handler;

import java.io.Serializable;

/**
 * Enumeration for failed operations in the engine.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class OperationFault implements Serializable {
    /**
     * The type of fault detected in the operation.
     */
    public enum Type {
        /**
         * The primary key that already exists.
         */
        PRIMARY_KEY_ALREADY_EXISTS,
        /**
         * Foreign key already exists.
         */
        FOREIGN_KEY_ALREADY_EXISTS,
        /**
         * Index already exists.
         */
        INDEX_ALREADY_EXISTS,
        /**
         * Sequence already exists.
         */
        SEQUENCE_ALREADY_EXISTS,
        /**
         * Sequence does not exist.
         */
        SEQUENCE_DOES_NOT_EXIST,
        /**
         * Table does not exist.
         */
        TABLE_DOES_NOT_EXIST,
        /**
         * Column does not exist.
         */
        COLUMN_DOES_NOT_EXIST,
        /**
         * Table does not exist.
         */
        TABLE_ALREADY_EXISTS
    }

    /**
     * The entity name.
     */
    private final String entity;
    /**
     * The type of fault originated by the operation.
     */
    private final Type type;

    /**
     * Creates a new instance of {@link OperationFault}.
     *
     * @param entity The entity name.
     * @param type   The type of fault originated by the operation.
     */
    public OperationFault(String entity, Type type) {
        this.entity = entity;
        this.type = type;
    }

    /**
     * Gets the entity name.
     *
     * @return The entity name.
     */
    public String getEntity() {
        return entity;
    }

    /**
     * Gets the fault type.
     *
     * @return the fault type.
     */
    public Type getType() {
        return type;
    }
}
