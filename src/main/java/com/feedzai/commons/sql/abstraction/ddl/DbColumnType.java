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

/**
 * The column types.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public enum DbColumnType {
    /**
     * The boolean type.
     */
    BOOLEAN,
    /**
     * The string type.
     */
    STRING,
    /**
     * The integer type.
     */
    INT,
    /**
     * The double type.
     */
    DOUBLE,
    /**
     * The long type.
     */
    LONG,
    /**
     * The blob type.
     */
    BLOB,
    /**
     * The clob type
     */
    CLOB,
    /**
     * The json type. This is not supported by all engines; engines not
     * supporting it use the CLOB type.
     * @since 2.1.5
     */
    JSON,
    /**
     * A type that is not mapped.
     */
    UNMAPPED;
}
