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
package com.feedzai.commons.sql.abstraction.dml.dialect;

/**
 * Represents the supported dialects distributed by the library.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public enum Dialect {
    /**
     * Oracle SQL dialect.
     */
    ORACLE,
    /**
     * PostgreSQL SQL dialect.
     */
    POSTGRESQL,
    /**
     * CockroachDB dialect.
     * @since 2.5.0
     */
    COCKROACHDB,
    /**
     * MySQL SQL dialect.
     */
    MYSQL,
    /**
     * SQLServer SQL dialect.
     */
    SQLSERVER,
    /**
     * H2 SQL dialect.
     */
    H2,
    /**
     * DB2 SQL dialect.
     */
    DB2,
    /**
     * Unknown implementation (can be used when implementing
     * new engines that are not distributed with the library.
     */
    UNKNOWN
}
