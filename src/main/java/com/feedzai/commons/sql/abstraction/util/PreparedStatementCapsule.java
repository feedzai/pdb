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
package com.feedzai.commons.sql.abstraction.util;

import java.sql.PreparedStatement;

/**
 * Encapsulates a prepared statement, name as its properties.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public final class PreparedStatementCapsule {
    /**
     * The query.
     */
    public final String query;
    /**
     * The prepared statement.
     */
    public final PreparedStatement ps;
    /**
     * The query timeout.
     */
    public final int timeout;

    /**
     * Creates a new instance of {@link PreparedStatementCapsule}.
     *
     * @param query   The query.
     * @param ps      The prepared statement.
     * @param timeout The query timeout.
     */
    public PreparedStatementCapsule(String query, PreparedStatement ps, int timeout) {
        this.query = query;
        this.ps = ps;
        this.timeout = timeout;
    }
}
