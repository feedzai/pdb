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
package com.feedzai.commons.sql.abstraction.dml.result;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.impl.PostgreSqlEngine;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Result iterator for the {@link PostgreSqlEngine} engine.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class PostgreSqlResultIterator extends ResultIterator {

    /**
     * The SQL State that indicates a timeout occurred.
     */
    private static final String TIMEOUT_SQL_STATE = "57014";

    /**
     * Creates a new instance of {@link PostgreSqlResultIterator}.
     *
     * @param statement The statement.
     * @param sql       The sql statement.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    public PostgreSqlResultIterator(Statement statement, String sql) throws DatabaseEngineException {
        super(statement, sql);
    }

    /**
     * Creates a new instance of {@link PostgreSqlResultIterator}.
     *
     * @param statement The prepared statement.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    public PostgreSqlResultIterator(PreparedStatement statement) throws DatabaseEngineException {
        super(statement);
    }

    @Override
    public ResultColumn createResultColumn(String name, Object value) {
        return new PostgreSqlResultColumn(name, value);
    }

    @Override
    protected boolean isTimeoutException(Exception exception) {
        return super.isTimeoutException (exception) ||
                exception instanceof SQLException && TIMEOUT_SQL_STATE.equals(((SQLException) exception).getSQLState());
    }
}
