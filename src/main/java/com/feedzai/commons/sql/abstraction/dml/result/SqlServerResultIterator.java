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
import com.feedzai.commons.sql.abstraction.engine.handler.QueryExceptionHandler;
import com.feedzai.commons.sql.abstraction.engine.impl.SqlServerEngine;

import java.sql.PreparedStatement;
import java.sql.Statement;

import static com.feedzai.commons.sql.abstraction.engine.impl.SqlServerEngine.SQLSERVER_QUERY_EXCEPTION_HANDLER;

/**
 * Result iterator for the {@link SqlServerEngine} engine.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class SqlServerResultIterator extends ResultIterator {
    /**
     * Creates a new instance of {@link SqlServerResultIterator}.
     *
     * @param statement The statement.
     * @param sql       The sql statement.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    public SqlServerResultIterator(Statement statement, String sql) throws DatabaseEngineException {
        super(statement, sql);
    }

    /**
     * Creates a new instance of {@link SqlServerResultIterator}.
     *
     * @param statement The prepared statement.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    public SqlServerResultIterator(PreparedStatement statement) throws DatabaseEngineException {
        super(statement);
    }

    @Override
    public ResultColumn createResultColumn(String name, Object value) {
        return new SqlServerResultColumn(name, value);
    }

    @Override
    protected QueryExceptionHandler getQueryExceptionHandler() {
        return SQLSERVER_QUERY_EXCEPTION_HANDLER;
    }
}
