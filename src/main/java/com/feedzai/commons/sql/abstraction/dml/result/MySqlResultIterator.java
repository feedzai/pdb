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
import com.feedzai.commons.sql.abstraction.engine.impl.MySqlEngine;

import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * Result iterator for the {@link MySqlEngine} engine.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class MySqlResultIterator extends ResultIterator {
    /**
     * Creates a new instance of {@link MySqlResultIterator}.
     *
     * @param statement The statement.
     * @param sql       The sql statement.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    public MySqlResultIterator(Statement statement, String sql) throws DatabaseEngineException {
        super(statement, sql);
    }

    /**
     * Creates a new instance of {@link MySqlResultIterator}.
     *
     * @param statement The prepared statement.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    public MySqlResultIterator(PreparedStatement statement) throws DatabaseEngineException {
        super(statement);
    }

    @Override
    public ResultColumn createResultColumn(String name, Object value) {
        return new MySqlResultColumn(name, value);
    }
}
