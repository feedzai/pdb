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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The abstract result iterator. Extending classes will create the specific {@link ResultColumn}
 * implementation given the engine in place.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public abstract class ResultIterator {
    /**
     * The logger.
     */
    private static final Logger logger = LoggerFactory.getLogger(ResultIterator.class);
    /**
     * The statement.
     */
    private final Statement statement;
    /**
     * The result set.
     */
    private final ResultSet resultSet;
    /**
     * The list of columns names for the given query.
     */
    private final List<String> columnNames;
    /**
     * Signals if the result set is already close.
     */
    private boolean closed = false;
    /**
     * Signals if statement in place is closeable. E.g. Prepared statements must no be closed.
     */
    private boolean statementCloseable;

    /**
     * Creates a new instance of {@link ResultIterator} for regular statements (on-demand query).
     * <p>
     * This constructor will also create a result set and get all the projection names of the query.
     * </p>
     *
     * @param statement           The statement.
     * @param sql                 The sql sentence if applicable. E.g. Prepared statements do not have a SQL query since it's already defined.
     * @param isPreparedStatement True if the given statement is a {@link PreparedStatement}.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    private ResultIterator(Statement statement, String sql, boolean isPreparedStatement) throws DatabaseEngineException {
        this.statement = statement;
        this.columnNames = new ArrayList<>();

        // Process column names.
        ResultSetMetaData meta;
        try {
            long start = System.currentTimeMillis();
            if (isPreparedStatement) {
                this.resultSet = ((PreparedStatement) statement).executeQuery();
                this.statementCloseable = false;
            } else {
                this.resultSet = statement.executeQuery(sql);
                this.statementCloseable = true;
            }

            logger.trace("[{} ms] {}", (System.currentTimeMillis() - start), sql == null ? "" : sql);

            meta = resultSet.getMetaData();
            final int columnCount = meta.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(meta.getColumnLabel(i));
            }

        } catch (Exception e) {
            close();
            throw new DatabaseEngineException("Could not process result set.", e);
        }
    }

    /**
     * Creates a new instance of {@link ResultIterator} for regular {@link Statement}.
     *
     * @param statement The statement.
     * @param sql       The SQL sentence.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    public ResultIterator(Statement statement, String sql) throws DatabaseEngineException {
        this(statement, sql, false);
    }

    /**
     * Creates a new instance of {@link ResultIterator} for {@link PreparedStatement}.
     *
     * @param statement The prepared statement.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    public ResultIterator(PreparedStatement statement) throws DatabaseEngineException {
        this(statement, null, true);
    }

    /**
     * Retrieves the next row in the result set.
     * <p>
     * This method also closes the result set upon the last call on the result set.
     * </p>
     * <p>
     * If the statement in place is not a {@link PreparedStatement} it also closes the statement.
     * </p>
     * <p>
     * If an exception is thrown the calling thread is responsible for repeating the action in place.
     * </p>
     *
     * @return The result row.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    public Map<String, ResultColumn> next() throws DatabaseEngineException {
        try {

            if (closed) {
                return null;
            }

            if (!resultSet.next()) {
                close();

                return null;
            }

            Map<String, ResultColumn> temp = new LinkedHashMap<>();
            int i = 1;
            for (String cname : columnNames) {
                temp.put(cname, createResultColumn(cname, resultSet.getObject(i)));
                i++;
            }

            return temp;
        } catch (Exception e) {
            close();
            throw new DatabaseEngineException("Could not fetch data.", e);
        }
    }

    /**
     * Checks if this result iterator is closed.
     *
     * @return {@code true} if the result set is closed, {@code false} otherwise.
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Closes the {@link ResultSet} and the {@link Statement} if applicable.
     */
    public void close() {
        // Check for previous closed.
        if (!closed) {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (Exception e) {
                logger.warn("Could not close result set.", e);
            }
            if (statementCloseable && statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                    logger.warn("Could not close statement.", e);
                }
            }
        }
        // Assume closed even if it fails.
        closed = true;
    }

    /**
     * Creates a {@link ResultColumn} for the given engine in place.
     *
     * @param name  The name of the column.
     * @param value The value on the column.
     * @return A specific result column given the implementation.
     */
    public abstract ResultColumn createResultColumn(final String name, final Object value);
}
