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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.feedzai.commons.sql.abstraction.engine.AbstractDatabaseEngine.DEFAULT_QUERY_EXCEPTION_HANDLER;

/**
 * The abstract result iterator. Extending classes will create the specific {@link ResultColumn}
 * implementation given the engine in place.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public abstract class ResultIterator implements AutoCloseable {
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
    private final List<String> columnNames = new ArrayList<>();

    /**
     * Signals if the result set is already closed.
     */
    private boolean closed = false;
    /**
     * Signals if statement in place is closeable. E.g. Prepared statements must not be closed.
     */
    private final boolean statementCloseable;

    /**
     * The number of rows processed by the iterator so far.
     */
    private int currentRowCount = 0;

    /**
     * Signals whether the iterator query was wrapped in a transaction before being sent to the database.
     */
    private final boolean isWrappedInTransaction;

    /**
     * Flag to keep the previous state of the {@link Connection#getAutoCommit() conection autocommit}.
     */
    private final boolean previousAutocommit;

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
    private ResultIterator(final Statement statement, final String sql, final boolean isPreparedStatement) throws DatabaseEngineException {
        this.statement = statement;

        // Process column names.
        try {
            long start = System.currentTimeMillis();

            final Connection connection = statement.getConnection();
            this.previousAutocommit = connection.getAutoCommit();
            this.isWrappedInTransaction = needsWrapInTransaction(statement.getFetchSize());

            if (isWrappedInTransaction) {
                connection.setAutoCommit(false);
            }

            if (isPreparedStatement) {
                this.resultSet = ((PreparedStatement) statement).executeQuery();
                this.statementCloseable = false;
            } else {
                this.resultSet = statement.executeQuery(sql);
                this.statementCloseable = true;
            }

            logger.trace("[{} ms] {}", (System.currentTimeMillis() - start), sql == null ? "" : sql);

            final ResultSetMetaData meta = resultSet.getMetaData();
            final int columnCount = meta.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(meta.getColumnLabel(i));
            }

        } catch (final Exception e) {
            throw closeAndHandleException(e, "Could not process result set.");
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
     * Retrieves the number of rows processed by the iterator so far. If the iteration
     * hasn't started, this method returns 0.
     *
     * @return The number of rows processed by the iterator so far or 0 if the iteration hasn't started.
     */
    public int getCurrentRowCount() {
        return this.currentRowCount;
    }

    /**
     * Retrieves the next row in the result set.
     * <p>
     * This method also closes the result set upon the last call on the result set.
     * <p>
     * If the statement in place is not a {@link PreparedStatement} it also closes the statement.
     * <p>
     * If an exception is thrown the calling thread is responsible for repeating the action in place.
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

            ++currentRowCount;

            Map<String, ResultColumn> temp = new LinkedHashMap<>(columnNames.size());
            int i = 1;
            for (String cname : columnNames) {
                temp.put(cname, createResultColumn(cname, resultSet.getObject(i)));
                i++;
            }

            return temp;
        } catch (final Exception e) {
            throw closeAndHandleException(e, "Could not fetch data.");
        }
    }

    /**
     * Retrieves the values of the next row in the result set as an array of objects.
     * <p>
     * This method provides an optimized version of the {@link #next()} method with less overhead.
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
     * @see #getColumnNames() for the names of the columns of this method return.
     */
    public ResultColumn[] nextResult() throws DatabaseEngineException {
        try {

            if (closed) {
                return null;
            }

            if (!resultSet.next()) {
                close();

                return null;
            }

            ++currentRowCount;

            ResultColumn[] temp = new ResultColumn[columnNames.size()];
            for (int i = 0; i < columnNames.size(); i++) {
                temp[i] = createResultColumn(columnNames.get(i), resultSet.getObject(i + 1));
            }
            return temp;
        } catch (final Exception e) {
            throw closeAndHandleException(e, "Could not fetch data.");
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
     * Retrieves the column names of the iterator.
     *
     * @return the column names of the iterator.
     */
    public List<String> getColumnNames() {
        return columnNames;
    }

    /**
     * Attempts to cancel the current query. This relies on the JDBC driver supporting
     * {@link Statement#cancel()}, which is not guaranteed on all drivers.
     * <p>
     * A possible use case for this method is to implement a timeout; If that's the case, see also
     * {@link com.feedzai.commons.sql.abstraction.engine.AbstractDatabaseEngine#iterator(String, int, int)} for
     * an alternative way to accomplish this.
     * <p>
     * This method is expected to be invoked from a thread distinct of the one that is reading
     * from the result set.
     *
     * @return {@code true} if the query was cancelled, {@code false} otherwise.
     */
    public boolean cancel() {
        try {
            if (!closed) {
                statement.cancel();
            }
            return true;
        } catch (SQLException ex) {
            logger.debug("Could not cancel statement", ex);
            return false;
        }
    }

    /**
     * Closes the {@link ResultSet} and the {@link Statement} if applicable.
     */
    @Override
    public void close() {
        // Check for previous closed.
        if (closed) {
            return;
        }

        if (statement != null && isWrappedInTransaction) {
            try {
                statement.getConnection().setAutoCommit(previousAutocommit);
            } catch (final Exception e) {
                logger.warn("Could not reset autocommit.", e);
            }
        }

        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (final Exception e) {
                logger.warn("Could not close result set.", e);
            }
        }

        if (statementCloseable && statement != null) {
            try {
                statement.close();
            } catch (final Exception e) {
                logger.warn("Could not close statement.", e);
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

    /**
     * Closes this iterator and handles the Exception, disambiguating it into a specific PDB Exception and throwing it.
     * <p>
     * If a specific type does not match the info in the provided Exception, throws a {@link DatabaseEngineException}.
     *
     * @param exception The exception to handle.
     * @param message   The message to associate with the thrown exception.
     * @return a {@link DatabaseEngineException} (declared, but only to keep Java type system happy; this method will
     * always throw an exception).
     * @since 2.5.1
     */
    private DatabaseEngineException closeAndHandleException(final Exception exception,
                                                            final String message) throws DatabaseEngineException {
        close();
        return getQueryExceptionHandler().handleException(exception, message);
    }

    /**
     * Gets the instance of {@link QueryExceptionHandler} to be used in disambiguating SQL exceptions.
     *
     * @return the {@link QueryExceptionHandler}.
     * @since 2.5.1
     */
    protected QueryExceptionHandler getQueryExceptionHandler() {
        return DEFAULT_QUERY_EXCEPTION_HANDLER;
    }

    /**
     * Indicates whether this iterator needs to run inside a transaction.
     *
     * PostgreSQL (and also CockroachDB) need this in order to keep a cursor on the server, otherwise the fetchsize
     * setting is ignored and all results are fetched from the database into memory at once.
     *
     * @param fetchSize The fetch size for result sets obtained in this iterator.
     * @return Whether this iterator needs to run inside a transaction.
     */
    protected boolean needsWrapInTransaction(final int fetchSize) {
        return false;
    }
}
