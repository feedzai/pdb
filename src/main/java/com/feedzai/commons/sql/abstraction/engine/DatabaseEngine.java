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

import com.feedzai.commons.sql.abstraction.batch.AbstractBatch;
import com.feedzai.commons.sql.abstraction.ddl.DbColumnType;
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.ddl.DbEntityType;
import com.feedzai.commons.sql.abstraction.dml.Expression;
import com.feedzai.commons.sql.abstraction.dml.dialect.Dialect;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.dml.result.ResultIterator;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.handler.ExceptionHandler;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Interface with the specific database implementation.
 * <p/>
 * Provides methods to query and manipulate database objects.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public interface DatabaseEngine {
    /**
     * Closes the connection to the database.
     */
    void close();

    /**
     * Starts a transaction. Doing this will set auto commit to false ({@link Connection#getAutoCommit()}).
     *
     * @throws DatabaseEngineException If something goes wrong while persisting data.
     */
    void beginTransaction() throws DatabaseEngineRuntimeException;

    /**
     * Adds an entity to the engine. It will create tables and everything necessary so persistence can work.
     *
     * @param entity The entity to add.
     * @throws DatabaseEngineException If something goes wrong while creating the structures.
     */
    void addEntity(DbEntity entity) throws DatabaseEngineException;

    /**
     * Loads an entity into the engine.
     * <p>
     * No DDL commands will be executed, only prepared statements will be created in order to {@link #persist(String, com.feedzai.commons.sql.abstraction.entry.EntityEntry) persist}
     * data into the entities.
     *
     * @param entity The entity to load into the connection.
     * @throws DatabaseEngineException If something goes wront while loading the entity.
     * @implSpec The invocation of this method multiple times is allowed. If the entity already exists, the invocation is a no-op.
     * @implNote The implementation is similar to the {@link #addEntity(com.feedzai.commons.sql.abstraction.ddl.DbEntity) addEntity} that configured with
     * {@link com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties#SCHEMA_POLICY SCHEMA_POLICY} of none.
     * @since 2.1.2
     */
    void loadEntity(DbEntity entity) throws DatabaseEngineException;

    /**
     * <p>
     * Updates an entity in the engine.
     * </p>
     * <p>
     * If the entity does not exists in the instance, the method {@link #addEntity(com.feedzai.commons.sql.abstraction.ddl.DbEntity)} will be invoked.
     * </p>
     * <p>
     * The engine will compare the entity with the {@link #getMetadata(String)} information and update the schema of the table.
     * </p>
     * <p>
     * ATTENTION: This method will only add new columns or drop removed columns in the database table.
     * Primary Keys, Foreign Keys, Indexes and column types changes will not be updated.
     * </p>
     *
     * @param entity The entity to update.
     * @throws DatabaseEngineException
     * @since 12.1.0
     */
    void updateEntity(DbEntity entity) throws DatabaseEngineException;

    /**
     * Removes the entity given the name. If the schema is drop-create then the entity will be dropped.
     *
     * @param name The name of the entity to remove.
     * @return The entity removed or null if there's no entity with the given name.
     */
    DbEntity removeEntity(final String name);

    /**
     * Returns if the current engine contains the entity.
     *
     * @param name The entity name.
     * @return True if the engine has the entity, false otherwise.
     */
    boolean containsEntity(final String name);

    /**
     * Drops an entity.
     *
     * @param entity The entity name.
     * @throws DatabaseEngineException
     */
    void dropEntity(final String entity) throws DatabaseEngineException;

    /**
     * <p>
     * Persists a given entry. Persisting a query implies executing the statement.
     * </p>
     * <p>
     * If you are inside of an explicit transaction, changes will only be visible upon explicit commit, otherwise a
     * commit will immediately take place.
     * </p>
     *
     * @param name  The entity name.
     * @param entry The entry to persist.
     * @return The ID of the auto generated value, {@code null} if there's no auto generated value.
     * @throws DatabaseEngineException If something goes wrong while persisting data.
     */
    Long persist(final String name, final EntityEntry entry) throws DatabaseEngineException;

    /**
     * <p>
     * Persists a given entry. Persisting a query implies executing the statement.
     * If define useAutoInc as false, PDB will disable the auto increments for the current insert and advance the sequences if needed.
     * </p>
     * <p>
     * If you are inside of an explicit transaction, changes will only be visible upon explicit commit, otherwise a
     * commit will immediately take place.
     * </p>
     *
     * @param name       The entity name.
     * @param entry      The entry to persist.
     * @param useAutoInc Use or not the autoinc.
     * @return The ID of the auto generated value, {@code null} if there's no auto generated value.
     * @throws DatabaseEngineException If something goes wrong while persisting data.
     */
    Long persist(final String name, final EntityEntry entry, final boolean useAutoInc) throws DatabaseEngineException;

    /**
     * Flushes the batches for all the registered entities.
     *
     * @throws DatabaseEngineException If something goes wrong while persisting data.
     */
    void flush() throws DatabaseEngineException;

    /**
     * Commits the current transaction. You should only call this method if you've previously called
     * {@link DatabaseEngine#beginTransaction()}.
     *
     * @throws DatabaseEngineException If something goes wrong while persisting data.
     */
    void commit() throws DatabaseEngineRuntimeException;

    /**
     * Rolls back a transaction. You should only call this method if you've previously called
     * {@link DatabaseEngine#beginTransaction()}.
     *
     * @throws DatabaseEngineRuntimeException If the rollback fails.
     */
    void rollback() throws DatabaseEngineRuntimeException;

    /**
     * Checks if a transaction is active.
     *
     * @return {@code true} if the transaction is active, {@code false} otherwise.
     * @throws DatabaseEngineRuntimeException If the access to the database fails.
     */
    boolean isTransactionActive() throws DatabaseEngineRuntimeException;

    /**
     * Executes a native query.
     *
     * @param query The query to execute.
     * @return The number of rows updated.
     * @throws DatabaseEngineException If something goes wrong executing the native query.
     */
    int executeUpdate(final String query) throws DatabaseEngineException;

    /**
     * Executes the given update.
     *
     * @param query The update to execute.
     * @throws DatabaseEngineException If something goes wrong executing the update.
     */
    int executeUpdate(final Expression query) throws DatabaseEngineException;

    /**
     * Translates the given expression to the current dialect.
     *
     * @param query The query to translate.
     * @return The translation result.
     */
    String translate(final Expression query);

    /**
     * Gets the dialect being used.
     *
     * @return The dialect being used.
     */
    Dialect getDialect();

    /**
     * Creates a new batch that periodically flushes a batch. A flush will also occur when the maximum number of
     * statements in the batch is reached.
     * <p/>
     * Please be sure to call {@link AbstractBatch#destroy() } before closing the session with the database.
     *
     * @param batchSize    The batch size.
     * @param batchTimeout If inserts do not occur after the specified time, a flush will be performed.
     * @return The batch.
     */
    AbstractBatch createBatch(final int batchSize, final long batchTimeout);

    /**
     * Creates a new batch that periodically flushes a batch. A flush will also occur when the maximum number of
     * statements in the batch is reached.
     * <p/>
     * Please be sure to call {@link AbstractBatch#destroy() } before closing the session with the database.
     *
     * @param batchSize    The batch size.
     * @param batchTimeout If inserts do not occur after the specified time, a flush will be performed.
     * @param batchName    The batch name.
     * @return The batch.
     */
    AbstractBatch createBatch(final int batchSize, final long batchTimeout, final String batchName);

    /**
     * Checks if the connection is alive.
     *
     * @param forceReconnect {@code true} to force the connection in case of failure, {@code false} otherwise.
     * @return {@code true} if the connection is valid, {@code} false otherwise.
     */
    boolean checkConnection(final boolean forceReconnect);

    /**
     * Checks if the connection is alive.
     *
     * @return {@code true} if the connection is valid, {@code false} otherwise.
     */
    boolean checkConnection();

    /**
     * Adds an entry to the batch.
     *
     * @param name  The entity name.
     * @param entry The entry to persist.
     * @throws DatabaseEngineException If something goes wrong while persisting data.
     */
    void addBatch(final String name, final EntityEntry entry) throws DatabaseEngineException;

    /**
     * Executes the given query.
     *
     * @param query The query to execute.
     * @throws DatabaseEngineException If something goes wrong executing the query.
     */
    List<Map<String, ResultColumn>> query(final Expression query) throws DatabaseEngineException;

    /**
     * Executes the given native query.
     *
     * @param query The query to execute.
     * @throws DatabaseEngineException If something goes wrong executing the query.
     */
    List<Map<String, ResultColumn>> query(final String query) throws DatabaseEngineException;

    /**
     * Gets the database entities.
     *
     * @return The list of database entities and types (tables, views, and so on).
     * @throws DatabaseEngineException If something occurs getting the existing tables.
     */
    Map<String, DbEntityType> getEntities() throws DatabaseEngineException;

    /**
     * Gets the table metadata.
     *
     * @return A representation of the table columns and types.
     * @throws DatabaseEngineException If something occurs getting the metadata.
     */
    Map<String, DbColumnType> getMetadata(final String name) throws DatabaseEngineException;

    /**
     * Gets the query metadata.
     *
     * @param query The query to retrieve the metadata from.
     * @return A representation of the table columns and types.
     * @throws DatabaseEngineException If something occurs getting the metadata.
     */
    Map<String, DbColumnType> getQueryMetadata(final Expression query) throws DatabaseEngineException;

    /**
     * Gets the query metadata.
     *
     * @param query The query to retrieve the metadata from.
     * @return A representation of the table columns and types.
     * @throws DatabaseEngineException If something occurs getting the metadata.
     */
    Map<String, DbColumnType> getQueryMetadata(final String query) throws DatabaseEngineException;

    /**
     * Duplicates a connection.
     *
     * @param mergeProperties Merge properties with the ones already existing.
     * @param copyEntities    {@code true} to include the entities in the new connection, {@code false} otherwise.
     * @return The new connection.
     * @throws DuplicateEngineException If policy is set to other than 'create' or 'none' or duplication fails for some reason.
     */
    DatabaseEngine duplicate(Properties mergeProperties, final boolean copyEntities) throws DuplicateEngineException;

    /**
     * Gets the properties in use.
     *
     * @return A clone of the properties in use.
     */
    PdbProperties getProperties();

    /**
     * Creates a prepared statement.
     *
     * @param name  The prepared statement name.
     * @param query The query.
     * @throws NameAlreadyExistsException If the name already exists.
     * @throws DatabaseEngineException    If something goes wrong creating the statement.
     */
    void createPreparedStatement(final String name, final Expression query) throws NameAlreadyExistsException,
            DatabaseEngineException;

    /**
     * Creates a prepared statement.
     *
     * @param name  The prepared statement name.
     * @param query The query.
     * @throws NameAlreadyExistsException If the name already exists.
     * @throws DatabaseEngineException    If something goes wrong creating the statement.
     */
    void createPreparedStatement(final String name, final String query) throws NameAlreadyExistsException,
            DatabaseEngineException;

    /**
     * Creates a prepared statement.
     *
     * @param name    The prepared statement name.
     * @param query   The query.
     * @param timeout The timeout (in seconds) for the query to execute.
     * @throws NameAlreadyExistsException If the name already exists.
     * @throws DatabaseEngineException    If something goes wrong creating the statement.
     */
    void createPreparedStatement(final String name, final Expression query, final int timeout) throws NameAlreadyExistsException,
            DatabaseEngineException;

    /**
     * Creates a prepared statement.
     *
     * @param name    The prepared statement name.
     * @param query   The query.
     * @param timeout The timeout (in seconds) for the query to execute.
     * @throws NameAlreadyExistsException If the name already exists.
     * @throws DatabaseEngineException    If something goes wrong creating the statement.
     */
    void createPreparedStatement(final String name, final String query, final int timeout) throws NameAlreadyExistsException,
            DatabaseEngineException;

    /**
     * Removes the given prepared statement.
     *
     * @param name The prepared statement name.
     */
    void removePreparedStatement(final String name);

    /**
     * Gets the result set of the specified prepared statement.
     *
     * @param name The prepared statement name.
     * @return The result.
     * @throws DatabaseEngineException If something occurs getting the result.
     */
    List<Map<String, ResultColumn>> getPSResultSet(final String name) throws DatabaseEngineException;

    /**
     * Sets the parameters on the specified prepared statement.
     *
     * @param name   The prepared statement name.
     * @param params The parameters to set.
     * @throws DatabaseEngineException  If something occurs setting the parameters.
     * @throws ConnectionResetException If the connection was reset while trying to set parameters.
     */
    void setParameters(final String name, final Object... params) throws DatabaseEngineException, ConnectionResetException;

    /**
     * Sets the parameter on the specified index.
     *
     * @param name  The prepared statement name.
     * @param index The index to set.
     * @param param The parameter to set.
     * @throws DatabaseEngineException  If something occurs setting the parameters.
     * @throws ConnectionResetException If the connection was reset while trying to set the parameter.
     */
    void setParameter(final String name, final int index, final Object param) throws DatabaseEngineException, ConnectionResetException;

    /**
     * Sets the parameter on the specified index given its type. This is for situations where the java type of the parameter
     * alone is not enough to determine the corresponding database type; for example, Strings can be used to represent both
     * actual Strings and Json values, so if we have an update statement that updates a json column we need to specify that
     * the bind parameter is of type json.
     *
     * @param name  The prepared statement name.
     * @param index The index to set.
     * @param param The parameter to set.
     * @param paramType The type of the parameter being set.
     * @throws DatabaseEngineException  If something occurs setting the parameters.
     * @throws ConnectionResetException If the connection was reset while trying to set the parameter.
     * @since 2.1.5
     */
    void setParameter(final String name, final int index, final Object param, DbColumnType paramType) throws DatabaseEngineException, ConnectionResetException;

    /**
     * Executes the specified prepared statement.
     *
     * @param name The prepared statement name.
     * @throws DatabaseEngineException  If something goes wrong while executing.
     * @throws ConnectionResetException If the connection is down and reestablishment occurs. If this happens, the user must reset the
     *                                  parameters and re-execute the query.
     */
    void executePS(final String name) throws DatabaseEngineException, ConnectionResetException;

    /**
     * Clears the prepared statement parameters.
     *
     * @param name The prepared statement name.
     * @throws DatabaseEngineException  If something occurs while clearing the parameters.
     * @throws ConnectionResetException If the connection was reset.
     */
    void clearParameters(final String name) throws DatabaseEngineException, ConnectionResetException;

    /**
     * Checks if there's a prepared statement with the given name.
     *
     * @param name The name to test.
     * @return {@code true} if it exists, {@code false} otherwise.
     */
    boolean preparedStatementExists(final String name);

    /**
     * Executes update on the specified prepared statement.
     *
     * @param name The prepared statement name.
     * @throws DatabaseEngineException  If the prepared statement does not exist or something goes wrong while executing.
     * @throws ConnectionResetException If the connection is down and reestablishment occurs. If this happens, the user must reset the
     *                                  parameters and re-execute the query.
     */
    Integer executePSUpdate(final String name) throws DatabaseEngineException, ConnectionResetException;

    /**
     * @return The comment character of this engine implementation.
     */
    String commentCharacter();

    /**
     * @return The escape character of this engine implementation.
     */
    String escapeCharacter();

    /**
     * Checks if the connection is available and returns it. If the connection is not available, it tries to reconnect
     * (the number of times defined in the properties with the delay there specified).
     *
     * @return The connection.
     * @throws RetryLimitExceededException If the retry limit is exceeded.
     * @throws InterruptedException        If the thread is interrupted during reconnection.
     */
    Connection getConnection() throws RetryLimitExceededException, InterruptedException, RecoveryException;

    /**
     * Creates an iterator for the given SQL sentence.
     *
     * @param query The query.
     * @return An iterator for the results of the given SQL query.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    ResultIterator iterator(final String query) throws DatabaseEngineException;

    /**
     * Creates an iterator for the given SQL expression.
     *
     * @param query The expression that represents the query.
     * @return An iterator for the results of the given SQL expression.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    ResultIterator iterator(final Expression query) throws DatabaseEngineException;

    /**
     * Creates an iterator for the given SQL sentence.
     *
     * @param query     The query.
     * @param fetchSize The number of rows to fetch each time.
     * @return An iterator for the results of the given SQL query.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    ResultIterator iterator(final String query, final int fetchSize) throws DatabaseEngineException;

    /**
     * Creates an iterator for the given SQL expression.
     *
     * @param query     The expression that represents the query.
     * @param fetchSize The number of rows to fetch each time.
     * @return An iterator for the results of the given SQL expression.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    ResultIterator iterator(final Expression query, final int fetchSize) throws DatabaseEngineException;

    /**
     * Creates an iterator for the {@link java.sql.PreparedStatement} bound to the given name.
     *
     * @param name The name of the prepared statement.
     * @return An iterator for the results of the prepared statement of the given name.
     * @throws DatabaseEngineException
     */
    ResultIterator getPSIterator(final String name) throws DatabaseEngineException;

    /**
     * Creates an iterator for the {@link java.sql.PreparedStatement} bound to the given name.
     *
     * @param name      The name of the prepared statement.
     * @param fetchSize The number of rows to fetch each time.
     * @return An iterator for the results of the prepared statement of the given name.
     * @throws DatabaseEngineException
     */
    ResultIterator getPSIterator(final String name, final int fetchSize) throws DatabaseEngineException;

    /**
     * Sets the given exception handler in the engine.
     *
     * @param eh The reference for exception callbacks.
     */
    void setExceptionHandler(ExceptionHandler eh);
}
