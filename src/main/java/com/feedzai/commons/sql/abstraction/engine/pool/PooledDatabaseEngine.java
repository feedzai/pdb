/*
 * Copyright 2021 Feedzai
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
package com.feedzai.commons.sql.abstraction.engine.pool;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedzai.commons.sql.abstraction.FailureListener;
import com.feedzai.commons.sql.abstraction.batch.AbstractBatch;
import com.feedzai.commons.sql.abstraction.ddl.DbColumnType;
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.ddl.DbEntityType;
import com.feedzai.commons.sql.abstraction.dml.Expression;
import com.feedzai.commons.sql.abstraction.dml.dialect.Dialect;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.dml.result.ResultIterator;
import com.feedzai.commons.sql.abstraction.engine.ConnectionResetException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.feedzai.commons.sql.abstraction.engine.DuplicateEngineException;
import com.feedzai.commons.sql.abstraction.engine.NameAlreadyExistsException;
import com.feedzai.commons.sql.abstraction.engine.RecoveryException;
import com.feedzai.commons.sql.abstraction.engine.RetryLimitExceededException;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.handler.ExceptionHandler;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.feedzai.commons.sql.abstraction.listeners.BatchListener;

/**
 * Pooled {@link DatabaseEngine} which delegates its operations to a wrapped database engine instance. The
 * {@link #close()} operation causes the pooled database engine to be returned to pool.
 *
 * @author Luiz Silva (luiz.silva@feedzai.com)
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.8.3
 */
class PooledDatabaseEngine implements DatabaseEngine {

    /**
     * The logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(PooledDatabaseEngine.class);

    /**
     * The pool which owns this pooled database engine.
     */
    private final GenericObjectPool<PooledDatabaseEngine> pool;

    /**
     * The wrapped database engine.
     */
    private final DatabaseEngine engine;

    /**
     * Creates a new {@link PooledDatabaseEngine}.
     *
     * @param pool the pool owning the pooled database engine.
     * @param engine the database engine to be wrapped.
     */
    PooledDatabaseEngine(final GenericObjectPool<PooledDatabaseEngine> pool, final DatabaseEngine engine) {
        this.pool = pool;
        this.engine = engine;
    }

    /**
     * Closes the underlying engine connection.
     */
    public void closeConnection() {
        engine.close();
    }

    @Override
    public void close() {
        try {
            pool.returnObject(this);
        } catch (Exception e) {
            LOGGER.error("Error returning a pooled database engine.", e);
        }
    }

    @Override
    public void beginTransaction() throws DatabaseEngineRuntimeException {
        engine.beginTransaction();
    }

    @Override
    public void addEntity(final DbEntity entity) throws DatabaseEngineException {
        engine.addEntity(entity);
    }

    @Override
    public void loadEntity(final DbEntity entity) throws DatabaseEngineException {
        engine.loadEntity(entity);
    }

    @Override
    public void updateEntity(final DbEntity entity) throws DatabaseEngineException {
        engine.updateEntity(entity);
    }

    @Override
    public DbEntity removeEntity(final String name) {
        return engine.removeEntity(name);
    }

    @Override
    public boolean containsEntity(final String name) {
        return engine.containsEntity(name);
    }

    @Override
    public void dropEntity(final String entity) throws DatabaseEngineException {
        engine.dropEntity(entity);
    }

    @Override
    public void dropEntity(final DbEntity entity) throws DatabaseEngineException {
        engine.dropEntity(entity);
    }

    @Override
    public Long persist(final String name, final EntityEntry entry) throws DatabaseEngineException {
        return engine.persist(name, entry);
    }

    @Override
    public Long persist(final String name, final EntityEntry entry, final boolean useAutoInc)
            throws DatabaseEngineException {
        return engine.persist(name, entry, useAutoInc);
    }

    @Override
    public void flush() throws DatabaseEngineException {
        engine.flush();
    }

    @Override
    public void flushIgnore() throws DatabaseEngineException {
        engine.flushIgnore();
    }

    @Override
    public void commit() throws DatabaseEngineRuntimeException {
        engine.commit();
    }

    @Override
    public void rollback() throws DatabaseEngineRuntimeException {
        engine.rollback();
    }

    @Override
    public boolean isTransactionActive() throws DatabaseEngineRuntimeException {
        return engine.isTransactionActive();
    }

    @Override
    public int executeUpdate(final String query) throws DatabaseEngineException {
        return engine.executeUpdate(query);
    }

    @Override
    public int executeUpdate(final Expression query) throws DatabaseEngineException {
        return engine.executeUpdate(query);
    }

    @Override
    public String translate(final Expression query) {
        return engine.translate(query);
    }

    @Override
    public Dialect getDialect() {
        return engine.getDialect();
    }

    @Override
    public AbstractBatch createBatch(final int batchSize, final long batchTimeout) {
        return engine.createBatch(batchSize, batchTimeout);
    }

    @Override
    public AbstractBatch createBatch(final int batchSize, final long batchTimeout, final String batchName) {
        return engine.createBatch(batchSize, batchTimeout, batchName);
    }

    @Override
    public AbstractBatch createBatch(final int batchSize,
                                     final long batchTimeout,
                                     final String batchName,
                                     @Nullable final BatchListener batchListener) {
        return engine.createBatch(batchSize, batchTimeout, batchName, batchListener);
    }

    @Override
    @Deprecated
    public AbstractBatch createBatch(final int batchSize, final long batchTimeout, final String batchName,
            final FailureListener failureListener) {
        return engine.createBatch(batchSize, batchTimeout, batchName, failureListener);
    }

    @Override
    public boolean checkConnection(final boolean forceReconnect) {
        return engine.checkConnection(forceReconnect);
    }

    @Override
    public boolean checkConnection() {
        return engine.checkConnection();
    }

    @Override
    public void addBatch(final String name, final EntityEntry entry) throws DatabaseEngineException {
        engine.addBatch(name, entry);
    }

    @Override
    public void addBatchIgnore(final String name, final EntityEntry entry) throws DatabaseEngineException {
        engine.addBatchIgnore(name, entry);
    }

    @Override
    public List<Map<String, ResultColumn>> query(final Expression query) throws DatabaseEngineException {
        return engine.query(query);
    }

    @Override
    public List<Map<String, ResultColumn>> query(final String query) throws DatabaseEngineException {
        return engine.query(query);
    }

    @Override
    public List<Map<String, ResultColumn>> query(final Expression query,
                                                 final int readTimeoutOverride) throws DatabaseEngineException {
        return engine.query(query, readTimeoutOverride);
    }

    @Override
    public List<Map<String, ResultColumn>> query(final String query,
                                                 final int readTimeoutOverride) throws DatabaseEngineException {
        return engine.query(query, readTimeoutOverride);
    }

    @Override
    public Map<String, DbEntityType> getEntities() throws DatabaseEngineException {
        return engine.getEntities();
    }

    @Override
    public Map<String, DbEntityType> getEntities(final String schemaPattern) throws DatabaseEngineException {
        return engine.getEntities(schemaPattern);
    }

    @Override
    public Map<String, DbColumnType> getMetadata(final String tableNamePattern) throws DatabaseEngineException {
        return engine.getMetadata(tableNamePattern);
    }

    @Override
    public Map<String, DbColumnType> getMetadata(final String schemaPattern, final String tableNamePattern)
            throws DatabaseEngineException {
        return engine.getMetadata(schemaPattern, tableNamePattern);
    }

    @Override
    public Map<String, DbColumnType> getQueryMetadata(final Expression query) throws DatabaseEngineException {
        return engine.getQueryMetadata(query);
    }

    @Override
    public Map<String, DbColumnType> getQueryMetadata(final String query) throws DatabaseEngineException {
        return engine.getQueryMetadata(query);
    }

    @Override
    public DatabaseEngine duplicate(final Properties mergeProperties, final boolean copyEntities)
            throws DuplicateEngineException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PdbProperties getProperties() {
        return engine.getProperties();
    }

    @Override
    public void createPreparedStatement(final String name, final Expression query)
            throws NameAlreadyExistsException, DatabaseEngineException {
        engine.createPreparedStatement(name, query);
    }

    @Override
    public void createPreparedStatement(final String name, final String query)
            throws NameAlreadyExistsException, DatabaseEngineException {
        engine.createPreparedStatement(name, query);
    }

    @Override
    public void createPreparedStatement(final String name, final Expression query, final int timeout)
            throws NameAlreadyExistsException, DatabaseEngineException {
        engine.createPreparedStatement(name, query, timeout);
    }

    @Override
    public void createPreparedStatement(final String name, final String query, final int timeout)
            throws NameAlreadyExistsException, DatabaseEngineException {
        engine.createPreparedStatement(name, query, timeout);
    }

    @Override
    public void removePreparedStatement(final String name) {
        engine.removePreparedStatement(name);
    }

    @Override
    public List<Map<String, ResultColumn>> getPSResultSet(final String name)
            throws DatabaseEngineException, ConnectionResetException {
        return engine.getPSResultSet(name);
    }

    @Override
    public void setParameters(final String name, final Object... params)
            throws DatabaseEngineException, ConnectionResetException {
        engine.setParameters(name, params);
    }

    @Override
    public void setParameter(final String name, final int index, final Object param)
            throws DatabaseEngineException, ConnectionResetException {
        engine.setParameter(name, index, param);
    }

    @Override
    public void setParameter(final String name, final int index, final Object param, final DbColumnType paramType)
            throws DatabaseEngineException, ConnectionResetException {
        engine.setParameter(name, index, param, paramType);
    }

    @Override
    public void executePS(final String name) throws DatabaseEngineException, ConnectionResetException {
        engine.executePS(name);
    }

    @Override
    public void clearParameters(final String name) throws DatabaseEngineException, ConnectionResetException {
        engine.clearParameters(name);
    }

    @Override
    public boolean preparedStatementExists(final String name) {
        return engine.preparedStatementExists(name);
    }

    @Override
    public Integer executePSUpdate(final String name) throws DatabaseEngineException, ConnectionResetException {
        return engine.executePSUpdate(name);
    }

    @Override
    public String commentCharacter() {
        return engine.commentCharacter();
    }

    @Override
    public String escapeCharacter() {
        return engine.escapeCharacter();
    }

    @Override
    public Connection getConnection() throws RetryLimitExceededException, InterruptedException, RecoveryException {
        return engine.getConnection();
    }

    @Override
    public ResultIterator iterator(final String query) throws DatabaseEngineException {
        return engine.iterator(query);
    }

    @Override
    public ResultIterator iterator(final Expression query) throws DatabaseEngineException {
        return engine.iterator(query);
    }

    @Override
    public ResultIterator iterator(final String query, final int fetchSize) throws DatabaseEngineException {
        return engine.iterator(query, fetchSize);
    }

    @Override
    public ResultIterator iterator(final Expression query, final int fetchSize) throws DatabaseEngineException {
        return engine.iterator(query, fetchSize);
    }

    @Override
    public ResultIterator iterator(final String query,
                                   final int fetchSize,
                                   final int readTimeoutOverride) throws DatabaseEngineException {
        return engine.iterator(query, fetchSize, readTimeoutOverride);
    }

    @Override
    public ResultIterator iterator(final Expression query,
                                   final int fetchSize,
                                   final int readTimeoutOverride) throws DatabaseEngineException {
        return engine.iterator(query, fetchSize, readTimeoutOverride);
    }

    @Override
    public ResultIterator getPSIterator(final String name) throws DatabaseEngineException, ConnectionResetException {
        return engine.getPSIterator(name);
    }

    @Override
    public ResultIterator getPSIterator(final String name, final int fetchSize)
            throws DatabaseEngineException, ConnectionResetException {
        return engine.getPSIterator(name, fetchSize);
    }

    @Override
    public void setExceptionHandler(final ExceptionHandler eh) {
        engine.setExceptionHandler(eh);
    }

    @Override
    public boolean isStringAggDistinctCapable() {
        return engine.isStringAggDistinctCapable();
    }

    @Override
    public void dropView(final String view) throws DatabaseEngineException {
        engine.dropView(view);
    }
}
