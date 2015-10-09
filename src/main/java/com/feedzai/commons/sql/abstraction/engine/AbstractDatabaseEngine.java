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
import com.feedzai.commons.sql.abstraction.batch.DefaultBatch;
import com.feedzai.commons.sql.abstraction.ddl.*;
import com.feedzai.commons.sql.abstraction.dml.Expression;
import com.feedzai.commons.sql.abstraction.dml.dialect.Dialect;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.dml.result.ResultIterator;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.handler.ExceptionHandler;
import com.feedzai.commons.sql.abstraction.engine.handler.OperationFault;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.feedzai.commons.sql.abstraction.util.AESHelper;
import com.feedzai.commons.sql.abstraction.util.InitiallyReusableByteArrayOutputStream;
import com.feedzai.commons.sql.abstraction.util.PreparedStatementCapsule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.readString;

/**
 * Provides a set of functions to interact with the database.
 * <p>
 * This Engine already provides thread safeness to all public exposed methods
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public abstract class AbstractDatabaseEngine implements DatabaseEngine {
    /**
     * The Guice injector.
     */
    @Inject
    protected Injector injector;
    /**
     * The translator in place.
     */
    @Inject
    protected AbstractTranslator translator;
    /**
     * The default fetch size for iterators.
     */
    private static final int DEFAULT_FETCH_SIZE = 1000;
    /**
     * The database connection.
     */
    protected Connection conn;
    /**
     * The dev Marker.
     */
    protected final static Marker dev = MarkerFactory.getMarker("DEV");
    /**
     * Encoder object for blob columns
     *
     * @since 2.1.5
     */
    protected  BlobEncoder blobEncoder;
    /**
     * Maximum number of tries.
     */
    private final int maximumNumberOfTries;
    /**
     * Retry interval.
     */
    private final long retryInterval;
    /**
     * The logger that will be used.
     */
    protected Logger logger;
    /**
     * The notification logger for administration.
     */
    protected Logger notificationLogger;
    /**
     * Map of entities.
     */
    protected final Map<String, MappedEntity> entities = new HashMap<>();
    /**
     * Map of prepared statements.
     */
    protected final Map<String, PreparedStatementCapsule> stmts = new HashMap<String, PreparedStatementCapsule>();
    /**
     * The configuration.
     */
    protected final PdbProperties properties;
    /**
     * The dialect.
     */
    protected final Dialect diaclect;
    /**
     * The exception handler to control the flow when defining new entities.
     */
    protected ExceptionHandler eh;

    /**
     * Creates a new instance of {@link AbstractDatabaseEngine}.
     *
     * @param driver     The driver to connect to the database.
     * @param properties The properties of the connection.
     * @param dialect    The dialect in use.
     * @throws DatabaseEngineException When errors occur trying to set up the database connection.
     */
    public AbstractDatabaseEngine(String driver, final PdbProperties properties, final Dialect dialect) throws DatabaseEngineException {
        this.eh = ExceptionHandler.DEFAULT;
        this.properties = properties;
        // Check if the driver is to override.
        if (properties.isDriverSet()) {
            driver = properties.getDriver();
        }


        logger = LoggerFactory.getLogger(this.getClass());
        notificationLogger = LoggerFactory.getLogger("admin-notifications");

        this.diaclect = dialect;

        final String jdbc = this.properties.getJdbc();

        if (StringUtils.isBlank(jdbc)) {
            throw new DatabaseEngineException(String.format("%s property is mandatory", JDBC));
        }

        maximumNumberOfTries = this.properties.getMaxRetries();
        retryInterval = this.properties.getRetryInterval();
        this.blobEncoder = new BlobEncoder(this.properties.getBlobBufferSize());

        try {
            Class.forName(driver);
            connect();
        } catch (ClassNotFoundException ex) {
            throw new DatabaseEngineException("Driver not found", ex);
        } catch (SQLException ex) {
            throw new DatabaseEngineException("Unable to connect to database", ex);
        } catch (DatabaseEngineException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DatabaseEngineException("An unknown error occurred while establishing a connection to the database.", ex);
        }
    }

    /**
     * Reads the private key from the secret location.
     *
     * @return A string with the private key.
     * @throws Exception If something occurs while reading.
     */
    protected String getPrivateKey() throws Exception {
        String location = this.properties.getProperty(SECRET_LOCATION);
        if (StringUtils.isBlank(location)) {
            throw new DatabaseEngineException("Encryption was specified but there's no location specified for the private key.");
        }

        File f = new File(location);
        if (!f.canRead()) {
            throw new DatabaseEngineException("Specified file '" + location + "' does not exist or the application does not have read permissions over it.");
        }

        return readString(new FileInputStream(f)).trim();
    }

    /**
     * Connects to the database.
     *
     * @throws Exception If connection is not possible, or failed to decrypt username/password if encryption was provided.
     */
    protected void connect() throws Exception {
        String username = this.properties.getProperty(USERNAME);
        String password = this.properties.getProperty(PASSWORD);

        if (this.properties.isEncryptedPassword() || this.properties.isEncryptedUsername()) {
            String privateKey = getPrivateKey();
            if (this.properties.isEncryptedUsername()) {
                final String decryptedUsername = AESHelper.decrypt(this.properties.getProperty(ENCRYPTED_USERNAME), privateKey);
                if (StringUtils.isEmpty(decryptedUsername)) {
                    throw new DatabaseEngineException("The encrypted username could not be decrypted.");
                }
                username = decryptedUsername;

            }

            if (this.properties.isEncryptedPassword()) {
                final String decryptedPassword = AESHelper.decrypt(this.properties.getProperty(ENCRYPTED_PASSWORD), privateKey);
                if (StringUtils.isEmpty(decryptedPassword)) {
                    throw new DatabaseEngineException("The encrypted password could not be decrypted.");
                }
                password = decryptedPassword;
            }
        }

        String jdbc = getFinalJdbcConnection(this.properties.getProperty(JDBC));

        conn = DriverManager.getConnection(
                jdbc,
                username,
                password);

        setTransactionIsolation();

        logger.debug("Connection successful.");
    }

    /**
     * Gets the final JDBC connection.
     * <p>
     * Implementations might override this method in order to change the JDBC connection.
     *
     * @param jdbc The current JDBC connection.
     * @return The final JDBC connection.
     */
    protected String getFinalJdbcConnection(String jdbc) {
        return jdbc;
    }

    /**
     * Sets the transaction isolation level.
     *
     * @throws SQLException If a database access error occurs.
     */
    protected void setTransactionIsolation() throws SQLException {
        conn.setTransactionIsolation(properties.getIsolationLevel());
    }

    /**
     * Gets the class that translates SQL bound to this engine.
     *
     * @return The class that translates SQL bound to this engine.
     */
    public abstract Class<? extends AbstractTranslator> getTranslatorClass();

    /**
     * Checks if the connection is available and returns it. If the connection is not available, it tries to reconnect (the number of times defined in the
     * properties with the delay there specified).
     *
     * @return The connection.
     * @throws RetryLimitExceededException If the retry limit is exceeded.
     * @throws InterruptedException        If the thread is interrupted during reconnection.
     */
    @Override
    public synchronized Connection getConnection() throws RetryLimitExceededException, InterruptedException, RecoveryException {

        if (!properties.isReconnectOnLost()) {
            return conn;
        }

        int retries = 1;

        if (checkConnection(conn)) {
            return conn;
        }

        logger.debug("Connection is down.");

        // reconnect.
        while (true) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            try {

                if (maximumNumberOfTries > 0) {
                    if (retries == (maximumNumberOfTries / 2) || retries == (maximumNumberOfTries - 1)) {
                        logger.error("The connection to the database was lost. Remaining retries: {}", (maximumNumberOfTries - retries));
                        notificationLogger.error("The connection to the database was lost. Remaining retries: {}", (maximumNumberOfTries - retries));
                    } else {
                        logger.debug("Retrying ({}/{}) in {} seconds...", new Object[]{retries, maximumNumberOfTries, TimeUnit.MILLISECONDS.toSeconds(retryInterval)});
                    }
                } else {
                    logger.debug("Retry number {} in {} seconds...", retries, TimeUnit.MILLISECONDS.toSeconds(retryInterval));
                    if (retries % 10 == 0) {
                        notificationLogger.error("The connection to the database was lost. Retry number {} in {}", retries, TimeUnit.MILLISECONDS.toSeconds(retryInterval));
                    }
                }
                Thread.sleep(retryInterval);
                connect(); // this sets the new object.


                // recover state.

                try {
                    recover();
                } catch (Exception e) {
                    throw new RecoveryException("Error recovering from lost connection.", e);
                }

                // return it.
                return conn;
            } catch (SQLException ex) {

                logger.debug("Connection failed.");

                if (maximumNumberOfTries > 0 && retries > maximumNumberOfTries) {
                    throw new RetryLimitExceededException("Maximum number of retries for a connection exceeded.", ex);
                }

                retries++;
            } catch (Exception e) {
                logger.error("An unexpected error occurred.", e);
            }
        }
    }

    private synchronized void recover() throws DatabaseEngineException, NameAlreadyExistsException {
        // Recover entities.
        final Map<String, MappedEntity> niw = new HashMap<String, MappedEntity>(entities);
        // clear the entities
        entities.clear();
        for (MappedEntity me : niw.values()) {
            loadEntity(me.getEntity());
        }

        // Recover prepared statements.
        final Map<String, PreparedStatementCapsule> niwStmts = new HashMap<String, PreparedStatementCapsule>(stmts);
        for (Map.Entry<String, PreparedStatementCapsule> e : niwStmts.entrySet()) {
            createPreparedStatement(e.getKey(), e.getValue().query, e.getValue().timeout, true);
        }

        logger.debug("State recovered.");
    }

    /**
     * Closes the connection to the database.
     */
    @Override
    public synchronized void close() {
        try {
            if (properties.isSchemaPolicyCreateDrop()) {
                for (Map.Entry<String, MappedEntity> me : entities.entrySet()) {
                    try {
                        // Flush first
                        final PreparedStatement insert = me.getValue().getInsert();
                        final PreparedStatement insertReturning = me.getValue().getInsertReturning();
                        try {
                            insert.executeBatch();

                            if (insertReturning != null) {
                                insertReturning.executeBatch();

                            }
                        } catch (SQLException ex) {
                            logger.debug(String.format("Failed to flush before dropping entity '%s'", me.getValue().getEntity().getName()), ex);
                        } finally {
                            if (insert != null) {
                                try {
                                    insert.close();
                                } catch (Exception e) {
                                    logger.trace("Could not close prepared statement.", e);
                                }
                            }

                            if (insertReturning != null) {
                                try {
                                    insertReturning.close();
                                } catch (Exception e) {
                                    logger.trace("Could not close prepared statement.", e);
                                }
                            }
                        }

                        dropEntity(me.getValue().getEntity());
                    } catch (DatabaseEngineException ex) {
                        logger.debug(String.format("Failed to drop entity '%s'", me.getValue().getEntity().getName()), ex);
                    }
                }
            }

            conn.close();
            logger.debug("Connection to database closed");
        } catch (SQLException ex) {
            logger.warn("Unable to close connection");
        }
    }

    /**
     * Starts a transaction. Doing this will set auto commit to false ({@link Connection#getAutoCommit()}).
     *
     * @throws DatabaseEngineException If something goes wrong while persisting data.
     */
    @Override
    public synchronized void beginTransaction() throws DatabaseEngineRuntimeException {
        /*
         * It makes sense trying to reconnect since it's the beginning of a transaction.
         */
        try {
            getConnection();
            if (!conn.getAutoCommit()) { //I.E. Manual control
                logger.debug("There's already one transaction active");
                return;
            }

            conn.setAutoCommit(false);
        } catch (Exception ex) {
            throw new DatabaseEngineRuntimeException("Error occurred while starting transaction", ex);
        }
    }

    /**
     * Adds an entity to the engine. It will create tables and everything necessary so persistence can work.
     *
     * @param entity The entity to add.
     * @throws DatabaseEngineException If something goes wrong while creating the structures.
     */
    @Override
    public synchronized void addEntity(DbEntity entity) throws DatabaseEngineException {
        addEntity(entity, false);
    }

    @Override
    public synchronized void loadEntity(DbEntity entity) throws DatabaseEngineException {
        if (!entities.containsKey(entity.getName())) {
            validateEntity(entity);
            MappedEntity me = createPreparedStatementForInserts(entity).setEntity(entity);
            entities.put(entity.getName(), me);
            logger.trace("Entity '{}' loaded", entity.getName());
        }
    }

    /**
     * Adds an entity to the engine. It will create tables and everything necessary so persistence can work.
     *
     * @param entity     The entity to add.
     * @param recovering True if entities are being add due to recovery.
     * @throws DatabaseEngineException If something goes wrong while creating the structures.
     */
    private void addEntity(DbEntity entity, boolean recovering) throws DatabaseEngineException {
        if (!recovering) {
            try {
                getConnection();
            } catch (Exception e) {
                throw new DatabaseEngineException("Could not add entity", e);
            }

            validateEntity(entity);

            if (entities.containsKey(entity.getName())) {
                throw new DatabaseEngineException(String.format("Entity '%s' is already defined", entity.getName()));
            }

            if (this.properties.isSchemaPolicyDropCreate()) {
                dropEntity(entity);
            }
        }

        if (!this.properties.isSchemaPolicyNone()) {
            createTable(entity);
            addPrimaryKey(entity);
            addFks(entity);
            addIndexes(entity);
            addSequences(entity);
        }

        loadEntity(entity);
    }

    /**
     * <p>
     * Updates an entity in the engine.
     * </p>
     * <p>
     * If the entity does not exist in the instance, the method {@link #addEntity(com.feedzai.commons.sql.abstraction.ddl.DbEntity)} will be invoked.
     * </p>
     * <p>
     * The engine will compare the entity with the {@link #getMetadata(String)} information and update the schema of the table.
     * </p>
     * <p>
     * ATTENTION: This method will only add new columns or drop removed columns in the database table. It will also
     * drop and create foreign keys.
     * Primary Keys, Indexes and column types changes will not be updated.
     * </p>
     *
     * @param entity The entity to update.
     * @throws com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException
     * @since 12.1.0
     */
    @Override
    public synchronized void updateEntity(DbEntity entity) throws DatabaseEngineException {
        // Only mutate the schema in the DB (i.e. add schema, drop/add columns, if the schema policy allows it.
        if (!properties.isSchemaPolicyNone()) {
            final Map<String, DbColumnType> tableMetadata = getMetadata(entity.getName());
            if (tableMetadata.size() == 0) { // the table does not exist
                addEntity(entity);
            } else if (this.properties.isSchemaPolicyDropCreate()) {
                dropEntity(entity);
                addEntity(entity);
            } else {
                validateEntity(entity);
                dropFks(entity.getName());
                List<String> toRemove = new ArrayList<String>();
                for (String dbColumn : tableMetadata.keySet()) {
                    if (!entity.containsColumn(dbColumn)) {
                        toRemove.add(dbColumn);
                    }
                }
                if (!toRemove.isEmpty()) {
                    if (properties.allowColumnDrop()) {
                        dropColumn(entity, toRemove.toArray(new String[toRemove.size()]));
                    } else {
                        logger.warn("Need to remove {} columns to update {} entity, but property allowColumnDrop is set to false.", StringUtils.join(toRemove, ","), entity.getName());
                    }
                }
                List<DbColumn> columns = new ArrayList<DbColumn>();
                for (DbColumn localColumn : entity.getColumns()) {
                    if (!tableMetadata.containsKey(localColumn.getName())) {
                        columns.add(localColumn);

                    }
                }
                if (!columns.isEmpty()) {
                    addColumn(entity, columns.toArray(new DbColumn[columns.size()]));
                }
                addFks(entity);
            }
        }

        // We still want to create prepared statements for the entity, regardless the schema policy
        MappedEntity me = createPreparedStatementForInserts(entity);

        me = me.setEntity(entity);

        entities.put(entity.getName(), me);
        logger.trace("Entity '{}' updated", entity.getName());
    }

    @Override
    public synchronized DbEntity removeEntity(final String name) {

        final MappedEntity toRemove = entities.get(name);

        if (toRemove == null) {
            return null;
        }
        try {
            toRemove.getInsert().executeBatch();
        } catch (SQLException ex) {
            logger.debug("Could not flush before remove '{}'", name, ex);
        }

        if (properties.isSchemaPolicyCreateDrop()) {
            try {
                dropEntity(toRemove.getEntity());
            } catch (DatabaseEngineException ex) {
                logger.debug(String.format("Something went wrong while dropping entity '%s'", name), ex);
            }
        }

        entities.remove(name);

        return toRemove.getEntity();
    }

    @Override
    public synchronized boolean containsEntity(String name) {
        return entities.containsKey(name);
    }

    /**
     * Drops an entity.
     *
     * @param entity The entity name.
     * @throws com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException
     */
    @Override
    public synchronized void dropEntity(String entity) throws DatabaseEngineException {
        if (!containsEntity(entity)) {
            return;
        }

        dropEntity(entities.get(entity).getEntity());
    }

    /**
     * Drops everything that belongs to the entity.
     *
     * @param entity The entity.
     * @throws DatabaseEngineException If something goes wrong while dropping the structures.
     */
    public synchronized void dropEntity(final DbEntity entity) throws DatabaseEngineException {
        dropSequences(entity);
        dropTable(entity);
        entities.remove(entity.getName());
        logger.trace("Entity {} dropped", entity.getName());
    }

    /**
     * <p>Persists a given entry. Persisting a query implies executing the statement.</p> <p>If you are inside of an explicit transaction, changes will only be
     * visible upon explicit commit, otherwise a commit will immediately take place.</p>
     *
     * @param name  The entity name.
     * @param entry The entry to persist.
     * @return The ID of the auto generated value, null if there's no auto generated value.
     * @throws DatabaseEngineException If something goes wrong while persisting data.
     */
    @Override
    public abstract Long persist(final String name, final EntityEntry entry) throws DatabaseEngineException;

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
     * @return The ID of the auto generated value, null if there's no auto generated value.
     * @throws DatabaseEngineException If something goes wrong while persisting data.
     */
    @Override
    public abstract Long persist(final String name, final EntityEntry entry, final boolean useAutoInc) throws DatabaseEngineException;

    /**
     * Flushes the batches for all the registered entities.
     *
     * @throws DatabaseEngineException If something goes wrong while persisting data.
     */
    @Override
    public synchronized void flush() throws DatabaseEngineException {
        /*
         * Reconnect on this method does not make sense since a new connection will have nothing to flush.
         */

        try {
            for (MappedEntity me : entities.values()) {
                me.getInsert().executeBatch();
            }
        } catch (Exception ex) {
            throw new DatabaseEngineException("Something went wrong while flushing", ex);
        }
    }

    /**
     * Commits the current transaction. You should only call this method if you've previously called {@link AbstractDatabaseEngine#beginTransaction()}.
     *
     * @throws DatabaseEngineException If something goes wrong while persisting data.
     */
    @Override
    public synchronized void commit() throws DatabaseEngineRuntimeException {
        /*
         * Reconnect on this method does not make sense since a new connection will have nothing to commit.
         */
        try {
            conn.commit();
            conn.setAutoCommit(true);
        } catch (SQLException ex) {
            throw new DatabaseEngineRuntimeException("Something went wrong while committing transaction", ex);
        }
    }

    /**
     * Rolls back a transaction. You should only call this method if you've previously called {@link AbstractDatabaseEngine#beginTransaction()}.
     *
     * @throws DatabaseEngineRuntimeException If the rollback fails.
     */
    @Override
    public synchronized void rollback() throws DatabaseEngineRuntimeException {
        /*
         * Reconnect on this method does not make sense since a new connection will have nothing to rollback.
         */
        try {
            conn.rollback();
            conn.setAutoCommit(true);
        } catch (SQLException ex) {
            throw new DatabaseEngineRuntimeException("Something went wrong while rolling back the transaction", ex);
        }
    }

    /**
     * @return True if the transaction is active.
     * @throws DatabaseEngineRuntimeException If the access to the database fails.
     */
    @Override
    public synchronized boolean isTransactionActive() throws DatabaseEngineRuntimeException {
        /*
         * This method is used to know if a rollback is needed like: beginTransaction(); try { // Do something commit(); } finally { if (isTransactionActive())
         * { rollback(); } }
         *
         * So reconnection in this case does not make sense either.
         */
        try {
            return !conn.getAutoCommit();
        } catch (SQLException ex) {
            throw new DatabaseEngineRuntimeException("Something went wrong while rolling back the transaction", ex);
        }
    }

    /**
     * Executes a native query.
     *
     * @param query The query to execute.
     * @return The number of rows updated.
     * @throws DatabaseEngineException If something goes wrong executing the native query.
     */
    @Override
    public synchronized int executeUpdate(final String query) throws DatabaseEngineException {
        Statement s = null;
        try {
            getConnection();
            s = conn.createStatement();
            return s.executeUpdate(query);
        } catch (Exception ex) {
            throw new DatabaseEngineException("Error handling native query", ex);
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (Exception e) {
                    logger.trace("Error closing statement.", e);
                }
            }
        }
    }

    /**
     * Executes the given update.
     *
     * @param query The update to execute.
     * @throws DatabaseEngineException If something goes wrong executing the update.
     */
    @Override
    public synchronized int executeUpdate(final Expression query) throws DatabaseEngineException {
        /*
         * Reconnection is already assured by "void executeUpdate(final String query)".
         */
        final String trans = translate(query);
        logger.trace(trans);
        return executeUpdate(trans);
    }

    /**
     * Translates the given expression to the current dialect.
     *
     * @param query The query to translate.
     * @return The translation result.
     */
    @Override
    public String translate(final Expression query) {
        inject(query);
        return query.translate();
    }

    /**
     * @return The dialect being in use.
     */
    @Override
    public Dialect getDialect() {
        return diaclect;
    }

    /**
     * Creates a new batch that periodically flushes a batch. A flush will also occur when the maximum number of statements in the batch is reached.
     * <p>
     * Please be sure to call {@link com.feedzai.commons.sql.abstraction.batch.AbstractBatch#destroy() } before closing the session with the database
     *
     * @param batchSize    The batch size.
     * @param batchTimeout If inserts do not occur after the specified time, a flush will be performed.
     * @return The batch.
     */
    @Override
    public AbstractBatch createBatch(final int batchSize, final long batchTimeout) {
        return createBatch(batchSize, batchTimeout, null);
    }

    @Override
    public AbstractBatch createBatch(final int batchSize, final long batchTimeout, final String batchName) {
        return DefaultBatch.create(this, batchName, batchSize, batchTimeout, properties.getMaximumAwaitTimeBatchShutdown());
    }

    /**
     * Validates the entity before trying to create the schema.
     *
     * @param entity The entity to validate.
     * @throws DatabaseEngineException If the validation fails.
     */
    private void validateEntity(final DbEntity entity) throws DatabaseEngineException {
        if (entity.getName() == null || entity.getName().length() == 0) {
            throw new DatabaseEngineException("You have to define the entity name");
        }

        final int maxIdentSize = properties.getMaxIdentifierSize();
        if (entity.getName().length() > maxIdentSize) {
            throw new DatabaseEngineException(String.format("Entity '%s' exceeds the maximum number of characters (%d)", entity.getName(), maxIdentSize));
        }

        if (entity.getColumns().isEmpty()) {
            throw new DatabaseEngineException("You have to specify at least one column");
        }

        int numberOfAutoIncs = 0;
        for (DbColumn c : entity.getColumns()) {
            if (c.getName() == null || c.getName().length() == 0) {
                throw new DatabaseEngineException(String.format("Column in entity '%s' must have a name", entity.getName()));
            }

            if (c.getName().length() > maxIdentSize) {
                throw new DatabaseEngineException(String.format("Column '%s' in entity '%s' exceeds the maximum number of characters (%d)", c.getName(), entity.getName(), maxIdentSize));
            }

            if (c.isAutoInc()) {
                numberOfAutoIncs++;
            }
        }

        if (numberOfAutoIncs > 1) {
            throw new DatabaseEngineException("You can only define one auto incremented column");
        }

        // Index validation

        List<DbIndex> indexes = entity.getIndexes();
        if (!indexes.isEmpty()) {
            for (DbIndex index : indexes) {
                if (index.getColumns().isEmpty()) {
                    throw new DatabaseEngineException(String.format("You have to specify at least one column to create an index in entity '%s'", entity.getName()));
                }

                for (String column : index.getColumns()) {
                    if (column == null || column.length() == 0) {
                        throw new DatabaseEngineException(String.format("Column indexes must have a name in entity '%s'", entity.getName()));
                    }
                }
            }
        }

        // FK validation

        List<DbFk> fks = entity.getFks();
        if (!fks.isEmpty()) {
            for (DbFk fk : fks) {
                if (fk.getForeignTable() == null || fk.getForeignTable().length() == 0) {
                    throw new DatabaseEngineException(String.format("You have to specify the table when creating a Foreign Key in entity '%s'", entity.getName()));
                }

                if (fk.getLocalColumns().isEmpty()) {
                    throw new DatabaseEngineException(String.format("You must specify at least one local column when defining a Foreign Key in '%s'", entity.getName()));
                }

                if (fk.getForeignColumns().isEmpty()) {
                    throw new DatabaseEngineException(String.format("You must specify at least one foreign column when defining a Foreign Key in '%s'", entity.getName()));
                }

                if (fk.getLocalColumns().size() != fk.getForeignColumns().size()) {
                    throw new DatabaseEngineException(String.format("Number of local columns does not match foreign ones in entity '%s'", entity.getName()));
                }
            }
        }
    }

    /**
     * Checks if the connection is alive.
     *
     * @param conn The connection to test.
     * @return True if the connection is valid, false otherwise.
     */
    protected abstract boolean checkConnection(final Connection conn);

    /**
     * Checks if the connection is alive.
     *
     * @param forceReconnect True to force the connection in case of failure.
     * @return True if the connection is valid, false otherwise.
     */
    @Override
    public synchronized boolean checkConnection(final boolean forceReconnect) {
        if (checkConnection(conn)) {
            return true;
        } else if (forceReconnect) {
            try {
                connect();
                recover();

                return true;
            } catch (Exception ex) {
                logger.debug(dev, "reconnection failure", ex);
                return false;
            }
        }

        return false;
    }

    /**
     * Checks if the connection is alive.
     *
     * @return True if the connection is valid, false otherwise.
     */
    @Override
    public synchronized boolean checkConnection() {
        return checkConnection(false);
    }

    /**
     * Add an entry to the batch.
     *
     * @param name  The entity name.
     * @param entry The entry to persist.
     * @throws DatabaseEngineException If something goes wrong while persisting data.
     */
    @Override
    public synchronized void addBatch(final String name, final EntityEntry entry) throws DatabaseEngineException {

        try {

            final MappedEntity me = entities.get(name);

            if (me == null) {
                throw new DatabaseEngineException(String.format("Unknown entity '%s'", name));
            }

            PreparedStatement ps = me.getInsert();
            entityToPreparedStatement(me.getEntity(), ps, entry, true);

            ps.addBatch();
        } catch (Exception ex) {
            throw new DatabaseEngineException("Error adding to batch", ex);
        }

    }

    /**
     * Translates the given entry entity to the prepared statement.
     *
     * @param entity The entity.
     * @param ps     The prepared statement.
     * @param entry  The entry.
     * @return The prepared statement filled in.
     * @throws DatabaseEngineException if something occurs during the translation.
     */
    protected abstract int entityToPreparedStatement(final DbEntity entity, final PreparedStatement ps, final EntityEntry entry, final boolean useAutoIncs) throws DatabaseEngineException;

    /**
     * Executes the given query.
     *
     * @param query The query to execute.
     * @throws DatabaseEngineException If something goes wrong executing the query.
     */
    @Override
    public synchronized List<Map<String, ResultColumn>> query(final Expression query) throws DatabaseEngineException {
        return query(translate(query));
    }

    /**
     * Executes the given query.
     *
     * @param query The query to execute.
     * @throws DatabaseEngineException If something goes wrong executing the query.
     */
    @Override
    public synchronized List<Map<String, ResultColumn>> query(final String query) throws DatabaseEngineException {
        return processResultIterator(iterator(query));
    }

    /**
     * Process a whole {@link ResultIterator}.
     *
     * @param it The iterator.
     * @return A list of rows in the form of {@link Map}.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    protected List<Map<String, ResultColumn>> processResultIterator(ResultIterator it) throws DatabaseEngineException {
        List<Map<String, ResultColumn>> res = new ArrayList<>();

        Map<String, ResultColumn> temp;
        while ((temp = it.next()) != null) {
            res.add(temp);
        }

        return res;
    }

    @Override
    public synchronized ResultIterator iterator(String query) throws DatabaseEngineException {
        return iterator(query, DEFAULT_FETCH_SIZE);
    }

    @Override
    public synchronized ResultIterator iterator(Expression query) throws DatabaseEngineException {
        return iterator(query, DEFAULT_FETCH_SIZE);
    }

    @Override
    public ResultIterator iterator(String query, int fetchSize) throws DatabaseEngineException {
        try {
            getConnection();
            Statement stmt = conn.createStatement();
            stmt.setFetchSize(fetchSize);
            logger.trace(query);
            return createResultIterator(stmt, query);

        } catch (final Exception e) {
            throw new DatabaseEngineException("Error querying", e);
        }
    }

    @Override
    public ResultIterator iterator(Expression query, int fetchSize) throws DatabaseEngineException {
        return iterator(translate(query), fetchSize);
    }

    /**
     * Creates a specific {@link ResultIterator} given the engine implementation.
     *
     * @param statement The statement.
     * @param sql       The SQL sentence.
     * @return An iterator for the specific engine.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    protected abstract ResultIterator createResultIterator(Statement statement, String sql) throws DatabaseEngineException;

    /**
     * Creates the table.
     *
     * @param entity The entity.
     * @throws DatabaseEngineException If something goes wrong while creating the table.
     */
    protected abstract void createTable(final DbEntity entity) throws DatabaseEngineException;

    /**
     * Add a primary key to the entity.
     *
     * @param entity The entity.
     * @throws DatabaseEngineException If something goes wrong while creating the table.
     */
    protected abstract void addPrimaryKey(final DbEntity entity) throws DatabaseEngineException;

    /**
     * Add the desired indexes.
     *
     * @param entity The entity.
     * @throws DatabaseEngineException If something goes wrong while creating the table.
     */
    protected abstract void addIndexes(final DbEntity entity) throws DatabaseEngineException;

    /**
     * Adds the necessary sequences.
     *
     * @param entity The entity.
     * @throws DatabaseEngineException If something goes wrong while creating the table.
     */
    protected abstract void addSequences(final DbEntity entity) throws DatabaseEngineException;

    /**
     * Drops the sequences of the entity.
     *
     * @param entity The entity.
     * @throws DatabaseEngineException If something goes wrong dropping the sequences.
     */
    protected abstract void dropSequences(final DbEntity entity) throws DatabaseEngineException;

    /**
     * Drops the table.
     *
     * @param entity The entity.
     * @throws DatabaseEngineException If something goes wrong dropping the sequences.
     */
    protected abstract void dropTable(final DbEntity entity) throws DatabaseEngineException;

    /**
     * Drops the column.
     *
     * @param entity  The entity.
     * @param columns The column name to drop.
     * @throws DatabaseEngineException If something goes wrong dropping the sequences.
     * @since 12.1.0
     */
    protected abstract void dropColumn(final DbEntity entity, final String... columns) throws DatabaseEngineException;

    /**
     * Adds the column to an existent table.
     *
     * @param entity  The entity that represents the table.
     * @param columns The db column to add.
     * @throws DatabaseEngineException
     * @since 12.1.0
     */
    protected abstract void addColumn(final DbEntity entity, final DbColumn... columns) throws DatabaseEngineException;

    /**
     * Adds the FKs.
     *
     * @param entity The entity.
     * @throws DatabaseEngineException If something goes wrong creating the FKs.
     */
    protected abstract void addFks(final DbEntity entity) throws DatabaseEngineException;

    /**
     * Creates and gets the prepared statement that will be used for insertions.
     *
     * @param entity The entity.
     * @throws DatabaseEngineException If something goes wrong while creating the table.
     */
    protected abstract MappedEntity createPreparedStatementForInserts(final DbEntity entity) throws DatabaseEngineException;

    /**
     * Translates the type present in the given column.
     *
     * @param c The column.
     * @return The translation.
     * @throws DatabaseEngineException If the type cannot be found.
     */
    protected abstract String translateType(final DbColumn c) throws DatabaseEngineException;

    /**
     * Creates a specific {@link ResultIterator} for the engine in place given given prepared statement.
     *
     * @param ps The prepared statement.
     * @return The result iterator.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    protected abstract ResultIterator createResultIterator(PreparedStatement ps) throws DatabaseEngineException;


    /**
     * Drops this table foreign keys.
     *
     * @param table The table name.
     * @throws Exception
     */
    protected void dropFks(final String table) throws DatabaseEngineException {
        String schema = StringUtils.stripToNull(properties.getSchema());
        ResultSet rs = null;
        try {
            getConnection();
            rs = conn.getMetaData().getImportedKeys(null, schema, table);
            Set<String> fks = new HashSet<String>();
            while (rs.next()) {
                fks.add(rs.getString("FK_NAME"));
            }
            for (String fk : fks) {
                try {
                    executeUpdate(String.format("ALTER TABLE %s DROP CONSTRAINT %s", quotize(table, escapeCharacter()), quotize(fk, escapeCharacter())));
                } catch (Exception e) {
                    logger.warn("Could not drop foreign key '{}' on table '{}'", fk, table);
                    logger.debug("Could not drop foreign key.", e);
                }
            }
        } catch (Exception e) {
            throw new DatabaseEngineException("Error dropping foreign key", e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception a) {
                logger.trace("Error closing result set.", a);
            }
        }
    }

    /**
     * Gets the database entities.
     *
     * @return The list of database entities and types (tables, views, procedures, and so on).
     * @throws DatabaseEngineException If something occurs getting the existing tables.
     */
    @Override
    public synchronized Map<String, DbEntityType> getEntities() throws DatabaseEngineException {
        final Map<String, DbEntityType> entities = new LinkedHashMap<String, DbEntityType>();
        ResultSet rs = null;

        try {
            getConnection();

            // restrict database schema if it exists
            String schema = properties.isSchemaSet() ? properties.getSchema() : null;

            // get the entities
            rs = conn.getMetaData().getTables(null, schema, "%", null);

            while (rs.next()) {
                String entityName = rs.getString("table_name");
                String entityType = rs.getString("table_type");

                DbEntityType type;

                // tag the entities
                if ("TABLE".equals(entityType)) {
                    type = DbEntityType.TABLE;
                } else if ("VIEW".equals(entityType)) {
                    type = DbEntityType.VIEW;
                } else if ("SYSTEM TABLE".equals(entityType)) {
                    type = DbEntityType.SYSTEM_TABLE;
                } else if ("SYSTEM VIEW".equals(entityType)) {
                    type = DbEntityType.SYSTEM_VIEW;
                } else {
                    type = DbEntityType.UNMAPPED;
                }

                entities.put(entityName, type);
            }

            return entities;
        } catch (Exception e) {
            throw new DatabaseEngineException("Could not get entities", e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception a) {
                logger.trace("Error closing result set.", a);
            }
        }
    }

    /**
     * Executes the given statement.
     * <p>
     * If the statement for some reason fails to execute, the error is logged
     * but no exception is thrown.
     *
     * @param statement The statement.
     */
    protected void executeUpdateSilently(String statement) {
        logger.trace(statement);
        try (Statement alter = conn.createStatement()) {
            alter.execute(statement);
        } catch (SQLException e) {
            logger.debug("Could not execute {}.", statement, e);
        }
    }

    /**
     * Gets the schema.
     *
     * @return The schema.
     */
    protected String getSchema() {
        return properties.getSchema();
    }

    /**
     * Gets the table metadata.
     *
     * @return A representation of the table columns and types.
     * @throws DatabaseEngineException If something occurs getting the metadata.
     */
    @Override
    public synchronized Map<String, DbColumnType> getMetadata(final String name) throws DatabaseEngineException {
        final Map<String, DbColumnType> metaMap = new LinkedHashMap<>();

        ResultSet rsColumns = null;
        try {
            getConnection();


            final DatabaseMetaData meta = conn.getMetaData();
            rsColumns = meta.getColumns(null, getSchema(), name, null);
            while (rsColumns.next()) {
                metaMap.put(rsColumns.getString("COLUMN_NAME"), toPdbType(rsColumns.getInt("DATA_TYPE")));
            }

            return metaMap;
        } catch (Exception e) {
            throw new DatabaseEngineException("Could not get metadata", e);
        } finally {
            try {
                if (rsColumns != null) {
                    rsColumns.close();
                }
            } catch (Exception a) {
                logger.trace("Error closing result set.", a);
            }
        }
    }

    /**
     * Gets the query metadata.
     *
     * @param query The query to retrieve the metadata from.
     * @return A representation of the table columns and types.
     * @throws DatabaseEngineException If something occurs getting the metadata.
     */
    @Override
    public Map<String, DbColumnType> getQueryMetadata(Expression query) throws DatabaseEngineException {
        final String queryString = translate(query);
        logger.trace(queryString);
        return getQueryMetadata(queryString);
    }

    /**
     * Gets the query metadata.
     *
     * @param query The query to retrieve the metadata from.
     * @return A representation of the table columns and types.
     * @throws DatabaseEngineException If something occurs getting the metadata.
     */
    @Override
    public Map<String, DbColumnType> getQueryMetadata(String query) throws DatabaseEngineException {
        final Map<String, DbColumnType> metaMap = new LinkedHashMap<String, DbColumnType>();
        ResultSet rs = null;
        Statement stmt = null;

        try {
            getConnection();
            stmt = conn.createStatement();
            long start = System.currentTimeMillis();
            rs = stmt.executeQuery(query);
            logger.trace("[{} ms] {}", (System.currentTimeMillis() - start), query);

            ResultSetMetaData meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                metaMap.put(meta.getColumnName(i), toPdbType(meta.getColumnType(i)));
            }

            stmt.close();

            return metaMap;
        } catch (final Exception e) {
            throw new DatabaseEngineException("Error querying", e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception e) {
                logger.trace("Error closing result set.", e);
            }

            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }
    }

    /**
     * Maps the database type to {@link DbColumnType}. If there's no mapping a {@link DbColumnType#UNMAPPED} is returned.
     *
     * @param type The SQL type from {@link java.sql.Types}.
     * @return The {@link DbColumnType}.
     */
    protected DbColumnType toPdbType(final int type) {
        switch (type) {
            case Types.ARRAY:
                return DbColumnType.UNMAPPED;
            case Types.BIGINT:
                return DbColumnType.LONG;
            case Types.BINARY:
                return DbColumnType.BLOB;
            case Types.BIT:
                return DbColumnType.BOOLEAN;
            case Types.BLOB:
                return DbColumnType.BLOB;
            case Types.BOOLEAN:
                return DbColumnType.BOOLEAN;
            case Types.CHAR:
                return DbColumnType.STRING;
            case Types.CLOB:
                return DbColumnType.STRING;
            case Types.DATALINK:
                return DbColumnType.UNMAPPED;
            case Types.DATE:
                return DbColumnType.UNMAPPED;
            case Types.DECIMAL:
                return DbColumnType.DOUBLE;
            case Types.DISTINCT:
                return DbColumnType.UNMAPPED;
            case Types.DOUBLE:
                return DbColumnType.DOUBLE;
            case Types.FLOAT:
                return DbColumnType.DOUBLE;
            case Types.INTEGER:
                return DbColumnType.INT;
            case Types.JAVA_OBJECT:
                return DbColumnType.BLOB;
            case Types.LONGNVARCHAR:
                return DbColumnType.STRING;
            case Types.LONGVARBINARY:
                return DbColumnType.BLOB;
            case Types.LONGVARCHAR:
                return DbColumnType.STRING;
            case Types.NCHAR:
                return DbColumnType.STRING;
            case Types.NCLOB:
                return DbColumnType.STRING;
            case Types.NULL:
                return DbColumnType.UNMAPPED;
            case Types.NUMERIC:
                return DbColumnType.DOUBLE;
            case Types.NVARCHAR:
                return DbColumnType.STRING;
            case Types.OTHER:
                return DbColumnType.UNMAPPED;
            case Types.REAL:
                return DbColumnType.DOUBLE;
            case Types.REF:
                return DbColumnType.UNMAPPED;
            case Types.ROWID:
                return DbColumnType.STRING;
            case Types.SMALLINT:
                return DbColumnType.INT;
            case Types.SQLXML:
                return DbColumnType.STRING;
            case Types.STRUCT:
                return DbColumnType.UNMAPPED;
            case Types.TIME:
                return DbColumnType.UNMAPPED;
            case Types.TIMESTAMP:
                return DbColumnType.LONG;
            case Types.TINYINT:
                return DbColumnType.INT;
            case Types.VARBINARY:
                return DbColumnType.BLOB;
            case Types.VARCHAR:
                return DbColumnType.STRING;
            default:
                return DbColumnType.UNMAPPED;
        }
    }

    /**
     * Duplicates a connection.
     *
     * @param mergeProperties Merge properties with the ones already existing.
     * @param copyEntities    True to include the entities in the new connection, false otherwise.
     * @return The new connection.
     * @throws DuplicateEngineException If policy is set to other than 'create' or 'none' or duplication fails for some reason.
     */
    @Override
    public synchronized DatabaseEngine duplicate(Properties mergeProperties, final boolean copyEntities) throws DuplicateEngineException {
        if (mergeProperties == null) {
            mergeProperties = new Properties();
        }

        final PdbProperties niwProps = properties.clone();
        niwProps.merge(mergeProperties);


        if (!(niwProps.isSchemaPolicyNone() || niwProps.isSchemaPolicyCreate())) {
            throw new DuplicateEngineException("Duplicate can only be called if pdb.policy is set to 'create' or 'none'");
        }

        try {
            DatabaseEngine niw = DatabaseFactory.getConnection(niwProps);

            if (copyEntities) {
                for (MappedEntity entity : entities.values()) {
                    niw.addEntity(entity.getEntity());
                }
            }

            return niw;
        } catch (Exception e) {
            throw new DuplicateEngineException("Could not duplicate connection", e);
        }
    }

    /**
     * @return A clone of the properties in use.
     */
    @Override
    public PdbProperties getProperties() {
        return properties.clone();
    }

    @Override
    public synchronized void createPreparedStatement(final String name, final Expression query) throws NameAlreadyExistsException, DatabaseEngineException {
        createPreparedStatement(name, query, -1);
    }

    @Override
    public synchronized void createPreparedStatement(final String name, final String query) throws NameAlreadyExistsException, DatabaseEngineException {
        createPreparedStatement(name, query, -1);
    }

    @Override
    public synchronized void createPreparedStatement(final String name, final Expression query, final int timeout) throws NameAlreadyExistsException, DatabaseEngineException {
        final String queryString = translate(query);
        logger.trace(queryString);

        createPreparedStatement(name, queryString, timeout);
    }

    @Override
    public synchronized void createPreparedStatement(final String name, final String query, final int timeout) throws NameAlreadyExistsException, DatabaseEngineException {
        createPreparedStatement(name, query, timeout, false);
    }

    @Override
    public synchronized void removePreparedStatement(String name) {
        final PreparedStatementCapsule ps = stmts.remove(name);
        if (ps == null) {
            return;
        }

        try {
            ps.ps.close();
        } catch (SQLException e) {
            logger.debug("Error closing prepared statement '{}'.", name, e);
        }
    }

    /**
     * Gets the result set of the specified prepared statement.
     *
     * @param name The prepared statement name.
     * @return The result.
     * @throws DatabaseEngineException If something occurs getting the result.
     */
    @Override
    public synchronized List<Map<String, ResultColumn>> getPSResultSet(final String name) throws DatabaseEngineException {
        return processResultIterator(getPSIterator(name));
    }

    @Override
    public synchronized ResultIterator getPSIterator(String name) throws DatabaseEngineException {
        return getPSIterator(name, DEFAULT_FETCH_SIZE);
    }

    @Override
    public ResultIterator getPSIterator(String name, int fetchSize) throws DatabaseEngineException {
        final PreparedStatementCapsule ps = stmts.get(name);
        if (ps == null) {
            throw new DatabaseEngineRuntimeException(String.format("PreparedStatement named '%s' does not exist", name));
        }

        try {
            ps.ps.setFetchSize(fetchSize);
        } catch (SQLException e) {
            throw new DatabaseEngineException("Error creating PS Iterator", e);
        }

        return createResultIterator(ps.ps);
    }

    @Override
    public synchronized void setParameters(final String name, final Object... params) throws DatabaseEngineException, ConnectionResetException {
        final PreparedStatementCapsule ps = stmts.get(name);
        if (ps == null) {
            throw new DatabaseEngineRuntimeException(String.format("PreparedStatement named '%s' does not exist", name));
        }
        int i = 1;
        for (Object o : params) {
            try {
                if (o instanceof byte[]) {
                    ps.ps.setBytes(i, (byte[]) o);
                } else {
                    ps.ps.setObject(i, o);
                }
            } catch (SQLException ex) {
                if (checkConnection(conn) || !properties.isReconnectOnLost()) {
                    throw new DatabaseEngineException("Could not set parameters", ex);
                }

                // At this point maybe it is an error with the connection, so we try to re-establish it.
                try {
                    getConnection();
                } catch (Exception e2) {
                    throw new DatabaseEngineException("Connection is down", e2);
                }

                throw new ConnectionResetException("Connection was lost, you must reset the prepared statement parameters and re-execute the statement");
            }
            i++;
        }
    }

    @Override
    public synchronized void setParameter(final String name, final int index, final Object param) throws DatabaseEngineException, ConnectionResetException {
        final PreparedStatementCapsule ps = stmts.get(name);
        if (ps == null) {
            throw new DatabaseEngineRuntimeException(String.format("PreparedStatement named '%s' does not exist", name));
        }

        try {
            if (param instanceof byte[]) {
                ps.ps.setBytes(index, (byte[]) param);
            } else {
                ps.ps.setObject(index, param);
            }
        } catch (SQLException ex) {
            if (checkConnection(conn) || !properties.isReconnectOnLost()) {
                throw new DatabaseEngineException("Could not set parameter", ex);
            }

            // At this point maybe it is an error with the connection, so we try to re-establish it.
            try {
                getConnection();
            } catch (Exception e2) {
                throw new DatabaseEngineException("Connection is down", e2);
            }

            throw new ConnectionResetException("Connection was lost, you must reset the prepared statement parameters and re-execute the statement");
        }
    }

    /**
     * Executes the specified prepared statement.
     *
     * @param name The prepared statement name.
     * @throws DatabaseEngineException  If something goes wrong while executing.
     * @throws ConnectionResetException If the connection is down and reestablishment occurs. If this happens, the user must reset the parameters and re-execute
     *                                  the query.
     */
    @Override
    public synchronized void executePS(final String name) throws DatabaseEngineException, ConnectionResetException {
        final PreparedStatementCapsule ps = stmts.get(name);
        if (ps == null) {
            throw new DatabaseEngineRuntimeException(String.format("PreparedStatement named '%s' does not exist", name));
        }

        try {
            ps.ps.execute();

        } catch (SQLException e) {
            if (checkConnection(conn) || !properties.isReconnectOnLost()) {
                throw new DatabaseEngineException(String.format("Something went wrong executing the prepared statement '%s'", name), e);
            }

            // At this point maybe it is an error with the connection, so we try to re-establish it.
            try {
                getConnection();
            } catch (Exception e2) {
                throw new DatabaseEngineException("Connection is down", e2);
            }

            throw new ConnectionResetException("Connection was lost, you must reset the prepared statement parameters and re-execute the statement");
        }
    }

    /**
     * Clears the prepared statement parameters.
     *
     * @param name The prepared statement name.
     * @throws DatabaseEngineException If something occurs while clearing the parameters.
     */
    @Override
    public synchronized void clearParameters(final String name) throws DatabaseEngineException, ConnectionResetException {
        final PreparedStatementCapsule ps = stmts.get(name);
        if (ps == null) {
            throw new DatabaseEngineRuntimeException(String.format("PreparedStatement named '%s' does not exist", name));
        }

        try {
            ps.ps.clearParameters();
        } catch (SQLException ex) {
            if (checkConnection(conn) || !properties.isReconnectOnLost()) {
                throw new DatabaseEngineException("Error clearing parameters", ex);
            }

            // At this point maybe it is an error with the connection, so we try to re-establish it.
            try {
                getConnection();
            } catch (Exception e2) {
                throw new DatabaseEngineException("Connection is down", e2);
            }

            throw new ConnectionResetException("Connection was reset.");
        }
    }

    /**
     * Checks if there's a prepared statement with the given name.
     *
     * @param name The name to test.
     * @return True if it exists, false otherwise.
     */
    @Override
    public synchronized boolean preparedStatementExists(final String name) {
        return stmts.containsKey(name);
    }

    /**
     * Executes update on the specified prepared statement.
     *
     * @param name The prepared statement name.
     * @throws DatabaseEngineException  If the prepared statement does not exist or something goes wrong while executing.
     * @throws ConnectionResetException If the connection is down and reestablishment occurs. If this happens, the user must reset the parameters and re-execute
     *                                  the query.
     */
    @Override
    public synchronized Integer executePSUpdate(final String name) throws DatabaseEngineException, ConnectionResetException {
        final PreparedStatementCapsule ps = stmts.get(name);
        if (ps == null) {
            throw new DatabaseEngineRuntimeException(String.format("PreparedStatement named '%s' does not exist", name));
        }

        try {
            return ps.ps.executeUpdate();

        } catch (SQLException e) {
            if (checkConnection(conn) || !properties.isReconnectOnLost()) {
                throw new DatabaseEngineException(String.format("Something went wrong executing the prepared statement '%s'", name), e);
            }

            // At this point maybe it is an error with the connection, so we try to re-establish it.
            try {
                getConnection();
            } catch (Exception e2) {
                throw new DatabaseEngineException("Connection is down", e2);
            }

            throw new ConnectionResetException("Connection was lost, you must reset the prepared statement parameters and re-execute the statement");
        }
    }

    /**
     * Creates a prepared statement.
     *
     * @param name       The name of the prepared statement.
     * @param query      The query.
     * @param timeout    The timeout (in seconds) if applicable. Only applicable if > 0.
     * @param recovering True if calling from recovering, false otherwise.
     * @throws NameAlreadyExistsException If the name already exists.
     * @throws DatabaseEngineException    If something goes wrong creating the statement.
     */
    private void createPreparedStatement(final String name, final String query, final int timeout, final boolean recovering) throws NameAlreadyExistsException, DatabaseEngineException {

        if (!recovering) {
            if (stmts.containsKey(name)) {
                throw new NameAlreadyExistsException(String.format("There's already a PreparedStatement with the name '%s'", name));
            }
            try {
                getConnection();
            } catch (Exception e) {
                throw new DatabaseEngineException("Could not create prepared statement", e);
            }
        }

        PreparedStatement ps;
        try {
            ps = conn.prepareStatement(query);
            if (timeout > 0) {
                ps.setQueryTimeout(timeout);
            }
            stmts.put(name, new PreparedStatementCapsule(query, ps, timeout));
        } catch (SQLException e) {
            throw new DatabaseEngineException("Could not create prepared statement", e);
        }
    }

    @Override
    public String commentCharacter() {
        return "--";
    }

    @Override
    public String escapeCharacter() {
        return translator.translateEscape();
    }

    @Override
    public synchronized void setExceptionHandler(ExceptionHandler eh) {
        this.eh = eh;
    }

    /**
     * Controls if the given faulty operation must be silenced or not. The decision can be controlled by an external party
     * using for that matter the {@link #setExceptionHandler(ExceptionHandler)} method to provide a
     * specific implementation.
     *
     * @param op The operation that originated the exception.
     * @param e  The exception.
     * @throws DatabaseEngineException If the faulty operation must stop the execution.
     */
    protected void handleOperation(OperationFault op, Exception e) throws DatabaseEngineException {
        if (!eh.proceed(op, e)) {
            throw new DatabaseEngineException("An error occurred adding the entity.", e);
        }
    }

    /**
     * Check if the entity has an identity column.
     *
     * @param entity The entity to check.
     * @return True if the entity has an identity column and false otherwise.
     */
    public boolean hasIdentityColumn(DbEntity entity) {
        for (DbColumn column : entity.getColumns()) {
            if (column.isAutoInc()) {
                return true;
            }
        }
        return false;
    }

    protected void inject(Expression... objs) {
        for (Object o : objs) {
            if (o == null) {
                continue;
            }

            injector.injectMembers(o);
        }
    }
}
