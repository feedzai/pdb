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
package com.feedzai.commons.sql.abstraction.engine.impl;

import com.feedzai.commons.sql.abstraction.ddl.DbColumn;
import com.feedzai.commons.sql.abstraction.ddl.DbColumnConstraint;
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.ddl.DbFk;
import com.feedzai.commons.sql.abstraction.ddl.DbIndex;
import com.feedzai.commons.sql.abstraction.dml.dialect.Dialect;
import com.feedzai.commons.sql.abstraction.dml.result.H2ResultIterator;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.dml.result.ResultIterator;
import com.feedzai.commons.sql.abstraction.engine.AbstractDatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.AbstractTranslator;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineDriver;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.MappedEntity;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.handler.OperationFault;
import com.feedzai.commons.sql.abstraction.engine.handler.QueryExceptionHandler;
import com.feedzai.commons.sql.abstraction.engine.impl.h2.H2QueryExceptionHandler;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.column;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.max;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.select;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.table;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.md5;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * H2 specific database implementation.
 *
 * @author Joao Silva (joao.silva@feedzai.com)
 * @since 2.0.0
 */
@Deprecated
public class H2Engine extends AbstractDatabaseEngine {
    /**
     * The max length of a VARCHAR type.
     * For more information, see: <a href="http://www.h2database.com/html/datatypes.html">H2 database ─ data types</a>.
     */
    private static final int MAX_VARCHAR_LENGTH = 1048576;

    /**
     * The H2 JDBC driver.
     */
    protected static final String H2_DRIVER = DatabaseEngineDriver.H2.driver();

    /**
     * Name is already used by an existing object.
     */
    public static final String NAME_ALREADY_EXISTS = "42S01";

    /**
     * Name is already used by an existing index.
     */
    public static final String INDEX_ALREADY_EXISTS = "42S11";

    /**
     * Table can have only one primary key.
     */
    public static final String TABLE_CAN_ONLY_HAVE_ONE_PRIMARY_KEY = "90017";

    /**
     * Table or view does not exist.
     */
    public static final String TABLE_OR_VIEW_DOES_NOT_EXIST = "42S02";

    /**
     * Constraint name already exists.
     */
    public static final String CONSTRAINT_NAME_ALREADY_EXISTS = "90045";

    /**
     * An optional feature is not implemented by the driver or not supported by the DB.
     *
     * @since 2.1.13
     */
    public static final String OPTIONAL_FEATURE_NOT_SUPPORTED = "HYC00";

    /**
     * An instance of {@link QueryExceptionHandler} specific for H2 engine, to be used in disambiguating SQL exceptions.
     * @since 2.5.1
     */
    public static final QueryExceptionHandler H2_QUERY_EXCEPTION_HANDLER = new H2QueryExceptionHandler();

    /**
     * The pattern to detect the AUTO_SERVER flag in the JDBC URL.
     */
    private static final Pattern AUTO_SERVER_PATTERN = Pattern.compile("AUTO_SERVER=(TRUE|FALSE)", Pattern.CASE_INSENSITIVE);

    /**
     * The pattern to detect the DB_CLOSE_ON_EXIT flag in the JDBC URL.
     */
    private static final Pattern CLOSE_ON_EXIT_PATTERN = Pattern.compile("DB_CLOSE_ON_EXIT=(TRUE|FALSE)", Pattern.CASE_INSENSITIVE);

    /**
     * Creates a new H2 connection.
     *
     * @param properties The properties for the database connection.
     * @throws DatabaseEngineException When the connection fails.
     */
    public H2Engine(PdbProperties properties) throws DatabaseEngineException {
        super(H2_DRIVER, properties, Dialect.H2);
    }

    @Override
    protected String getFinalJdbcConnection(final String jdbc) {

        final Matcher autoServerMatcher = AUTO_SERVER_PATTERN.matcher(jdbc);
        Boolean autoServer = null;
        if (autoServerMatcher.find()) {
            autoServer = Boolean.parseBoolean(autoServerMatcher.group(1));
        }

        final Matcher closeOnExitMatcher = CLOSE_ON_EXIT_PATTERN.matcher(jdbc);
        Boolean closeOnExit = null;
        if (closeOnExitMatcher.find()) {
            closeOnExit = Boolean.parseBoolean(closeOnExitMatcher.group(1));
        }

        String finalJdbc = jdbc;

        /*
         Having AUTO_SERVER=TRUE and DB_CLOSE_ON_EXIT=FALSE is not allowed.
         See:
         - https://github.com/h2database/h2database/pull/3503
         - https://github.com/h2database/h2database/commit/6a0f7a973251409f428565c7544dbd272de856f3#commitcomment-76882889
         - https://thepracticaldeveloper.com/book-update-2.7.1/#feature-not-supported-auto_servertrue--db_close_on_exitfalse
         */
        if (autoServer == null && !Boolean.FALSE.equals(closeOnExit)) {
            // if AUTO_SERVER is not explicitly defined, enable it, unless DB_CLOSE_ON_EXIT is explicitly set to false
            autoServer = true;
            finalJdbc = finalJdbc.concat(";AUTO_SERVER=TRUE");
        }
        if (closeOnExit == null && !Boolean.TRUE.equals(autoServer)) {
            // if DB_CLOSE_ON_EXIT is not explicitly defined, disable it, unless AUTO_SERVER is enabled
            finalJdbc = finalJdbc.concat(";DB_CLOSE_ON_EXIT=FALSE");
        }

        return finalJdbc;
    }

    @Override
    protected void onConnectionCreated() throws DatabaseEngineException {
        if (supportsLegacyMode()) {
            try (PreparedStatement stmt = conn.prepareStatement("SET MODE LEGACY")) {
                stmt.execute();
            } catch (final SQLException ex) {
                throw new DatabaseEngineException("Error defining the legacy mode in H2", ex);
            }
        }
    }

    /**
     * Checks if the legacy mode is supported.
     * Legacy mode is currently supported only by version 2.x of H2.
     * @return true if legacy mode is supported; false otherwise.
     * @since 2.8.10
     */
    private boolean supportsLegacyMode() throws DatabaseEngineException {
        try {
            return conn.getMetaData().getDatabaseMajorVersion() == 2;
        } catch (final SQLException ex) {
            throw new DatabaseEngineException("Error accessing the database metadata", ex);
        }
    }

    @Override
    public Class<? extends AbstractTranslator> getTranslatorClass() {
        return H2Translator.class;
    }

    @Override
    protected void setPreparedStatementValue(final PreparedStatement ps,
                                             final int index,
                                             final DbColumn dbColumn,
                                             final Object value,
                                             final boolean fromBatch) throws Exception {
        switch (dbColumn.getDbColumnType()) {
            case BLOB:
                if (value == null) {
                    ps.setNull(index, Types.BLOB);
                } else if (value instanceof Serializable) {
                    ps.setBinaryStream(index, new ByteArrayInputStream(objectToArray(value)));
                } else {
                    throw new DatabaseEngineException("Cannot convert " + value.getClass().getSimpleName() + " to byte[]. BLOB columns only accept byte arrays.");
                }
                break;

            case JSON:
            case CLOB:
                if (value == null) {
                    ps.setNull(index, Types.CLOB);
                } else if (value instanceof String) {
                    StringReader sr = new StringReader((String) value);
                    ps.setCharacterStream(index, sr);
                } else {
                    throw new DatabaseEngineException("Cannot convert " + value.getClass().getSimpleName() + " to String. CLOB columns only accept Strings.");
                }
                break;

            default:
                ps.setObject(index, value);
        }
    }

    private DbEntity injectNotNullIfMissing(DbEntity entity) {
        final DbEntity.Builder builder = new DbEntity.Builder()
                .name(entity.getName())
                .addFks(entity.getFks())
                .pkFields(entity.getPkFields())
                .addIndexes(entity.getIndexes());

        final List<String> pkFields = entity.getPkFields();
        final List<DbColumn> columns = new ArrayList<>();

        for (DbColumn c : entity.getColumns()) {
            if (pkFields.contains(c.getName()) && !c.getColumnConstraints().contains(DbColumnConstraint.NOT_NULL)) {
                columns.add(new DbColumn.Builder()
                        .name(c.getName())
                        .type(c.getDbColumnType())
                        .size(c.getSize())
                        .addConstraints(c.getColumnConstraints())
                        .addConstraint(DbColumnConstraint.NOT_NULL)
                        .autoInc(c.isAutoInc())
                        .defaultValue(c.getDefaultValue())
                        .build()
                );
            } else {
                columns.add(c);
            }
        }

        return builder.addColumn(columns).build();
    }

    @Override
    protected void createTable(DbEntity entity) throws DatabaseEngineException {
        entity = injectNotNullIfMissing(entity);

        List<String> createTable = new ArrayList<>();

        createTable.add("CREATE TABLE");
        createTable.add(quotize(entity.getName()));
        List<String> columns = new ArrayList<>();
        List<String> pkFields = entity.getPkFields();
        for (DbColumn c : entity.getColumns()) {
            List<String> column = new ArrayList<>();
            column.add(quotize(c.getName()));
            column.add(translateType(c));

            // If this column is PK, it must be forced to be NOT NULL (only if it's not already...)
            if (pkFields.contains(c.getName()) && !c.getColumnConstraints().contains(DbColumnConstraint.NOT_NULL)) {
                // Create a NOT NULL constraint
                c.getColumnConstraints().add(DbColumnConstraint.NOT_NULL);
            }

            for (DbColumnConstraint cc : c.getColumnConstraints()) {
                column.add(cc.translate());
            }

            if (c.isDefaultValueSet()) {
                column.add("DEFAULT");
                column.add(translate(c.getDefaultValue()));
            }

            columns.add(join(column, " "));
        }
        createTable.add("(" + join(columns, ", ") + ")");

        final String createTableStatement = join(createTable, " ");

        logger.trace(createTableStatement);

        Statement s = null;
        try {
            s = conn.createStatement();
            s.executeUpdate(createTableStatement);
        } catch (final SQLException ex) {
            if (ex.getSQLState().startsWith(NAME_ALREADY_EXISTS)) {
                logger.debug(dev, "'{}' is already defined", entity.getName());
                handleOperation(new OperationFault(entity.getName(), OperationFault.Type.TABLE_ALREADY_EXISTS), ex);
            } else {
                throw new DatabaseEngineException("Something went wrong handling statement", ex);
            }
        } finally {
            try {
                if (s != null) {
                    s.close();
                }
            } catch (final Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }
    }

    @Override
    protected void addPrimaryKey(final DbEntity entity) throws DatabaseEngineException {
        if (entity.getPkFields().size() == 0) {
            return;
        }

        List<String> pks = new ArrayList<>();
        for (String pk : entity.getPkFields()) {
            pks.add(quotize(pk));
        }

        final String pkName = md5(format("PK_%s", entity.getName()), properties.getMaxIdentifierSize());

        List<String> statement = new ArrayList<>();
        statement.add("ALTER TABLE");
        statement.add(quotize(entity.getName()));
        statement.add("ADD CONSTRAINT");
        statement.add(quotize(pkName));
        statement.add("PRIMARY KEY");
        statement.add("(" + join(pks, ", ") + ")");

        final String addPrimaryKey = join(statement, " ");

        logger.trace(addPrimaryKey);

        Statement s = null;
        try {
            s = conn.createStatement();
            s.executeUpdate(addPrimaryKey);
        } catch (final SQLException ex) {
            if (ex.getSQLState().startsWith(TABLE_CAN_ONLY_HAVE_ONE_PRIMARY_KEY) || ex.getSQLState().startsWith(CONSTRAINT_NAME_ALREADY_EXISTS)) {
                logger.debug(dev, "'{}' already has a primary key", entity.getName());
                handleOperation(new OperationFault(entity.getName(), OperationFault.Type.PRIMARY_KEY_ALREADY_EXISTS), ex);
            } else {
                throw new DatabaseEngineException("Something went wrong handling statement", ex);
            }
        } finally {
            try {
                if (s != null) {
                    s.close();
                }
            } catch (final Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }
    }

    @Override
    protected void addIndexes(final DbEntity entity) throws DatabaseEngineException {
        List<DbIndex> indexes = entity.getIndexes();

        for (DbIndex index : indexes) {

            List<String> createIndex = new ArrayList<>();
            createIndex.add("CREATE");
            if (index.isUnique()) {
                createIndex.add("UNIQUE");
            }
            createIndex.add("INDEX");

            List<String> columns = new ArrayList<>();
            List<String> columnsForName = new ArrayList<>();
            for (String column : index.getColumns()) {
                columns.add(quotize(column));
                columnsForName.add(column);
            }
            final String idxName = md5(format("%s_%s_IDX", entity.getName(), join(columnsForName, "_")), properties.getMaxIdentifierSize());
            createIndex.add(quotize(idxName));
            createIndex.add("ON");
            createIndex.add(quotize(entity.getName()));
            createIndex.add("(" + join(columns, ", ") + ")");

            final String statement = join(createIndex, " ");

            logger.trace(statement);

            Statement s = null;
            try {
                s = conn.createStatement();
                s.executeUpdate(statement);
            } catch (final SQLException ex) {
                if (ex.getSQLState().startsWith(INDEX_ALREADY_EXISTS)) {
                    logger.debug(dev, "'{}' is already defined", idxName);
                    handleOperation(new OperationFault(entity.getName(), OperationFault.Type.INDEX_ALREADY_EXISTS), ex);
                } else {
                    throw new DatabaseEngineException("Something went wrong handling statement", ex);
                }
            } finally {
                try {
                    if (s != null) {
                        s.close();
                    }
                } catch (final Exception e) {
                    logger.trace("Error closing statement.", e);
                }
            }
        }
    }

    @Override
    protected void addSequences(DbEntity entity) {
        /*
         * Do nothing by default since we support auto incrementation using the serial types.
         */
    }

    @Override
    protected MappedEntity createPreparedStatementForInserts(final DbEntity entity) throws DatabaseEngineException {
        final List<String> insertInto = new ArrayList<>();
        insertInto.add("INSERT INTO");
        insertInto.add(quotize(entity.getName()));
        final List<String> insertIntoWithAutoInc = new ArrayList<>();
        insertIntoWithAutoInc.add("INSERT INTO");
        insertIntoWithAutoInc.add(quotize(entity.getName()));
        final List<String> columns = new ArrayList<>();
        final List<String> values = new ArrayList<>();
        final List<String> columnsWithAutoInc = new ArrayList<>();
        final List<String> valuesWithAutoInc = new ArrayList<>();
        String columnWithAutoIncName = null;
        for (final DbColumn column : entity.getColumns()) {
            columnsWithAutoInc.add(quotize(column.getName()));
            valuesWithAutoInc.add("?");
            if (column.isAutoInc()) {
                // Get the only column name with auto incremented values.
                columnWithAutoIncName = column.getName();
            } else {
                columns.add(quotize(column.getName()));
                values.add("?");
            }
        }

        insertInto.add("(" + join(columns, ", ") + ")");
        insertInto.add("VALUES (" + join(values, ", ") + ")");

        insertIntoWithAutoInc.add("(" + join(columnsWithAutoInc, ", ") + ")");
        insertIntoWithAutoInc.add("VALUES (" + join(valuesWithAutoInc, ", ") + ")");

        final String statementWithMerge = buildMergeStatement(entity, columns, values);

        final String statement = join(insertInto, " ");
        // The H2 DB doesn't implement INSERT RETURNING. Therefore, we just create a dummy statement, which will
        // never be invoked by this implementation.
        final String insertReturnStatement = "";
        final String statementWithAutoInt = join(insertIntoWithAutoInc, " ");
        logger.trace(statement);


        final PreparedStatement ps, psReturn, psWithAutoInc, psMerge;
        try {

            // Generate keys when the table has at least 1 column with auto generate value.
            final int generateKeys = columnWithAutoIncName != null? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS;
            ps = this.conn.prepareStatement(statement, generateKeys);
            psReturn = this.conn.prepareStatement(insertReturnStatement, generateKeys);
            psWithAutoInc = this.conn.prepareStatement(statementWithAutoInt, generateKeys);
            psMerge = this.conn.prepareStatement(statementWithMerge, generateKeys);
            return new MappedEntity()
                .setInsert(ps)
                .setInsertReturning(psReturn)
                .setInsertWithAutoInc(psWithAutoInc)
                // The auto incremented column must be set, so when persisting a row, it's possible to retrieve its value
                // by consulting the column name from this MappedEntity.
                .setAutoIncColumn(columnWithAutoIncName)
                .setInsertIgnoring(psMerge);
        } catch (final SQLException ex) {
            throw new DatabaseEngineException("Something went wrong handling statement", ex);
        }
    }

    /**
     * Helper method to create a merge statement for this engine.
     *
     * @param entity    The entity.
     * @param columns   The columns of this entity.
     * @param values    The values of the entity.
     *
     * @return          A merge statement.
     */
    private String buildMergeStatement(final DbEntity entity, final List<String> columns, final List<String> values) {
        final String statementWithMerge;
        if (!entity.getPkFields().isEmpty() && !columns.isEmpty() && !values.isEmpty()) {
            final List<String> mergeInto = new ArrayList<>();
            mergeInto.add("MERGE INTO");
            mergeInto.add(quotize(entity.getName()));

            mergeInto.add("(" + join(columns, ", ") + ")");
            mergeInto.add("VALUES (" + join(values, ", ") + ")");

            statementWithMerge = join(mergeInto, " ");
        } else {
            statementWithMerge = "";
        }
        return statementWithMerge;
    }

    @Override
    protected void dropSequences(DbEntity entity) {
        /*
         * H2 does not support sequences. We're using SERIAL types instead.
         */
    }

    @Override
    protected void dropTable(DbEntity entity) throws DatabaseEngineException {
        Statement drop = null;
        try {
            drop = conn.createStatement();
            final String query = format("DROP TABLE %s CASCADE", quotize(entity.getName()));
            logger.trace(query);
            drop.executeUpdate(query);
        } catch (final SQLException ex) {
            if (ex.getSQLState().startsWith(TABLE_OR_VIEW_DOES_NOT_EXIST)) {
                logger.debug(dev, "Table '{}' does not exist", entity.getName());
                handleOperation(new OperationFault(entity.getName(), OperationFault.Type.TABLE_DOES_NOT_EXIST), ex);
            } else {
                throw new DatabaseEngineException("Error dropping table", ex);
            }
        } finally {
            try {
                if (drop != null) {
                    drop.close();
                }
            } catch (final Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }
    }

    @Override
    protected void dropColumn(DbEntity entity, String... columns) throws DatabaseEngineException {
        Statement drop = null;

        try {
            drop = conn.createStatement();
            for (String column : columns) {
                final String query = format("ALTER TABLE %s DROP COLUMN %s", quotize(entity.getName()), quotize(column));
                logger.trace(query);
                drop.executeUpdate(query);
            }
        } catch (final SQLException ex) {
            if (ex.getSQLState().startsWith(TABLE_OR_VIEW_DOES_NOT_EXIST)) {
                logger.debug(dev, "Table '{}' does not exist", entity.getName());
                handleOperation(new OperationFault(entity.getName(), OperationFault.Type.COLUMN_DOES_NOT_EXIST), ex);
            } else {
                throw new DatabaseEngineException("Error dropping column", ex);
            }
        } finally {
            try {
                if (drop != null) {
                    drop.close();
                }
            } catch (final Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }

    }

    @Override
    protected void addColumn(DbEntity entity, DbColumn... columns) throws DatabaseEngineException {

        Statement s = null;
        try {
            s = conn.createStatement();
            for (DbColumn c : columns) {
                List<String> column = new ArrayList<>();
                column.add(quotize(c.getName()));
                column.add(translateType(c));

                for (DbColumnConstraint cc : c.getColumnConstraints()) {
                    column.add(cc.translate());
                }

                if (c.isDefaultValueSet()) {
                    column.add("DEFAULT");
                    column.add(translate(c.getDefaultValue()));
                }

                final String query = format("ALTER TABLE %s ADD COLUMN %s", quotize(entity.getName()), join(column, " "));
                logger.trace(query);
                s.executeUpdate(query);
            }

        } catch (final SQLException ex) {
            throw new DatabaseEngineException("Something went wrong handling statement", ex);
        } finally {
            try {
                if (s != null) {
                    s.close();
                }
            } catch (final Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }

    }

    @Override
    protected PreparedStatement getPreparedStatementForPersist(final boolean useAutoInc, final MappedEntity mappedEntity) {
        return useAutoInc ? mappedEntity.getInsert() : mappedEntity.getInsertWithAutoInc();
    }

    @Override
    protected synchronized long doPersist(final PreparedStatement ps,
                                          final MappedEntity me,
                                          final boolean useAutoInc,
                                          final int lastBindPosition) throws Exception {
        ps.execute();
        long generatedKey = 0;
        final String autoIncColumnName = me.getAutoIncColumn();

        if (autoIncColumnName != null) {
            if (useAutoInc) {
                try (final ResultSet generatedKeys = ps.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        // These generated keys belong to the primary key and might not have auto incremented values nor
                        // even have integer values.
                        // That's why it's necessary to only retrieve the value from a column that is auto incremented.
                        generatedKey = generatedKeys.getLong(autoIncColumnName);
                    }
                }
            } else {
                final List<Map<String, ResultColumn>> q = query(
                    select(max(column(me.getAutoIncColumn()))).from(table(me.getEntity().getName()))
                );
                if (!q.isEmpty()) {
                    generatedKey = q.get(0).values().iterator().next().toLong();
                }

                updatePersistAutoIncSequence(me, generatedKey);
            }
        }

        return generatedKey;
    }

    /**
     * Updates the autoInc sequence value after a persist operation.
     *
     * @param mappedEntity      The mapped entity to for which to update the autoInc sequence.
     * @param currentAutoIncVal The current value for the autoInc column.
     * @since 2.8.10
     */
    private void updatePersistAutoIncSequence(final MappedEntity mappedEntity, final long currentAutoIncVal) {
        executeUpdateSilently(format(
                "ALTER TABLE %s ALTER COLUMN %s RESTART WITH %d",
                quotize(mappedEntity.getEntity().getName()),
                quotize(mappedEntity.getAutoIncColumn()),
                currentAutoIncVal + 1
        ));
    }

    @Override
    public boolean isStringAggDistinctCapable() {
        return true;
    }

    @Override
    protected void addFks(final DbEntity entity, final Set<DbFk> fks) throws DatabaseEngineException {
        for (final DbFk fk : fks) {
            final List<String> quotizedLocalColumns = new ArrayList<>();
            for (String s : fk.getLocalColumns()) {
                quotizedLocalColumns.add(quotize(s));
            }

            final List<String> quotizedForeignColumns = new ArrayList<>();
            for (final String s : fk.getReferencedColumns()) {
                quotizedForeignColumns.add(quotize(s));
            }

            final String table = quotize(entity.getName());
            final String quotizedLocalColumnsSting = join(quotizedLocalColumns, ", ");
            final String quotizedForeignColumnsString = join(quotizedForeignColumns, ", ");

            final String alterTable =
                    format(
                            "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
                            table,
                            quotize(md5(
                                    "FK_" + table + quotizedLocalColumnsSting + quotizedForeignColumnsString,
                                    properties.getMaxIdentifierSize()
                            )),
                            quotizedLocalColumnsSting,
                            quotize(fk.getReferencedTable()),
                            quotizedForeignColumnsString
                    );

            Statement alterTableStmt = null;
            try {
                alterTableStmt = conn.createStatement();
                logger.trace(alterTable);
                alterTableStmt.executeUpdate(alterTable);
            } catch (final SQLException ex) {
                if (ex.getSQLState().equals(CONSTRAINT_NAME_ALREADY_EXISTS)) {
                    logger.debug(dev, "Foreign key for table '{}' already exists. Error code: {}.", entity.getName(), ex.getSQLState());
                } else {
                    throw new DatabaseEngineException(format(
                            "Could not add Foreign Key to entity %s. Error code: %s.",
                            entity.getName(),
                            ex.getSQLState()
                    ), ex);
                }
            } finally {
                try {
                    if (alterTableStmt != null) {
                        alterTableStmt.close();
                    }
                } catch (final Exception e) {
                    logger.trace("Error closing statement.", e);
                }
            }
        }
    }

    @Override
    public String getSchema() throws DatabaseEngineException {
        try {
            return this.conn.getSchema();

        } catch (final Exception ex) {
            if (ex instanceof SQLException && OPTIONAL_FEATURE_NOT_SUPPORTED.equals(((SQLException) ex).getSQLState())) {
                // if connection is remote, getSchema() may not be supported; try again
                try (final Statement stmt = conn.createStatement();
                     final ResultSet resultSet = stmt.executeQuery("SELECT SCHEMA()")) {

                    if (!resultSet.next()) {
                        return null;
                    }

                    return resultSet.getString(1);

                } catch (final Exception queryEx) {
                    throw new DatabaseEngineException("Could not get current schema", queryEx);
                }
            }

            throw new DatabaseEngineException("Could not get current schema", ex);
        }
    }

    @Override
    protected void setSchema(final String schema) throws DatabaseEngineException {
        try {
            this.conn.setSchema(schema);

        } catch (final Exception ex) {
            if (ex instanceof SQLException && OPTIONAL_FEATURE_NOT_SUPPORTED.equals(((SQLException) ex).getSQLState())) {
                // if connection is remote, getSchema() may not be supported; try again
                try (final Statement stmt = conn.createStatement()) {
                    stmt.execute("SET SCHEMA " + quotize(schema));
                    return;

                } catch (final Exception queryEx) {
                    throw new DatabaseEngineException(String.format("Could not set current schema to '%s'", schema), queryEx);
                }
            }

            throw new DatabaseEngineException(String.format("Could not set current schema to '%s'", schema), ex);
        }
    }

    @Override
    protected ResultIterator createResultIterator(Statement statement, String sql) throws DatabaseEngineException {
        return new H2ResultIterator(statement, sql);
    }

    @Override
    protected ResultIterator createResultIterator(PreparedStatement ps) throws DatabaseEngineException {
        return new H2ResultIterator(ps);
    }

    @Override
    protected QueryExceptionHandler getQueryExceptionHandler() {
        return H2_QUERY_EXCEPTION_HANDLER;
    }

    @Override
    protected void setParameterValues(final PreparedStatement ps, final int index, final Object param) throws SQLException {
        if (param instanceof String) {
            final String paramStr = (String) param;
            if (paramStr.length() > MAX_VARCHAR_LENGTH) {
                ps.setCharacterStream(index, new StringReader(paramStr));
            } else {
                super.setParameterValues(ps, index, param);
            }
        } else if (param instanceof byte[]) {
            ps.setBinaryStream(index, new ByteArrayInputStream((byte[]) param));
        } else {
            super.setParameterValues(ps, index, param);
        }
    }
}
