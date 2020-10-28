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
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

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
public class H2Engine extends AbstractDatabaseEngine {

    /**
     * The PostgreSQL JDBC driver.
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
     * Creates a new PostgreSql connection.
     *
     * @param properties The properties for the database connection.
     * @throws DatabaseEngineException When the connection fails.
     */
    public H2Engine(PdbProperties properties) throws DatabaseEngineException {
        super(H2_DRIVER, properties, Dialect.H2);
    }

    @Override
    protected String getFinalJdbcConnection(String jdbc) {
        if (!jdbc.contains("AUTO_SERVER")) {
            jdbc = jdbc.concat(";AUTO_SERVER=TRUE");
        }

        if (!jdbc.contains("DB_CLOSE_ON_EXIT")) {
            jdbc = jdbc.concat(";DB_CLOSE_ON_EXIT=FALSE");
        }

        return jdbc;
    }

    @Override
    public Class<? extends AbstractTranslator> getTranslatorClass() {
        return H2Translator.class;
    }

    @Override
    protected int entityToPreparedStatement(final DbEntity entity, final PreparedStatement ps, final EntityEntry entry, final boolean useAutoInc) throws DatabaseEngineException {

        int i = 1;
        for (DbColumn column : entity.getColumns()) {
            if ((column.isAutoInc() && useAutoInc)) {
                continue;
            }

            try {
                final Object val;
                if (column.isDefaultValueSet() && !entry.containsKey(column.getName())) {
                   val = column.getDefaultValue().getConstant();
                } else {
                    val = entry.get(column.getName());
                }

                switch (column.getDbColumnType()) {
                    case BLOB:
                        ps.setBytes(i, objectToArray(val));

                        break;
                    case JSON:
                    case CLOB:
                        if (val == null) {
                            ps.setNull(i, Types.CLOB);
                            break;
                        }

                        if (val instanceof String) {
                            StringReader sr = new StringReader((String) val);
                            ps.setCharacterStream(i, sr);
                        } else {
                            throw new DatabaseEngineException("Cannot convert " + val.getClass().getSimpleName() + " to String. CLOB columns only accept Strings.");
                        }
                        break;
                    default:
                        ps.setObject(i, val);
                }
            } catch (final Exception ex) {
                throw new DatabaseEngineException("Error while mapping variables to database", ex);
            }

            i++;
        }

        return i - 1;
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
    protected void addSequences(DbEntity entity) throws DatabaseEngineException {
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


        final String statement = join(insertInto, " ");
        // The H2 DB doesn't implement INSERT RETURNING. Therefore, we just create a dummy statement, which will
        // never be invoked by this implementation.
        final String insertReturnStatement = "";
        final String statementWithAutoInt = join(insertIntoWithAutoInc, " ");
        logger.trace(statement);


        final PreparedStatement ps, psReturn, psWithAutoInc;
        try {

            // Generate keys when the table has at least 1 column with auto generate value.
            final int generateKeys = columnWithAutoIncName != null? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS;
            ps = this.conn.prepareStatement(statement, generateKeys);
            psReturn = this.conn.prepareStatement(insertReturnStatement, generateKeys);
            psWithAutoInc = this.conn.prepareStatement(statementWithAutoInt, generateKeys);

            return new MappedEntity()
                .setInsert(ps)
                .setInsertReturning(psReturn)
                .setInsertWithAutoInc(psWithAutoInc)
                // The auto incremented column must be set, so when persisting a row, its possible to retrieve its value
                // by consulting the column name from this MappedEntity.
                .setAutoIncColumn(columnWithAutoIncName);
        } catch (final SQLException ex) {
            throw new DatabaseEngineException("Something went wrong handling statement", ex);
        }
    }

    @Override
    protected void dropSequences(DbEntity entity) throws DatabaseEngineException {
        /*
         * Remember that we not support sequences in PostgreSql. We're using SERIAL types instead.
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

        final String autoIncColumnName = me.getAutoIncColumn();
        if (useAutoInc && autoIncColumnName != null) {
            try (final ResultSet generatedKeys = ps.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    // These generated keys belong to the primary key and might not have auto incremented values nor
                    // even have integer values.
                    // That's why its necessary to only retrieve the value from a column that is auto incremented.
                    return generatedKeys.getLong(autoIncColumnName);
                }
            }
        }

        return 0;
    }

    @Override
    public boolean isStringAggDistinctCapable() {
        return true;
    }

    @Override
    protected void addFks(DbEntity entity) throws DatabaseEngineException {
        for (DbFk fk : entity.getFks()) {
            final List<String> quotizedLocalColumns = new ArrayList<>();
            for (String s : fk.getLocalColumns()) {
                quotizedLocalColumns.add(quotize(s));
            }

            final List<String> quotizedForeignColumns = new ArrayList<>();
            for (String s : fk.getForeignColumns()) {
                quotizedForeignColumns.add(quotize(s));
            }

            final String table = quotize(entity.getName());
            final String quotizedLocalColumnsSting = join(quotizedLocalColumns, ", ");
            final String quotizedForeignColumnsString = join(quotizedForeignColumns, ", ");

            final String alterTable =
                    format(
                            "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
                            table,
                            quotize(md5("FK_" + table + quotizedLocalColumnsSting + quotizedForeignColumnsString, properties.getMaxIdentifierSize())),
                            quotizedLocalColumnsSting,
                            quotize(fk.getForeignTable()),
                            quotizedForeignColumnsString);

            Statement alterTableStmt = null;
            try {
                alterTableStmt = conn.createStatement();
                logger.trace(alterTable);
                alterTableStmt.executeUpdate(alterTable);
            } catch (final SQLException ex) {
                if (ex.getSQLState().equals(CONSTRAINT_NAME_ALREADY_EXISTS)) {
                    logger.debug(dev, "Foreign key for table '{}' already exists. Error code: {}.", entity.getName(), ex.getSQLState());
                } else {
                    throw new DatabaseEngineException(format("Could not add Foreign Key to entity %s. Error code: %s.", entity.getName(), ex.getSQLState()), ex);
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
    protected String getSchema() throws DatabaseEngineException {
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
}
