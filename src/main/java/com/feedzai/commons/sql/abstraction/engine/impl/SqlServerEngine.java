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
import com.feedzai.commons.sql.abstraction.dml.Expression;
import com.feedzai.commons.sql.abstraction.dml.View;
import com.feedzai.commons.sql.abstraction.dml.dialect.Dialect;
import com.feedzai.commons.sql.abstraction.dml.result.ResultIterator;
import com.feedzai.commons.sql.abstraction.dml.result.SqlServerResultIterator;
import com.feedzai.commons.sql.abstraction.engine.AbstractDatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.AbstractTranslator;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineDriver;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.MappedEntity;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.handler.OperationFault;
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
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.feedzai.commons.sql.abstraction.util.StringUtils.md5;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * SQLServer specific database implementation.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class SqlServerEngine extends AbstractDatabaseEngine {

    /**
     * The SQLServer JDBC driver.
     */
    protected static final String SQLSERVER_DRIVER = DatabaseEngineDriver.SQLSERVER.driver();

    /**
     * Name is already used by an existing object.
     */
    public static final int NAME_ALREADY_EXISTS = 2714;

    /**
     * Table can have only one primary key.
     */
    public static final int TABLE_CAN_ONLY_HAVE_ONE_PRIMARY_KEY = 1779;

    /**
     * Sequence does not exist.
     */
    public static final int INDEX_ALREADY_EXISTS = 1913;

    /**
     * Table or view does not exist.
     */
    public static final int TABLE_OR_VIEW_DOES_NOT_EXIST = 3701;

    /**
     * The maximum size for the VARBINARY type.
     */
    public static final int MAXIMUM_VARBINARY_SIZE = 8000;

    /**
     * Creates a new SQL Server connection.
     *
     * @param properties The properties for the database connection.
     * @throws DatabaseEngineException When the connection fails.
     */
    public SqlServerEngine(PdbProperties properties) throws DatabaseEngineException {
        super(SQLSERVER_DRIVER, properties, Dialect.SQLSERVER);
    }

    @Override
    protected Properties getDBProperties() {
        final Properties props = super.getDBProperties();

        /*
         Driver supports DriverManager.setLoginTimeout but this login timeout only applies after the socket connection;
         if there is a problem connecting (e.g. if the DB server is down) the connection process may be blocked for a
         long time, so an "initial" socket timeout is set here for the connection/login, and after the connection it is
         set to the value specified by the SOCKET_TIMEOUT Pdb property
         */
        props.setProperty("socketTimeout", Long.toString(TimeUnit.SECONDS.toMillis(this.properties.getLoginTimeout())));

        return props;
    }

    @Override
    protected int entityToPreparedStatement(final DbEntity entity, final PreparedStatement ps, final EntityEntry entry, boolean useAutoInc) throws DatabaseEngineException {

        int i = 1;
        for (DbColumn column : entity.getColumns()) {
            if (column.isAutoInc() && useAutoInc) {
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
                            ps.setClob(i, sr);
                        } else {
                            throw new DatabaseEngineException("Cannot convert " + val.getClass().getSimpleName() + " to String. CLOB columns only accept Strings.");
                        }
                        break;
                    case BOOLEAN:
                        Boolean b = (Boolean) val;
                        if (b == null) {
                            ps.setObject(i, null);
                        } else if (b) {
                            ps.setObject(i, 1);
                        } else {
                            ps.setObject(i, 0);
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

    @Override
    protected void createTable(final DbEntity entity) throws DatabaseEngineException {

        List<String> createTable = new ArrayList<>();

        createTable.add("CREATE TABLE");
        createTable.add(quotize(entity.getName()));
        List<String> columns = new ArrayList<>();

        int numberOfAutoIncs = 0;
        for (DbColumn c : entity.getColumns()) {
            List<String> column = new ArrayList<>();
            column.add(quotize(c.getName()));

            column.add(translateType(c));

            if (c.isAutoInc()) {
                column.add("IDENTITY");
                numberOfAutoIncs++;
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

        if (numberOfAutoIncs > 1) {
            throw new DatabaseEngineException("In SQLServer you can only define one auto increment column");
        }

        createTable.add("(" + join(columns, ", ") + ")");

        final String createTableStatement = join(createTable, " ");

        logger.trace(createTableStatement);

        Statement s = null;
        try {
            s = conn.createStatement();
            s.executeUpdate(createTableStatement);
        } catch (final SQLException ex) {
            if (ex.getErrorCode() == NAME_ALREADY_EXISTS) {
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
            DbColumn toAlter = null;
            for (DbColumn col : entity.getColumns()) {
                if (col.getName().equals(pk)) {
                    toAlter = col;
                    break;
                }
            }

            if (toAlter == null) {
                throw new DatabaseEngineException("The column you specified for Primary Key does not exist.");
            } else {
                boolean isNotNull = false;
                List<String> cons = new ArrayList<>();
                cons.add(quotize(toAlter.getName()));
                cons.add(translateType(toAlter));
                for (DbColumnConstraint cc : toAlter.getColumnConstraints()) {
                    if (cc == DbColumnConstraint.NOT_NULL) {
                        isNotNull = true;
                    }

                    cons.add(cc.translate());
                }

                if (!isNotNull) {
                    cons.add(DbColumnConstraint.NOT_NULL.translate());

                    final String alterTable = format("ALTER TABLE %s ALTER COLUMN %s", quotize(entity.getName()), join(cons, " "));
                    logger.trace(alterTable);
                    Statement s = null;
                    try {
                        s = conn.createStatement();
                        s.executeUpdate(alterTable);
                    } catch (final SQLException ex) {
                        throw new DatabaseEngineException("Something went wrong altering the table. This shouldn't have happened", ex);
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
            if (ex.getErrorCode() == TABLE_CAN_ONLY_HAVE_ONE_PRIMARY_KEY) {
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
                if (ex.getErrorCode() == INDEX_ALREADY_EXISTS) {
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
         * SQL Server uses IDENTITY to perform sequencing.
         */
    }

    @Override
    protected MappedEntity createPreparedStatementForInserts(final DbEntity entity) throws DatabaseEngineException {

        List<String> insertInto = new ArrayList<>();
        insertInto.add("INSERT INTO");
        insertInto.add(quotize(entity.getName()));
        List<String> insertIntoWithAutoInc = new ArrayList<>();
        insertIntoWithAutoInc.add("INSERT INTO");
        insertIntoWithAutoInc.add(quotize(entity.getName()));
        List<String> columns = new ArrayList<>();
        List<String> values = new ArrayList<>();
        List<String> columnsWithAutoInc = new ArrayList<>();
        List<String> valuesWithAutoInc = new ArrayList<>();
        for (DbColumn column : entity.getColumns()) {
            columnsWithAutoInc.add(quotize(column.getName()));
            valuesWithAutoInc.add("?");
            if (!column.isAutoInc()) {
                columns.add(quotize(column.getName()));
                values.add("?");
            }
        }

        insertInto.add("(" + join(columns, ", ") + ")");
        insertInto.add("VALUES (" + join(values, ", ") + ")");

        insertIntoWithAutoInc.add("(" + join(columnsWithAutoInc, ", ") + ")");
        insertIntoWithAutoInc.add("VALUES (" + join(valuesWithAutoInc, ", ") + ")");

        final String statement = join(insertInto, " ");
        final String statementWithAutoInt = join(insertIntoWithAutoInc, " ");

        logger.trace(statement);

        PreparedStatement ps, psReturn, psWithAutoInc;
        try {
            ps = conn.prepareStatement(statement);
            psReturn = conn.prepareStatement(statement, Statement.RETURN_GENERATED_KEYS);
            psWithAutoInc = conn.prepareStatement(statementWithAutoInt);

            return new MappedEntity().setInsert(ps).setInsertReturning(psReturn).setInsertWithAutoInc(psWithAutoInc);
        } catch (final SQLException ex) {
            throw new DatabaseEngineException("Something went wrong handling statement", ex);
        }
    }

    @Override
    protected void dropSequences(DbEntity entity) throws DatabaseEngineException {
        /*
         * SQL Server uses IDENTITY for sequencing.
         */
    }

    @Override
    protected void dropTable(DbEntity entity) throws DatabaseEngineException {

        dropReferringFks(entity);

        Statement drop = null;
        try {
            drop = conn.createStatement();
            final String query = format("DROP TABLE %s", quotize(entity.getName()));
            logger.trace(query);
            drop.executeUpdate(query);
        } catch (final SQLException ex) {
            if (ex.getErrorCode() == TABLE_OR_VIEW_DOES_NOT_EXIST) {
                logger.debug("Table '{}' does not exist", entity.getName());
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

        List<String> removeColumns = new ArrayList<>();
        removeColumns.add("ALTER TABLE");
        removeColumns.add(quotize(entity.getName()));
        removeColumns.add("DROP COLUMN");
        List<String> cols = new ArrayList<>();
        for (String col : columns) {
            cols.add(quotize(col));
        }
        removeColumns.add(join(cols, ","));

        try {
            drop = conn.createStatement();
            final String query = join(removeColumns, " ");
            logger.trace(query);
            drop.executeUpdate(query);
        } catch (final SQLException ex) {
            if (ex.getErrorCode() == TABLE_OR_VIEW_DOES_NOT_EXIST) {
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
        List<String> addColumns = new ArrayList<>();
        addColumns.add("ALTER TABLE");
        addColumns.add(quotize(entity.getName()));
        addColumns.add("ADD");
        List<String> cols = new ArrayList<>();
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

            cols.add(join(column, " "));
        }
        addColumns.add(join(cols, ","));
        final String addColumnsStatement = join(addColumns, " ");

        logger.trace(addColumnsStatement);

        Statement s = null;
        try {
            s = conn.createStatement();
            s.executeUpdate(addColumnsStatement);
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
    protected String translateType(final DbColumn c) throws DatabaseEngineException {
        return translator.translate(c);
    }

    @Override
    public Class<? extends AbstractTranslator> getTranslatorClass() {
        return SqlServerTranslator.class;
    }

    @Override
    public synchronized Long persist(final String name, final EntityEntry entry) throws DatabaseEngineException {
        return persist(name, entry, true);
    }

    @Override
    public synchronized Long persist(String name, EntityEntry entry, boolean useAutoInc) throws DatabaseEngineException {
        ResultSet generatedKeys = null;
        MappedEntity me = null;
        try {
            getConnection();
            me = entities.get(name);

            if (me == null) {
                throw new DatabaseEngineException(String.format("Unknown entity '%s'", name));
            }

            final PreparedStatement ps;
            if (useAutoInc) {
                ps = entities.get(name).getInsertReturning();
            } else {
                ps = entities.get(name).getInsertWithAutoInc();
            }

            entityToPreparedStatement(me.getEntity(), ps, entry, useAutoInc);

            if (!useAutoInc) {
                // Only SET IDENTITY_INSERT for tables that have an identity column
                if (hasIdentityColumn(me.getEntity())) {
                    executeUpdateSilently("SET IDENTITY_INSERT \"" + name + "\" ON");
                }
                ps.execute();
            } else {
                ps.execute();
            }

            long ret = 0;
            if (useAutoInc) {
                generatedKeys = ps.getGeneratedKeys();

                if (generatedKeys.next()) {
                    ret = generatedKeys.getLong(1);
                }
            }

            return ret == 0 ? null : ret;
        } catch (final Exception ex) {
            throw new DatabaseEngineException("Something went wrong persisting the entity", ex);
        } finally {
            try {
                if (generatedKeys != null) {
                    generatedKeys.close();
                }
                if (!useAutoInc) {
                    // Only SET IDENTITY_INSERT for tables that have an identity column
                    if (me != null && hasIdentityColumn(me.getEntity())) {
                        getConnection().createStatement().execute("SET IDENTITY_INSERT \"" + name + "\" OFF");
                    }
                }
            } catch (final Exception e) {
                logger.trace("Error closing result set.", e);
            }
        }
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
                if (ex.getErrorCode() == NAME_ALREADY_EXISTS) {
                    logger.debug(dev, "Foreign key for table '{}' already exists. Error code: {}.", entity.getName(), ex.getErrorCode());
                    handleOperation(new OperationFault(entity.getName(), OperationFault.Type.FOREIGN_KEY_ALREADY_EXISTS), ex);
                } else {
                    throw new DatabaseEngineException(format("Could not add Foreign Key to entity %s. Error code: %d.", entity.getName(), ex.getErrorCode()), ex);
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

    protected void dropReferringFks(DbEntity entity) throws DatabaseEngineException {
        Statement s = null;
        ResultSet dependentTables = null;
        try {
            /*
             * List of constraints that won't let the table be dropped.
             */
            s = conn.createStatement();
            final String sString = format(
                    "SELECT T1.TABLE_NAME, CONSTRAINT_NAME "
                            + "FROM INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE AS T1 "
                            + "JOIN SYS.FOREIGN_KEYS AS F "
                            + "ON (F.parent_object_id = OBJECT_ID(N'%s') OR "
                            + "F.referenced_object_id = OBJECT_ID(N'%s')) AND "
                            + "T1.CONSTRAINT_NAME = OBJECT_NAME(F.object_id) AND "
                            + "T1.TABLE_NAME NOT LIKE '%s'", entity.getName(), entity.getName(), entity.getName());

            logger.trace(sString);
            s.executeQuery(sString);

            dependentTables = s.getResultSet();

            while (dependentTables.next()) {

                Statement dropFk = null;
                try {
                    dropFk = conn.createStatement();
                    final String dropFkString = format(
                            "ALTER TABLE %s DROP CONSTRAINT %s",
                            quotize(dependentTables.getString(1)),
                            quotize(dependentTables.getString(2)));
                    logger.trace(dropFkString);
                    dropFk.executeUpdate(dropFkString);

                } catch (final SQLException ex) {
                    logger.debug(format("Unable to drop constraint '%s' in table '%s'", dependentTables.getString(2), dependentTables.getString(1)), ex);
                } finally {
                    if (dropFk != null) {
                        try {
                            dropFk.close();
                        } catch (final Exception e) {
                            logger.trace("Error closing statement.", e);
                        }
                    }
                }
            }
        } catch (final SQLException ex) {
            throw new DatabaseEngineException(format("Unable to drop foreign keys of the tables that depend on '%s'", entity.getName()), ex);
        } finally {
            if (dependentTables != null) {
                try {
                    dependentTables.close();
                } catch (final Exception e) {
                    logger.trace("Error closing result set.", e);
                }
            }
            if (s != null) {
                try {
                    s.close();
                } catch (final Exception e) {
                    logger.trace("Error closing statement.", e);
                }
            }
        }
    }

    @Override
    public synchronized int executeUpdate(final Expression query) throws DatabaseEngineException {
        /*
         * SQL Server does not support CREATE OR REPLACE VIEW so we have to manually drop it first.
         */
        if (query instanceof View && ((View) query).isReplace()) {
            final View v = (View) query;
            final String dropIfExists = String.format("IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = '%s') "
                    + "DROP VIEW %s", v.getName(), quotize(v.getName()));

            logger.trace(dropIfExists);
            executeUpdate(dropIfExists);

        }

        return super.executeUpdate(query);
    }

    @Override
    public boolean isStringAggDistinctCapable() {
        return false;
    }

    @Override
    protected boolean checkConnection(final Connection conn) {
        Statement s = null;
        try {
            s = conn.createStatement();
            s.executeQuery("select 1");

            return true;
        } catch (final SQLException e) {
            logger.debug("Connection is down.", e);
            return false;
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
    protected void setSchema(final String schema) throws DatabaseEngineException {
        if (!properties.getSchema().equals(getSchema())) {
            throw new DatabaseEngineException(
                "Microsoft Sql Server doesn't support setting a schema for the session! Use a different user or database instead."
            );
        }
    }

    @Override
    protected ResultIterator createResultIterator(Statement statement, String sql) throws DatabaseEngineException {
        return new SqlServerResultIterator(statement, sql);
    }

    @Override
    protected ResultIterator createResultIterator(PreparedStatement ps) throws DatabaseEngineException {
        return new SqlServerResultIterator(ps);
    }
}
