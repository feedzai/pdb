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
import com.feedzai.commons.sql.abstraction.dml.result.MySqlResultIterator;
import com.feedzai.commons.sql.abstraction.dml.result.ResultIterator;
import com.feedzai.commons.sql.abstraction.engine.AbstractDatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.AbstractTranslator;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineDriver;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.MappedEntity;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.handler.OperationFault;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import org.apache.commons.lang.StringUtils;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.feedzai.commons.sql.abstraction.util.StringUtils.md5;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static java.lang.String.format;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.apache.commons.lang.StringUtils.join;

/**
 * MySQL specific database implementation.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class MySqlEngine extends AbstractDatabaseEngine {

    /**
     * The MySQL JDBC driver.
     */
    protected static final String MYSQL_DRIVER = DatabaseEngineDriver.MYSQL.driver();
    /**
     * Table name is already used by an existing object.
     */
    public static final int TABLE_NAME_ALREADY_EXISTS = 1050;
    /**
     * Duplicate key name
     */
    public static final int DUPLICATE_KEY_NAME = 1061;
    /**
     * Table can have only one primary key.
     */
    public static final int TABLE_CAN_ONLY_HAVE_ONE_PRIMARY_KEY = 1068;
    /**
     * Table or view does not exist.
     */
    public static final int TABLE_DOES_NOT_EXIST = 1051;
    /**
     * Foreign Key already exists.
     */
    public static final List<Integer> CONSTRAINT_NAME_ALREADY_EXISTS = Arrays.asList(1005, 1022);

    /**
     * Creates a new MySQL connection.
     *
     * @param properties The properties for the database connection.
     * @throws DatabaseEngineException When the connection fails.
     */
    public MySqlEngine(PdbProperties properties) throws DatabaseEngineException {
        super(MYSQL_DRIVER, properties, Dialect.MYSQL);
    }

    /**
     * Sets the session to ANSI mode.
     *
     * @throws SQLException If something goes wrong contacting the database.
     */
    private void setAnsiMode() throws SQLException {
        Statement s = conn.createStatement();
        s.executeUpdate("SET sql_mode = 'ansi'");
        s.close();
    }

    @Override
    protected int entityToPreparedStatement(final DbEntity entity, final PreparedStatement ps, final EntityEntry entry, final boolean useAutoInc) throws DatabaseEngineException {

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
            } catch (Exception ex) {
                throw new DatabaseEngineException("Error while mapping variable s to database", ex);
            }

            i++;
        }

        return i - 1;
    }

    @Override
    protected void createTable(final DbEntity entity) throws DatabaseEngineException {

        List<String> createTable = new ArrayList<>();

        createTable.add("CREATE TABLE");
        createTable.add(quotize(entity.getName(), escapeCharacter()));
        List<String> columns = new ArrayList<>();
        String autoIncName = "";
        // Remember that MySQL only supports one!
        int numberOfAutoIncs = 0;
        for (DbColumn c : entity.getColumns()) {
            List<String> column = new ArrayList<>();
            column.add(quotize(c.getName(), escapeCharacter()));

            column.add(translateType(c));

            /*
             * In MySQL only one column can be auto incremented and it must
             * be set as primary key.
             */
            if (c.isAutoInc()) {
                autoIncName = c.getName();
                column.add("AUTO_INCREMENT");
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
            throw new DatabaseEngineException("In MySQL you can only define one auto increment column");
        }

        String pks = "";
        if (numberOfAutoIncs == 1) {
            if (entity.getPkFields().size() == 0) {
                pks = ", PRIMARY KEY(" + autoIncName + ")";
            } else {

                pks = ", PRIMARY KEY(" + join(entity.getPkFields(), ", ") + ")";
            }
        }

        createTable.add("(" + join(columns, ", ") + pks + ")");

        final String createTableStatement = join(createTable, " ");
        logger.trace(createTableStatement);

        Statement s = null;
        try {
            s = conn.createStatement();
            s.executeUpdate(createTableStatement);
        } catch (SQLException ex) {
            if (ex.getErrorCode() == TABLE_NAME_ALREADY_EXISTS) {
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
            } catch (Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }
    }

    @Override
    protected void addPrimaryKey(final DbEntity entity) throws DatabaseEngineException {
        if (entity.getPkFields().size() == 0) {
            return;
        }

        for (DbColumn column : entity.getColumns()) {
            if (column.isAutoInc()) {
                logger.debug(dev, "There's already a primary key since you set '{}' to AUTO INCREMENT", column.getName());
                return;
            }
        }

        List<String> pks = new ArrayList<>();
        for (String pk : entity.getPkFields()) {
            pks.add(quotize(pk, escapeCharacter()));
        }

        final String pkName = md5(format("PK_%s", entity.getName()), properties.getMaxIdentifierSize());

        List<String> statement = new ArrayList<>();
        statement.add("ALTER TABLE");
        statement.add(quotize(entity.getName(), escapeCharacter()));
        statement.add("ADD CONSTRAINT");
        statement.add(quotize(pkName, escapeCharacter()));
        statement.add("PRIMARY KEY");
        statement.add("(" + join(pks, ", ") + ")");

        final String addPrimaryKey = join(statement, " ");

        logger.trace(addPrimaryKey);

        Statement s = null;
        try {
            s = conn.createStatement();
            s.executeUpdate(addPrimaryKey);
        } catch (SQLException ex) {
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
            } catch (Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }
    }

    @Override
    protected void addIndexes(final DbEntity entity) throws DatabaseEngineException {
        if (entity.getIndexes().isEmpty()) {
            return;
        }

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
                columns.add(quotize(column, escapeCharacter()));
                columnsForName.add(column);
            }
            final String idxName = md5(format("%s_%s_IDX", entity.getName(), join(columnsForName, "_")), properties.getMaxIdentifierSize());
            createIndex.add(quotize(idxName, escapeCharacter()));
            createIndex.add("ON");
            createIndex.add(quotize(entity.getName(), escapeCharacter()));
            createIndex.add("(" + join(columns, ", ") + ")");

            final String statement = join(createIndex, " ");

            logger.trace(statement);

            Statement s = null;
            try {
                s = conn.createStatement();
                s.executeUpdate(statement);
            } catch (SQLException ex) {
                if (ex.getErrorCode() == DUPLICATE_KEY_NAME) {
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
                } catch (Exception e) {
                    logger.trace("Error closing statement.", e);
                }
            }
        }
    }

    @Override
    protected void addSequences(DbEntity entity) throws DatabaseEngineException {
        /*
         * Do nothing by default since we support
         * auto incrementation using the serial types.
         */
    }

    @Override
    protected MappedEntity createPreparedStatementForInserts(final DbEntity entity) throws DatabaseEngineException {

        List<String> insertInto = new ArrayList<>();
        insertInto.add("INSERT INTO");
        insertInto.add(quotize(entity.getName(), escapeCharacter()));
        List<String> insertIntoWithAutoInc = new ArrayList<>();
        insertIntoWithAutoInc.add("INSERT INTO");
        insertIntoWithAutoInc.add(quotize(entity.getName(), escapeCharacter()));
        List<String> columns = new ArrayList<>();
        List<String> values = new ArrayList<>();
        List<String> columnsWithAutoInc = new ArrayList<>();
        List<String> valuesWithAutoInc = new ArrayList<>();
        for (DbColumn column : entity.getColumns()) {
            columnsWithAutoInc.add(quotize(column.getName(), escapeCharacter()));
            valuesWithAutoInc.add("?");
            if (!column.isAutoInc()) {
                columns.add(quotize(column.getName(), escapeCharacter()));
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
        logger.trace(statementWithAutoInt);

        PreparedStatement ps, psWithAutoInc;
        try {
            ps = conn.prepareStatement(statement, Statement.RETURN_GENERATED_KEYS);
            psWithAutoInc = conn.prepareStatement(statementWithAutoInt);

            return new MappedEntity().setInsert(ps).setInsertWithAutoInc(psWithAutoInc);
        } catch (SQLException ex) {
            throw new DatabaseEngineException("Something went wrong handling statement", ex);
        }
    }

    @Override
    protected void dropSequences(DbEntity entity) throws DatabaseEngineException {
        /*
         * Remember that we not support sequences in MySQL.
         * We're using AUTO_INC types.
         */
    }

    @Override
    protected void dropTable(DbEntity entity) throws DatabaseEngineException {
        dropReferringFks(entity);

        Statement drop = null;
        try {
            drop = conn.createStatement();
            final String query = format("DROP TABLE %s", quotize(entity.getName(), escapeCharacter()));
            logger.trace(query);
            drop.executeUpdate(query);
        } catch (SQLException ex) {
            if (ex.getErrorCode() == TABLE_DOES_NOT_EXIST) {
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
            } catch (Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }
    }

    @Override
    protected void dropColumn(DbEntity entity, String... columns) throws DatabaseEngineException {
        Statement drop = null;

        List<String> removeColumns = new ArrayList<>();
        removeColumns.add("ALTER TABLE");
        removeColumns.add(quotize(entity.getName(), escapeCharacter()));
        List<String> cols = new ArrayList<>();
        for (String col : columns) {
            cols.add("DROP COLUMN " + quotize(col, escapeCharacter()));
        }
        removeColumns.add(join(cols, ","));

        try {
            drop = conn.createStatement();
            final String query = join(removeColumns, " ");
            logger.trace(query);
            drop.executeUpdate(query);
        } catch (SQLException ex) {
            if (ex.getErrorCode() == TABLE_DOES_NOT_EXIST) {
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
            } catch (Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }

    }

    @Override
    protected void addColumn(DbEntity entity, DbColumn... columns) throws DatabaseEngineException {
        List<String> addColumns = new ArrayList<>();
        addColumns.add("ALTER TABLE");
        addColumns.add(quotize(entity.getName(), escapeCharacter()));
        List<String> cols = new ArrayList<>();
        for (DbColumn c : columns) {
            List<String> column = new ArrayList<>();
            column.add("ADD COLUMN");
            column.add(quotize(c.getName(), escapeCharacter()));
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
        } catch (SQLException ex) {
            throw new DatabaseEngineException("Something went wrong handling statement", ex);
        } finally {
            try {
                if (s != null) {
                    s.close();
                }
            } catch (Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }

    }

    @Override
    protected String translateType(DbColumn c) throws DatabaseEngineException {
        return translator.translate(c);
    }

    @Override
    public Class<? extends AbstractTranslator> getTranslatorClass() {
        return MySqlTranslator.class;
    }

    @Override
    public synchronized Long persist(final String name, final EntityEntry entry) throws DatabaseEngineException {
        return persist(name, entry, true);
    }

    @Override
    public synchronized Long persist(String name, EntityEntry entry, boolean useAutoInc) throws DatabaseEngineException {
        ResultSet generatedKeys = null;
        try {
            getConnection();

            final MappedEntity me = entities.get(name);

            if (me == null) {
                throw new DatabaseEngineException(String.format("Unknown entity '%s'", name));
            }

            final PreparedStatement ps;
            if (useAutoInc) {
                ps = entities.get(name).getInsert();
            } else {
                ps = entities.get(name).getInsertWithAutoInc();
            }

            entityToPreparedStatement(me.getEntity(), ps, entry, useAutoInc);

            ps.execute();

            long ret = 0;
            if (useAutoInc) {
                generatedKeys = ps.getGeneratedKeys();

                if (generatedKeys.next()) {
                    ret = generatedKeys.getLong(1);
                }

                generatedKeys.close();
            }

            return ret == 0 ? null : ret;
        } catch (Exception ex) {
            throw new DatabaseEngineException("Something went wrong persisting the entity", ex);
        } finally {
            try {
                if (generatedKeys != null) {
                    generatedKeys.close();
                }
            } catch (Exception e) {
                logger.trace("Error closing result set.", e);
            }
        }
    }

    @Override
    protected void addFks(DbEntity entity) throws DatabaseEngineException {
        for (DbFk fk : entity.getFks()) {
            final List<String> quotizedLocalColumns = new ArrayList<>();
            for (String s : fk.getLocalColumns()) {
                quotizedLocalColumns.add(quotize(s, escapeCharacter()));
            }

            final List<String> quotizedForeignColumns = new ArrayList<>();
            for (String s : fk.getForeignColumns()) {
                quotizedForeignColumns.add(quotize(s, escapeCharacter()));
            }

            final String table = quotize(entity.getName(), escapeCharacter());
            final String quotizedLocalColumnsSting = join(quotizedLocalColumns, ", ");
            final String quotizedForeignColumnsString = join(quotizedForeignColumns, ", ");

            final String alterTable =
                    format(
                            "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
                            table,
                            quotize(md5("FK_" + table + quotizedLocalColumnsSting + quotizedForeignColumnsString, properties.getMaxIdentifierSize()), escapeCharacter()),
                            quotizedLocalColumnsSting,
                            quotize(fk.getForeignTable(), escapeCharacter()),
                            quotizedForeignColumnsString);

            Statement alterTableStmt = null;
            try {
                alterTableStmt = conn.createStatement();
                logger.trace(alterTable);
                alterTableStmt.executeUpdate(alterTable);
                alterTableStmt.close();
            } catch (SQLException ex) {
                if (CONSTRAINT_NAME_ALREADY_EXISTS.contains(ex.getErrorCode())) {
                    logger.debug(dev, "Foreign key for table '{}' already exists. Error code: {}.", entity.getName(), ex.getErrorCode());
                } else {
                    throw new DatabaseEngineException(format("Could not add Foreign Key to entity %s. Error code: %d.", entity.getName(), ex.getErrorCode()), ex);
                }
            } finally {
                try {
                    if (alterTableStmt != null) {
                        alterTableStmt.close();
                    }
                } catch (Exception e) {
                    logger.trace("Error closing statement.", e);
                }
            }

        }
    }

    @Override
    protected void dropFks(final String table) throws DatabaseEngineException {
        String schema = StringUtils.stripToNull(properties.getSchema());
        ResultSet rs = null;
        try {
            getConnection();
            rs = conn.getMetaData().getImportedKeys(null, schema, table);
            Set<String> fks = new HashSet<>();
            while (rs.next()) {
                fks.add(rs.getString("FK_NAME"));
            }
            for (String fk : fks) {
                try {
                    executeUpdate(String.format("alter table %s drop foreign key %s", quotize(table, escapeCharacter()), quotize(fk, escapeCharacter())));
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

    protected void dropReferringFks(DbEntity entity) throws DatabaseEngineException {
        Statement s = null;
        ResultSet dependentTables = null;
        try {
            /*
             * List of constraints that won't let the table be dropped.
             */
            s = conn.createStatement();
            final String sString = format(
                    "SELECT TABLE_NAME, CONSTRAINT_NAME "
                            + "FROM information_schema.KEY_COLUMN_USAGE "
                            + "WHERE REFERENCED_TABLE_NAME = '%s'", entity.getName()
            );

            logger.trace(sString);
            s.executeQuery(sString);

            dependentTables = s.getResultSet();

            while (dependentTables.next()) {

                Statement dropFk = null;
                try {
                    dropFk = conn.createStatement();
                    final String dropFkString = format(
                            "ALTER TABLE %s DROP FOREIGN KEY %s",
                            quotize(dependentTables.getString(1), escapeCharacter()),
                            quotize(dependentTables.getString(2), escapeCharacter()));
                    logger.trace(dropFkString);
                    dropFk.executeUpdate(dropFkString);

                } catch (SQLException ex) {
                    logger.debug(format("Unable to drop constraint '%s' in table '%s'", dependentTables.getString(2), dependentTables.getString(1)), ex);
                } finally {
                    if (dropFk != null) {
                        try {
                            dropFk.close();
                        } catch (Exception e) {
                            logger.trace("Error closing statement.", e);
                        }
                    }
                }
            }


        } catch (SQLException ex) {
            throw new DatabaseEngineException(format("Unable to drop foreign keys of the tables that depend on '%s'", entity.getName()), ex);
        } finally {
            if (dependentTables != null) {
                try {
                    dependentTables.close();
                } catch (Throwable e) {
                }
            }
            if (s != null) {
                try {
                    s.close();
                } catch (Throwable e) {
                }
            }
        }
    }

    @Override
    protected boolean checkConnection(final Connection conn) {
        Statement s = null;
        try {
            s = conn.createStatement();
            s.executeQuery("select 1");

            return true;
        } catch (SQLException e) {
            logger.debug("Connection is down.", e);
            return false;
        } finally {
            try {
                if (s != null) {
                    s.close();
                }
            } catch (Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }
    }

    @Override
    public synchronized ResultIterator iterator(String query) throws DatabaseEngineException {
        try {
            getConnection();
            final Statement stmt = conn.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
            stmt.setFetchSize(properties.getFetchSize());

            return createResultIterator(stmt, query);

        } catch (final Exception e) {
            throw new DatabaseEngineException("Error querying", e);
        }
    }

    @Override
    public String commentCharacter() {
        return "#";
    }

    @Override
    public String escapeCharacter() {
        return translator.translateEscape();
    }

    @Override
    protected ResultIterator createResultIterator(Statement statement, String sql) throws DatabaseEngineException {
        return new MySqlResultIterator(statement, sql);
    }

    @Override
    protected ResultIterator createResultIterator(PreparedStatement ps) throws DatabaseEngineException {
        return new MySqlResultIterator(ps);
    }
}
