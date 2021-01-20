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
import com.feedzai.commons.sql.abstraction.ddl.DbColumnType;
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.ddl.DbFk;
import com.feedzai.commons.sql.abstraction.ddl.DbIndex;
import com.feedzai.commons.sql.abstraction.dml.dialect.Dialect;
import com.feedzai.commons.sql.abstraction.dml.result.DB2ResultIterator;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.dml.result.ResultIterator;
import com.feedzai.commons.sql.abstraction.engine.AbstractDatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.AbstractTranslator;
import com.feedzai.commons.sql.abstraction.engine.ConnectionResetException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineDriver;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.feedzai.commons.sql.abstraction.engine.MappedEntity;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.handler.OperationFault;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.feedzai.commons.sql.abstraction.util.Constants;
import com.feedzai.commons.sql.abstraction.util.PreparedStatementCapsule;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.feedzai.commons.sql.abstraction.util.StringUtils.md5;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * DB2 specific database implementation.
 *
 * @author Marco Jorge (marco.jorge@feedzai.com)
 * @since 2.0.0
 */
public class DB2Engine extends AbstractDatabaseEngine {

    /**
     * The DB2 JDBC driver.
     */
    protected static final String DB2_DRIVER = DatabaseEngineDriver.DB2.driver();
    /**
     * Name is already used by an existing object.
     */
    public static final String NAME_ALREADY_EXISTS = "DB2 SQL Error: SQLCODE=-601, SQLSTATE=42710";
    /**
     * Table can have only one primary key.
     */
    public static final String TABLE_CAN_ONLY_HAVE_ONE_PRIMARY_KEY = "DB2 SQL Error: SQLCODE=-624, SQLSTATE=42889";
    /**
     * Sequence does not exist.
     */
    public static final String SEQUENCE_DOES_NOT_EXIST = "DB2 SQL Error: SQLCODE=-204, SQLSTATE=42704";
    /**
     * Table or view does not exist.
     */
    public static final String TABLE_OR_VIEW_DOES_NOT_EXIST = "DB2 SQL Error: SQLCODE=-204, SQLSTATE=42704";
    /**
     * Foreign key already exists
     */
    public static final String FOREIGN_ALREADY_EXISTS = "DB2 SQL Error: SQLCODE=-601, SQLSTATE=42710";

    /**
     * The default size of a BLOB in DB2.
     */
    public static final String DB2_DEFAULT_BLOB_SIZE = "2G";

    /**
     * Creates a new DB2 connection.
     *
     * @param properties The properties for the database connection.
     * @throws DatabaseEngineException When the connection fails.
     */
    public DB2Engine(PdbProperties properties) throws DatabaseEngineException {
        super(DB2_DRIVER, properties, Dialect.DB2);
    }

    @Override
    public Class<? extends AbstractTranslator> getTranslatorClass() {
        return DB2Translator.class;
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
                    /*
                     * CLOB, BLOB and and JSON are handled the same way in DB2 since neither CLOB nor JSON are supported.
                     */
                    case JSON:
                    case CLOB:
                    case BLOB:
                        ps.setBytes(i, objectToArray(val));

                        break;
                    case BOOLEAN:
                        Boolean b = (Boolean) val;
                        if (b == null) {
                            ps.setObject(i, null);
                        } else if (b) {
                            ps.setObject(i, "1");
                        } else {
                            ps.setObject(i, "0");
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
        for (DbColumn c : entity.getColumns()) {
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
            if (ex.getMessage().startsWith(NAME_ALREADY_EXISTS)) {
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


        String alterColumnSetNotNull = alterColumnSetNotNull(entity.getName(), entity.getPkFields());

        final String pkName = md5(format("PK_%s", entity.getName()), properties.getMaxIdentifierSize());

        List<String> statement = new ArrayList<>();
        statement.add("ALTER TABLE");
        statement.add(quotize(entity.getName()));
        statement.add("ADD CONSTRAINT");
        statement.add(quotize(pkName));
        statement.add("PRIMARY KEY");
        statement.add("(" + join(pks, ", ") + ")");

        final String addPrimaryKey = join(statement, " ");
        String reorg = reorg(entity.getName());


        Statement s = null;
        try {
            logger.trace(alterColumnSetNotNull);
            s = conn.createStatement();
            s.executeUpdate(alterColumnSetNotNull);
            s.close();

            logger.trace(reorg);
            s = conn.createStatement();
            s.executeUpdate(reorg);
            s.close();

            logger.trace(addPrimaryKey);
            s = conn.createStatement();
            s.executeUpdate(addPrimaryKey);
            s.close();

            logger.trace(reorg);
            s = conn.createStatement();
            s.executeUpdate(reorg);
        } catch (final SQLException ex) {
            if (ex.getMessage().startsWith(TABLE_CAN_ONLY_HAVE_ONE_PRIMARY_KEY)) {
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

    /**
     * Reorganizes the table so it doesn't contain fragments.
     *
     * @param tableName The table name to reorganize.
     * @return The command to perform the operation.
     */
    private String reorg(String tableName) {
        List<String> statement = new ArrayList<>();
        statement.add("CALL sysproc.admin_cmd('REORG TABLE");
        statement.add(quotize(tableName));
        statement.add("')");

        return join(statement, " ");
    }

    /**
     * Generates a command to set the specified columns to enforce non nullability.
     *
     * @param tableName   The table name.
     * @param columnNames The columns.
     * @return The command to perform the operation.
     */
    private String alterColumnSetNotNull(String tableName, List<String> columnNames) {
        List<String> statement = new ArrayList<>();
        statement.add("ALTER TABLE");
        statement.add(quotize(tableName));

        for (String columnName : columnNames) {
            statement.add("ALTER COLUMN");
            statement.add(quotize(columnName));
            statement.add("SET NOT NULL");
        }

        return join(statement, " ");
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
                if (ex.getMessage().startsWith(NAME_ALREADY_EXISTS)) {
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
        for (DbColumn column : entity.getColumns()) {
            if (!column.isAutoInc()) {
                continue;
            }

            final String sequenceName = md5(format("%s_%s_SEQ", entity.getName(), column.getName()), properties.getMaxIdentifierSize());

            List<String> createSequence = new ArrayList<>();
            createSequence.add("CREATE SEQUENCE");
            createSequence.add(quotize(sequenceName));
            createSequence.add("MINVALUE 0");
            switch (column.getDbColumnType()) {
                case INT:
                    createSequence.add("MAXVALUE");
                    createSequence.add(format("%d", Integer.MAX_VALUE));

                    break;
                case LONG:
                    createSequence.add("NO MAXVALUE");

                    break;
                default:
                    throw new DatabaseEngineException("Auto incrementation is only supported on INT and LONG");
            }
            createSequence.add("START WITH 1");
            createSequence.add("INCREMENT BY 1");

            String statement = join(createSequence, " ");

            logger.trace(statement);

            Statement s = null;
            try {
                s = conn.createStatement();
                s.executeUpdate(statement);
            } catch (final SQLException ex) {
                if (ex.getMessage().startsWith(NAME_ALREADY_EXISTS)) {
                    logger.debug(dev, "'{}' is already defined", sequenceName);
                    handleOperation(new OperationFault(entity.getName(), OperationFault.Type.SEQUENCE_ALREADY_EXISTS), ex);
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

    /*
     * This is a small hack to support submitting several DML statements under the same call.
     * It seems that for some reason using JDBC DB2 cannot execute more than one DML operation
     * under the same JDBC statement.
     */
    @Override
    public synchronized int executeUpdate(String query) throws DatabaseEngineException {
        String[] split = query.split(Constants.UNIT_SEPARATOR_CHARACTER + "");
        int i = -1;
        for (String s : split) {
            if (StringUtils.isNotBlank(s)) {
                i = super.executeUpdate(s);
            }
        }

        return i;
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
        String returning = null;
        for (DbColumn column : entity.getColumns()) {
            columnsWithAutoInc.add(quotize(column.getName()));
            valuesWithAutoInc.add("?");

            columns.add(quotize(column.getName()));
            if (column.isAutoInc()) {
                final String sequenceName = md5(format("%s_%s_SEQ", entity.getName(), column.getName()), properties.getMaxIdentifierSize());
                values.add(format("%s.nextval", quotize(sequenceName)));
                returning = column.getName();
            } else {
                values.add("?");
            }
        }

        insertInto.add("(" + join(columns, ", ") + ")");
        insertInto.add("VALUES (" + join(values, ", ") + ")");

        insertIntoWithAutoInc.add("(" + join(columnsWithAutoInc, ", ") + ")");
        insertIntoWithAutoInc.add("VALUES (" + join(valuesWithAutoInc, ", ") + ")");

        List<String> insertIntoReturn = new ArrayList<>(insertInto);

        final String insertStatement = join(insertInto, " ");
        final String insertReturnStatement = join(insertIntoReturn, " ");
        final String insertWithAutoInc = join(insertIntoWithAutoInc, " ");

        logger.trace(insertStatement);
        logger.trace(insertReturnStatement);

        PreparedStatement ps, psReturn, psWithAutoInc;
        try {

            ps = conn.prepareStatement(insertStatement);
            psReturn = conn.prepareStatement(insertReturnStatement);
            psWithAutoInc = conn.prepareStatement(insertWithAutoInc);

            return new MappedEntity().setInsert(ps).setInsertReturning(psReturn).setInsertWithAutoInc(psWithAutoInc).setAutoIncColumn(returning);
        } catch (final SQLException ex) {
            throw new DatabaseEngineException("Something went wrong handling statement", ex);
        }
    }

    @Override
    protected void dropSequences(DbEntity entity) throws DatabaseEngineException {
        for (DbColumn column : entity.getColumns()) {
            if (!column.isAutoInc()) {
                continue;
            }

            final String sequenceName = md5(format("%s_%s_SEQ", entity.getName(), column.getName()), properties.getMaxIdentifierSize());

            final String stmt = format("DROP SEQUENCE %s", quotize(sequenceName));

            Statement drop = null;
            try {
                drop = conn.createStatement();
                logger.trace(stmt);
                drop.executeUpdate(stmt);
            } catch (final SQLException ex) {
                if (ex.getMessage().startsWith(SEQUENCE_DOES_NOT_EXIST)) {
                    logger.debug(dev, "Sequence '{}' does not exist", sequenceName);
                    handleOperation(new OperationFault(entity.getName(), OperationFault.Type.SEQUENCE_DOES_NOT_EXIST), ex);
                } else {
                    throw new DatabaseEngineException("Error dropping sequence", ex);
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
    }

    @Override
    protected void dropTable(DbEntity entity) throws DatabaseEngineException {
        Statement drop = null;
        try {
            drop = conn.createStatement();
            final String query = format("DROP TABLE %s", quotize(entity.getName()));
            logger.trace(query);
            drop.executeUpdate(query);
        } catch (final SQLException ex) {
            if (ex.getMessage().startsWith(TABLE_OR_VIEW_DOES_NOT_EXIST)) {
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
        Statement reorgStatement = null;

        List<String> removeColumns = new ArrayList<>();
        removeColumns.add("ALTER TABLE");
        removeColumns.add(quotize(entity.getName()));

        for (String col : columns) {
            removeColumns.add("DROP COLUMN");
            removeColumns.add(quotize(col));
        }

        try {
            drop = conn.createStatement();
            final String query = join(removeColumns, " ");
            logger.trace(query);
            drop.executeUpdate(query);

            String reorg = reorg(entity.getName());
            logger.trace(reorg);
            reorgStatement = conn.createStatement();
            reorgStatement.executeUpdate(reorg);
        } catch (final SQLException ex) {
            if (ex.getMessage().startsWith(TABLE_OR_VIEW_DOES_NOT_EXIST)) {
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
            try {
                if (reorgStatement != null) {
                    reorgStatement.close();
                }
            } catch (final Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }

    }

    /*
     * This method is overwritten because every time an update is made in DB2 the table must be re-organized.
     */
    @Override
    public synchronized void updateEntity(DbEntity entity) throws DatabaseEngineException {
        super.updateEntity(entity);
        try (Statement reorg = conn.createStatement()) {
            reorg.executeUpdate(reorg(entity.getName()));
        } catch (final SQLException e) {
            throw new DatabaseEngineException("Error reorganizing table '" + entity.getName() + "'", e);
        }
    }

    @Override
    protected void addColumn(DbEntity entity, DbColumn... columns) throws DatabaseEngineException {
        List<String> addColumns = new ArrayList<>();
        addColumns.add("ALTER TABLE");
        addColumns.add(quotize(entity.getName(), translator.translateEscape()));

        for (DbColumn c : columns) {
            addColumns.add("ADD COLUMN");
            List<String> column = new ArrayList<>();
            column.add(quotize(c.getName(), translator.translateEscape()));
            column.add(translateType(c));

            for (DbColumnConstraint cc : c.getColumnConstraints()) {
                column.add(cc.translate());
            }

            if (c.isDefaultValueSet()) {
                column.add("DEFAULT");
                column.add(translate(c.getDefaultValue()));
            }

            addColumns.add(join(column, " "));
        }

        final String addColumnsStatement = join(addColumns, " ");
        logger.trace(addColumnsStatement);

        Statement s = null;
        Statement reorgStatement = null;
        try {
            s = conn.createStatement();
            s.executeUpdate(addColumnsStatement);

            String reorg = reorg(entity.getName());
            logger.trace(reorg);

            reorgStatement = conn.createStatement();
            reorgStatement.executeUpdate(reorg);

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
            try {
                if (reorgStatement != null) {
                    reorgStatement.close();
                }
            } catch (final Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }

    }

    @Override
    protected synchronized long doPersist(final PreparedStatement ps,
                                          final MappedEntity me,
                                          final boolean useAutoInc,
                                          int lastBindPosition) throws Exception {
        ps.execute();

        if (me.getAutoIncColumn() == null) {
            return 0;
        }

        // the entity has autoinc columns: retrieve the sequence number or adjust it

        final String name = me.getEntity().getName();
        final String quotizedSeqName = quotize(
                md5(format("%s_%s_SEQ", name, me.getAutoIncColumn()), properties.getMaxIdentifierSize())
        );

        long ret = 0;

        if (useAutoInc) {
            final List<Map<String, ResultColumn>> q = query(format("SELECT PREVIOUS VALUE FOR %s FROM sysibm.sysdummy1", quotizedSeqName));
            if (!q.isEmpty()) {
                for (final ResultColumn rc : q.get(0).values()) {
                    return rc.toLong();
                }
            }

        } else {
            final String sql = "select (select max(\"" + me.getAutoIncColumn() + "\") from \"" + name + "\") , " + quotizedSeqName + ".NEXTVAL FROM sysibm.sysdummy1";
            final List<Map<String, ResultColumn>> q = query(sql);

            if (!q.isEmpty()) {
                final Iterator<ResultColumn> it = q.get(0).values().iterator();
                long max = Optional.ofNullable(it.next().toLong()).orElse(-1L);
                long seqCurVal = Optional.ofNullable(it.next().toLong()).orElse(-1L);

                if (seqCurVal != max) {
                    //table and sequence are not synchronized, readjust sequence max+1 (next val will return max+1)
                    executeUpdateSilently("ALTER SEQUENCE " + quotizedSeqName + " RESTART WITH " + (ret + 1));
                }
            }


            final List<Map<String, ResultColumn>> keys = query(sql);
            if (!keys.isEmpty()) {
                final Iterator<ResultColumn> it = keys.get(0).values().iterator();
                ret = it.next().toLong();
                long seqCurVal = it.next().toLong();
                if (seqCurVal != ret) {
                    // table and sequence are not synchronized, readjust sequence max+1 (next val will return max+1)
                    executeUpdateSilently("ALTER SEQUENCE " + quotizedSeqName + " RESTART WITH " + (ret + 1));
                }
            }
        }

        return ret;
    }

    @Override
    protected void addFks(DbEntity entity) throws DatabaseEngineException {
        for (DbFk fk : entity.getFks()) {
            final List<String> quotizedLocalColumns = new ArrayList<>();
            for (String s : fk.getLocalColumns()) {
                quotizedLocalColumns.add(quotize(s));
            }

            final List<String> quotizedForeignColumns = new ArrayList<>();
            for (final String s : fk.getReferencedColumns()) {
                quotizedForeignColumns.add(quotize(s));
            }

            final String table = quotize(entity.getName(), translator.translateEscape());
            final String quotizedLocalColumnsSting = join(quotizedLocalColumns, ", ");
            final String quotizedForeignColumnsString = join(quotizedForeignColumns, ", ");

            final String alterTable =
                    format(
                            "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
                            table,
                            quotize(md5("FK_" + table + quotizedLocalColumnsSting + quotizedForeignColumnsString, properties.getMaxIdentifierSize())),
                            quotizedLocalColumnsSting,
                            quotize(fk.getReferencedTable()),
                            quotizedForeignColumnsString);

            Statement alterTableStmt = null;
            Statement reorgStatement = null;
            try {
                alterTableStmt = conn.createStatement();
                logger.trace(alterTable);
                alterTableStmt.executeUpdate(alterTable);

                String reorg = reorg(entity.getName());
                logger.trace(reorg);
                reorgStatement = conn.createStatement();
                reorgStatement.executeUpdate(reorg);
            } catch (final SQLException ex) {
                if (ex.getMessage().startsWith(FOREIGN_ALREADY_EXISTS)) {
                    logger.debug(dev, "Foreign key for table '{}' already exists. Error code: {}.", entity.getName(), ex.getMessage());
                    handleOperation(new OperationFault(entity.getName(), OperationFault.Type.FOREIGN_KEY_ALREADY_EXISTS), ex);
                } else {
                    throw new DatabaseEngineException(format("Could not add Foreign Key to entity %s. Error code: %s.", entity.getName(), ex.getMessage()), ex);
                }
            } finally {
                try {
                    if (alterTableStmt != null) {
                        alterTableStmt.close();
                    }
                } catch (final Exception e) {
                    logger.trace("Error closing statement.", e);
                }
                try {
                    if (reorgStatement != null) {
                        reorgStatement.close();
                    }
                } catch (final Exception e) {
                    logger.trace("Error closing statement.", e);
                }
            }
        }
    }

    @Override
    protected boolean checkConnection(final Connection conn) {
        final int timeout = this.properties.getCheckConnectionTimeout();
        final int socketTimeout;
        try {
            socketTimeout = conn.getNetworkTimeout();
            try {
                // Set the socket timeout to verify the connection.
                conn.setNetworkTimeout(socketTimeoutExecutor, timeout * 1000);
                return pingConnection(conn);
            } catch (final Exception ex) {
                logger.debug("It wasn't possible to verify the connection state within the timeout of {} seconds.",
                        timeout,
                        ex);
                return false;
            } finally {
                // Make sure to respect it afterwards.
                conn.setNetworkTimeout(socketTimeoutExecutor, socketTimeout);
            }
        } catch (final Exception ex) {
            logger.warn("It wasn't possible to reset the connection / fetch the timeout.");

            try {
                conn.close();
            } catch (final Exception e) {
                logger.debug("Error closing the connection.", e);
            }

            return false;
        }
    }

    /**
     * Executes a dummy query to verify if the connection is alive.
     *
     * @param conn The connection to test.
     * @return {@code true} if the connection is valid, {@code false} otherwise.
     */
    private boolean pingConnection(final Connection conn) {
        Statement s = null;
        try {
            s = conn.createStatement();
            s.executeQuery("SELECT 1 FROM sysibm.sysdummy1");

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
    protected ResultIterator createResultIterator(Statement statement, String sql) throws DatabaseEngineException {
        return new DB2ResultIterator(statement, sql);
    }

    @Override
    protected String getSchema() throws DatabaseEngineException {
        try (final Statement stmt = conn.createStatement();
             final ResultSet resultSet = stmt.executeQuery("VALUES(CURRENT SCHEMA)")) {

            if (!resultSet.next()) {
                return null;
            }

            final String schema = resultSet.getString(1);
            // result needs to be trimmed because it has padding spaces
            return schema == null ? null : schema.trim();
        } catch (final Exception e) {
            throw new DatabaseEngineException("Could not get current schema", e);
        }
    }

    @Override
    protected void setSchema(final String schema) throws DatabaseEngineException {
        final boolean schemaExists;

        try (final PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM syscat.schemata WHERE SCHEMANAME = ?")) {
            ps.setString(1, schema);

            try (final ResultSet resultSet = ps.executeQuery()) {
                schemaExists = resultSet.next() && resultSet.getInt(1) == 1;
            }

        } catch (final Exception e) {
            throw new DatabaseEngineException(String.format("Could not set current schema to '%s'", schema), e);
        }

        if (!schemaExists) {
            throw new DatabaseEngineException(String.format("Could not set current schema to non existing '%s'", schema));
        }

        try (final PreparedStatement ps = conn.prepareStatement("SET CURRENT SCHEMA ?")) {
            ps.setString(1, schema);
            ps.execute();
        } catch (final Exception e) {
            throw new DatabaseEngineException(String.format("Could not set current schema to '%s'", schema), e);
        }

    }

    @Override
    public synchronized Map<String, DbColumnType> getMetadata(final String schemaPattern,
                                                              final String tableNamePattern) throws DatabaseEngineException {
        final Map<String, DbColumnType> metaMap = new LinkedHashMap<>();

        PreparedStatement ps = null;
        ResultSet rsColumns = null;
        try {
            getConnection();

            ps = conn.prepareStatement(
                    "SELECT NAME, COLTYPE, SCALE FROM sysibm.SYSCOLUMNS WHERE tbname LIKE ? AND TBCREATOR LIKE ?");
            ps.setString(1, tableNamePattern == null ? "%" : tableNamePattern);
            ps.setString(2, schemaPattern == null ? "%" : schemaPattern);

            rsColumns = ps.executeQuery();

            while (rsColumns.next()) {
                final String columnType = rsColumns.getString("COLTYPE").trim();

                int scale = 0;
                try {
                    scale = Integer.parseInt(rsColumns.getString("SCALE"));
                } catch (final NumberFormatException e) {
                    /* swallow - scale is already 0*/
                }

                metaMap.put(rsColumns.getString("NAME"), toPdbType(scale == 0 ? columnType : (columnType + scale)));
            }

            return metaMap;
        } catch (final Exception e) {
            throw new DatabaseEngineException("Could not get metadata", e);
        } finally {
            try {
                if (rsColumns != null) {
                    rsColumns.close();
                }
            } catch (final Exception a) {
                logger.trace("Error closing result set.", a);
            }

            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (final Exception a) {
                logger.trace("Error closing statement.", a);
            }
        }
    }

    private DbColumnType toPdbType(String type) {
        if (type.equals("INTEGER")) {
            return DbColumnType.INT;
        }

        if (type.equals("CHAR")) {
            return DbColumnType.BOOLEAN;
        }

        if (type.equals("DECIMAL")) {
            return DbColumnType.LONG;
        }

        if (type.equals("DOUBLE")) {
            return DbColumnType.DOUBLE;
        }

        if (type.equals("NUMBER19")) {
            return DbColumnType.LONG;
        }

        if (type.equals("VARCHAR2") || type.equals("VARCHAR")) {
            return DbColumnType.STRING;
        }

        if (type.equals("CLOB")) {
            return DbColumnType.BLOB;
        }

        if (type.equals("BLOB")) {
            return DbColumnType.BLOB;
        }

        return DbColumnType.UNMAPPED;
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
                    setObjectParameter(ps, i, o);
                }
            } catch (final Exception ex) {
                if (checkConnection(conn) || !properties.isReconnectOnLost()) {
                    throw new DatabaseEngineException("Could not set parameters", ex);
                }

                // At this point maybe it is an error with the connection, so we try to re-establish it.
                reconnectExceptionally("Connection is down");

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
                setObjectParameter(ps, index, param);
            }
        } catch (final Exception ex) {
            if (checkConnection(conn) || !properties.isReconnectOnLost()) {
                throw new DatabaseEngineException("Could not set parameter", ex);
            }

            // At this point maybe it is an error with the connection, so we try to re-establish it.
            reconnectExceptionally("Connection is down");

            throw new ConnectionResetException("Connection was lost, you must reset the prepared statement parameters and re-execute the statement");
        }
    }

    @Override
    public boolean isStringAggDistinctCapable() {
        // The current version of DB2 supported by PDB, does not allow for the use of distinct.
        return false;
    }

    /**
     * DB2 does not support CLOB. The strategy here is to try and write the object. If the object is a {@link String} DB2 will not allow
     * and then we encapsulate the String in a {@link byte[]}. If it fails and the value is not a string then throw the error since it is
     * not related with this issue.
     *
     * @param ps    The {@link PreparedStatementCapsule} that contains all the context.
     * @param index The index where to insert the value.
     * @param o     The object to insert.
     * @throws Exception If something occurs setting the object or the allocated memory is not enough to make the conversion.
     */
    private void setObjectParameter(PreparedStatementCapsule ps, int index, Object o) throws Exception {
        try {
            ps.ps.setObject(index, o);
        } catch (final SQLException e) {
            if (!(o instanceof String)) {
                throw e;
            }

            ps.ps.setBytes(index, objectToArray(((String) o).getBytes()));
        }
    }

    @Override
    protected ResultIterator createResultIterator(PreparedStatement ps) throws DatabaseEngineException {
        return new DB2ResultIterator(ps);
    }
}
