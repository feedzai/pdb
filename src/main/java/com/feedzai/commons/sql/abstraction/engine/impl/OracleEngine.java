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

import com.feedzai.commons.sql.abstraction.ddl.*;
import com.feedzai.commons.sql.abstraction.dml.dialect.Dialect;
import com.feedzai.commons.sql.abstraction.dml.result.OracleResultIterator;
import com.feedzai.commons.sql.abstraction.dml.result.ResultIterator;
import com.feedzai.commons.sql.abstraction.engine.*;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.handler.OperationFault;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.feedzai.commons.sql.abstraction.util.PreparedStatementCapsule;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleTypes;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.feedzai.commons.sql.abstraction.util.StringUtils.md5;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.join;

/**
 * Oracle specific database implementation.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class OracleEngine extends AbstractDatabaseEngine {

    /**
     * The Oracle JDBC driver.
     */
    protected static final String ORACLE_DRIVER = DatabaseEngineDriver.ORACLE.driver();
    /**
     * Name is already used by an existing object.
     */
    public static final String NAME_ALREADY_EXISTS = "ORA-00955";
    /**
     * Table can have only one primary key.
     */
    public static final String TABLE_CAN_ONLY_HAVE_ONE_PRIMARY_KEY = "ORA-02260";
    /**
     * Sequence does not exist.
     */
    public static final String SEQUENCE_DOES_NOT_EXIST = "ORA-02289";
    /**
     * Table or view does not exist.
     */
    public static final String TABLE_OR_VIEW_DOES_NOT_EXIST = "ORA-00942";
    /**
     * Foreign key already exists
     */
    public static final String FOREIGN_ALREADY_EXISTS = "ORA-02275";
    /**
     *  Double instance for 0.0, so no Double instance is created whenever 0.0 is necessary.
     *
     *  @since 2.1.4
     */
    private static Double ZERO = Double.valueOf(0.0);

    /**
     * Creates a new Oracle connection.
     *
     * @param properties The properties for the database connection.
     * @throws DatabaseEngineException When the connection fails.
     */
    public OracleEngine(PdbProperties properties) throws DatabaseEngineException {
        super(ORACLE_DRIVER, properties, Dialect.ORACLE);
    }

    @Override
    public synchronized void setParameters(final String name, final Object... params) throws DatabaseEngineException, ConnectionResetException {
        for(int i = 0 ; i < params.length ; i++) {
            params[i] = ensureNoUnderflow(params[i]);
        }
        super.setParameters(name, params);
    }

    @Override
    public synchronized void setParameter(final String name, final int index, final Object param) throws DatabaseEngineException, ConnectionResetException {
        super.setParameter(name, index, ensureNoUnderflow(param));
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
                            ((OraclePreparedStatement) ps).setStringForClob(i, (String) val);
                        } else {
                            throw new DatabaseEngineException("Cannot convert " + val.getClass().getSimpleName() + " to String. CLOB columns only accept Strings.");
                        }
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
                    case DOUBLE:
                        setParameterValues(ps, i, val);
                        break;
                    default:
                        ps.setObject(i, ensureNoUnderflow(val));
                }
            } catch (Exception ex) {
                throw new DatabaseEngineException("Error while mapping variables to database", ex);
            }

            i++;
        }

        return i - 1;
    }

    @Override
    protected void setParameterValues(final PreparedStatement ps, final int index, final Object param) throws SQLException {
        if (param instanceof byte[]) {
            ps.setBytes(index, (byte[]) param);
        } else {
            if (!(param instanceof Double)) {
                /**
                 * If it is not a double it may be:
                 *  - A String with the special values such as infinity or NaN
                 *  - A String with some invalid value
                 *  - An integer/long
                 *  - Some other invalid type
                 *
                 *  In these cases we will invoke the setObject.
                 */
                ps.setObject(index, param);
            } else {
                /**
                 * Only Double typed values will execute here.
                 * They may be:
                 *  - Regular double number
                 *  - Double.NaN
                 *  - Double.{POSITIVE|NEGATIVE}_INFINITY
                 *
                 *  In these cases we use the setBinaryDouble specific to the oracle prepared statement.
                 */
                final OraclePreparedStatement orclPs = (OraclePreparedStatement) ps;
                orclPs.setBinaryDouble(index, (Double) ensureNoUnderflow(param));
            }
        }
    }

    /**
     * Returns 0.0 if the provided value is a double and less than 1.0e-131, since values
     * under 1.0e-131 causes an underflow error in the jdbc driver.
     *
     * @param val   The value to check
     * @return      The value corrected in case val is less than 1.0e-131, unchanged otherwise.
     *
     * @since       2.1.4
     */
    private Object ensureNoUnderflow(Object val) {
        if (val instanceof  Double) {
            Double dblVal = (Double)val;
            if (Math.abs(dblVal) <= 1.0e-131) {
                val = ZERO;
            }
        }
        return val;
    }

    @Override
    public void connect() throws Exception {
        super.connect();
        if (properties.getSchema() != null && properties.getSchema().length() > 0 &&
                        !properties.getUsername().equals(properties.getSchema())) {
            // Set connection schema if one is defined and not the same as the user name
            Statement stmt = conn.createStatement();
            stmt.execute("ALTER SESSION SET CURRENT_SCHEMA=" + properties.getSchema());
            stmt.close();
        }
    }

    /**
     * Overrides {@link com.feedzai.commons.sql.abstraction.engine.AbstractDatabaseEngine#setTransactionIsolation()} This is because
     * Oracle does not support READ_UNCOMMITTED e REPEATABLE_READ.
     *
     * @throws SQLException If a database access error occurs.
     */
    @Override
    protected void setTransactionIsolation() throws SQLException {
        int isolation = properties.getIsolationLevel();

        if (isolation == Connection.TRANSACTION_READ_UNCOMMITTED) {
            isolation = Connection.TRANSACTION_READ_COMMITTED;
        }

        if (isolation == Connection.TRANSACTION_REPEATABLE_READ) {
            isolation = Connection.TRANSACTION_SERIALIZABLE;
        }

        conn.setTransactionIsolation(isolation);
    }

    @Override
    public Class<? extends AbstractTranslator> getTranslatorClass() {
        return OracleTranslator.class;
    }

    @Override
    protected void createTable(final DbEntity entity) throws DatabaseEngineException {

        List<String> createTable = new ArrayList<String>();

        createTable.add("CREATE TABLE");
        createTable.add(quotize(entity.getName()));
        List<String> columns = new ArrayList<String>();
        for (DbColumn c : entity.getColumns()) {
            List<String> column = new ArrayList<String>();
            column.add(quotize(c.getName()));
            column.add(translateType(c));

            if (c.isDefaultValueSet() && !c.getDbColumnType().equals(DbColumnType.BOOLEAN)) {
                column.add("DEFAULT");
                column.add(translate(c.getDefaultValue()));
            }

            for (DbColumnConstraint cc : c.getColumnConstraints()) {
                column.add(cc.translate());
            }
            columns.add(join(column, " "));
        }
        createTable.add("(" + join(columns, ", ") + ")");
        // segment creation deferred can cause unexpected behaviour in sequences
        // see: http://dirknachbar.blogspot.pt/2011/01/deferred-segment-creation-under-oracle.html
        createTable.add("SEGMENT CREATION IMMEDIATE");

        final String createTableStatement = join(createTable, " ");

        logger.trace(createTableStatement);

        Statement s = null;
        try {
            s = conn.createStatement();
            s.executeUpdate(createTableStatement);
        } catch (SQLException ex) {
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

        List<String> pks = new ArrayList<String>();
        for (String pk : entity.getPkFields()) {
            pks.add(quotize(pk));
        }

        final String pkName = md5(format("PK_%s", entity.getName()), properties.getMaxIdentifierSize());

        List<String> statement = new ArrayList<String>();
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
        } catch (SQLException ex) {
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
            } catch (Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }
    }

    @Override
    protected void addIndexes(final DbEntity entity, final List<DbIndex> indexes) throws DatabaseEngineException {
        for (DbIndex index : indexes) {
            List<String> createIndex = new ArrayList<String>();
            createIndex.add("CREATE");
            if (index.isUnique()) {
                createIndex.add("UNIQUE");
            }
            createIndex.add("INDEX");

            List<String> columns = new ArrayList<String>();
            List<String> columnsForName = new ArrayList<String>();
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
            } catch (SQLException ex) {
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
                } catch (Exception e) {
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

            List<String> createSequence = new ArrayList<String>();
            createSequence.add("CREATE SEQUENCE ");
            createSequence.add(quotize(sequenceName));
            createSequence.add("MINVALUE 0");
            createSequence.add("MAXVALUE");
            switch (column.getDbColumnType()) {
                case INT:
                    createSequence.add(format("%d", Integer.MAX_VALUE));

                    break;
                case LONG:
                    createSequence.add(format("%d", Long.MAX_VALUE));

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
            } catch (SQLException ex) {
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
                } catch (Exception e) {
                    logger.trace("Error closing statement.", e);
                }
            }
        }
    }

    @Override
    protected MappedEntity createPreparedStatementForInserts(final DbEntity entity) throws DatabaseEngineException {
        List<String> insertInto = new ArrayList<String>();
        insertInto.add("INSERT INTO");
        insertInto.add(quotize(entity.getName()));
        List<String> insertIntoWithAutoInc = new ArrayList<String>();
        insertIntoWithAutoInc.add("INSERT INTO");
        insertIntoWithAutoInc.add(quotize(entity.getName()));
        List<String> columns = new ArrayList<String>();
        List<String> values = new ArrayList<String>();
        List<String> columnsWithAutoInc = new ArrayList<String>();
        List<String> valuesWithAutoInc = new ArrayList<String>();
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

        List<String> insertIntoReturn = new ArrayList<String>(insertInto);

        if (returning != null) {
            insertIntoReturn.add(format("RETURNING %s INTO ?", quotize(returning)));
        } else {
            insertIntoReturn.add("RETURNING 0 INTO ?");
        }

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
        } catch (SQLException ex) {
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
            } catch (SQLException ex) {
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
                } catch (Exception e) {
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
            final String query = format("DROP TABLE %s CASCADE CONSTRAINTS", quotize(entity.getName()));
            logger.trace(query);
            drop.executeUpdate(query);
        } catch (SQLException ex) {
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
            } catch (Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }
    }


    @Override
    protected void dropColumn(DbEntity entity, String... columns) throws DatabaseEngineException {
        Statement drop = null;

        List<String> removeColumns = new ArrayList<String>();
        removeColumns.add("ALTER TABLE");
        removeColumns.add(quotize(entity.getName()));
        removeColumns.add("DROP");
        List<String> cols = new ArrayList<String>();
        for (String col : columns) {
            cols.add(quotize(col));
        }
        removeColumns.add("( " + join(cols, ",") + " )");

        try {
            drop = conn.createStatement();
            final String query = join(removeColumns, " ");
            logger.trace(query);
            drop.executeUpdate(query);
        } catch (SQLException ex) {
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
            } catch (Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }

    }

    @Override
    protected void addColumn(DbEntity entity, DbColumn... columns) throws DatabaseEngineException {
        List<String> addColumns = new ArrayList<String>();
        addColumns.add("ALTER TABLE");
        addColumns.add(quotize(entity.getName()));
        addColumns.add("ADD");
        List<String> cols = new ArrayList<String>();
        for (DbColumn c : columns) {
            List<String> column = new ArrayList<String>();
            column.add(quotize(c.getName()));
            column.add(translateType(c));

            if (c.isDefaultValueSet() && !c.getDbColumnType().equals(DbColumnType.BOOLEAN)) {
                column.add("DEFAULT");
                column.add(translate(c.getDefaultValue()));
            }

            for (DbColumnConstraint cc : c.getColumnConstraints()) {
                column.add(cc.translate());
            }

            cols.add(join(column, " "));
        }
        addColumns.add("( " + join(cols, ",") + " )");
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

            PreparedStatement ps = null;
            if (useAutoInc) {
                ps = me.getInsertReturning();
                synchronizeSequence(name, me);
            } else {
                ps = me.getInsertWithAutoInc();
            }

            int i = entityToPreparedStatement(me.getEntity(), ps, entry, useAutoInc);

            if (useAutoInc) {
                ((OraclePreparedStatement) ps).registerReturnParameter(i + 1, OracleTypes.NUMBER);
            }

            ps.execute();

            long ret = 0;

            if (useAutoInc) {
                generatedKeys = ((OraclePreparedStatement) ps).getReturnResultSet();
                if (generatedKeys.next()) {
                    ret = generatedKeys.getLong(1);
                }

                generatedKeys.close();
            } else {
                me.setSequenceDirty(true);
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

    /**
     * Synchronizes the given sequence to the current value.
     *
     * @param name The table name that the sequence belongs.
     * @param me   The mapped entity.
     */
    private void synchronizeSequence(String name, MappedEntity me) {
        if (!me.isSequenceDirty()) {
            return;
        }

        final String sequenceName = md5(format("%s_%s_SEQ", name, me.getAutoIncColumn()), properties.getMaxIdentifierSize());

        // Touch the sequence.
        final List<String> touch = ImmutableList.of(
                String.format("SELECT %s.NEXTVAL FROM DUAL", quotize(sequenceName)),
                String.format("ALTER SEQUENCE %s INCREMENT BY -1", quotize(sequenceName)),
                String.format("SELECT %s.NEXTVAL FROM DUAL", quotize(sequenceName)),
                String.format("ALTER SEQUENCE %s INCREMENT BY 1", quotize(sequenceName))
        );
        silentlyExecuteStatements(touch);

        // Event if there are no entries in the table, the operation is evaluated as NULL so it's safe.
        final String sqlDiff = String.format("SELECT (SELECT MAX(%s) FROM %s) - %s.CURRVAL FROM DUAL",
                quotize(me.getAutoIncColumn()),
                quotize(name),
                quotize(sequenceName));
        logger.trace(dev, "{}", sqlDiff);

        try (
                Statement statementDiffSeq = conn.createStatement();
                ResultSet diff = statementDiffSeq.executeQuery(sqlDiff);
        ) {

            if (diff.next()) {
                final long diffLong = diff.getObject(1) != null ? diff.getLong(1) : -1L;
                if (diffLong > 0) {

                    final List<String> stmts = ImmutableList.of(
                            String.format("ALTER SEQUENCE %s INCREMENT BY %s", quotize(sequenceName), diffLong),
                            String.format("SELECT %s.NEXTVAL FROM DUAL", quotize(sequenceName)),
                            String.format("ALTER SEQUENCE %s INCREMENT BY 1", quotize(sequenceName))
                    );

                    silentlyExecuteStatements(stmts);
                }
            }

            me.setSequenceDirty(false);
        } catch (SQLException e) {
            logger.debug("Error querying.", e);
        } catch (Exception e) {
            logger.debug("Error closing resources.", e);
        }
    }

    /**
     * Executes the given list of statements silently, i.e. there's no control over failures.
     *
     * @param stmts The list of statements to execute.
     */
    private void silentlyExecuteStatements(List<String> stmts) {
        for (String stmt : stmts) {
            try (
                    Statement statementSyncSequence = conn.createStatement();
            ) {
                logger.trace(dev, "{}", stmt);
                statementSyncSequence.execute(stmt);
            } catch (SQLException e) {
                logger.debug("Error executing statement.", e);
            } catch (Exception e) {
                logger.debug("Error closing statement.", e);
            }
        }
    }

    @Override
    protected void addFks(DbEntity entity) throws DatabaseEngineException {
        for (DbFk fk : entity.getFks()) {
            final List<String> quotizedLocalColumns = new ArrayList<String>();
            for (String s : fk.getLocalColumns()) {
                quotizedLocalColumns.add(quotize(s));
            }

            final List<String> quotizedForeignColumns = new ArrayList<String>();
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
            } catch (SQLException ex) {
                if (ex.getMessage().startsWith(FOREIGN_ALREADY_EXISTS)) {
                    logger.debug(dev, "Foreign key for table '{}' already exists. Error code: {}.", entity.getName(), ex.getMessage(), ex);
                    handleOperation(new OperationFault(entity.getName(), OperationFault.Type.FOREIGN_KEY_ALREADY_EXISTS), ex);
                } else {
                    throw new DatabaseEngineException(format("Could not add Foreign Key to entity %s. Error code: %s.", entity.getName(), ex.getMessage()), ex);
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
    protected boolean checkConnection(final Connection conn) {
        Statement s = null;
        try {
            s = conn.createStatement();
            s.executeQuery("select 1 from dual");

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

    /**
     * Gets the schema. This implementation uses the username when the schema is null.
     *
     * @return The schema or the username when the first is null.
     */
    @Override
    protected String getSchema() {
        return Optional.fromNullable(super.getSchema()).or(properties.getUsername());
    }

    @Override
    public synchronized Map<String, DbColumnType> getMetadata(final String name) throws DatabaseEngineException {

        // start with the default implementation. Then override some definitions here.
        final Map<String, DbColumnType> metaMap = super.getMetadata(name);
        Statement s = null;
        ResultSet rsColumns = null;
        try {
            getConnection();

            s = conn.createStatement();
            rsColumns = s.executeQuery(String.format("SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION FROM ALL_TAB_COLS WHERE TABLE_NAME = '%s' AND OWNER = UPPER('%s')", name, properties.getProperty(PdbProperties.USERNAME)));

            while (rsColumns.next()) {
                String columnName = rsColumns.getString("COLUMN_NAME");
                /*
                Columns starting with SYS_ are Oracle system-generated columns
                and PDB should not interfere with these columns. Not storing
                these in the metaMap makes them invisible to PDB.

                See: http://docs.oracle.com/cd/B19306_01/server.102/b14200/ap_keywd.htm
                 */
                if (!columnName.toUpperCase().startsWith("SYS_")) {
                    final String dataPrecision = rsColumns.getString("DATA_PRECISION");

                    final DbColumnType value = toPdbType(dataPrecision == null ? rsColumns.getString("DATA_TYPE") : (rsColumns.getString("DATA_TYPE") + dataPrecision));
                    if (value != DbColumnType.UNMAPPED) {
                        metaMap.put(columnName, value);
                    }
                }
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

            try {
                if (s != null) {
                    s.close();
                }
            } catch (Exception a) {
                logger.trace("Error closing statement.", a);
            }
        }
    }

    private DbColumnType toPdbType(String type) {
        /**
         * We want to override the default mappings depending on number precision.
         */
        Preconditions.checkNotNull(type, "Type cannot be null.");

        switch (type) {
            case "NUMBER":
                return DbColumnType.INT;
            case "CHAR":
                return DbColumnType.BOOLEAN;
            case "FLOAT":
            case "FLOAT126":
            case "BINARY_DOUBLE":
                return DbColumnType.DOUBLE;
            case "LONG":
            case "NUMBER19":
                return DbColumnType.LONG;
            case "VARCHAR2":
            case "NVARCHAR2":
                return DbColumnType.STRING;
            case "CLOB":
                return DbColumnType.CLOB;
            case "BLOB":
                return DbColumnType.BLOB;
            default:
                return DbColumnType.UNMAPPED;
        }
    }

    @Override
    protected ResultIterator createResultIterator(Statement statement, String sql) throws DatabaseEngineException {
        return new OracleResultIterator(statement, sql);
    }

    @Override
    protected ResultIterator createResultIterator(PreparedStatement ps) throws DatabaseEngineException {
        return new OracleResultIterator(ps);
    }
}
