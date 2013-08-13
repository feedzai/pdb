/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine.impl;

import com.feedzai.commons.sql.abstraction.ddl.*;
import com.feedzai.commons.sql.abstraction.dml.dialect.Dialect;
import com.feedzai.commons.sql.abstraction.dml.result.OracleResultIterator;
import com.feedzai.commons.sql.abstraction.dml.result.ResultIterator;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineDriver;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineImpl;
import com.feedzai.commons.sql.abstraction.engine.MappedEntity;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.feedzai.commons.sql.abstraction.util.TypeTranslationUtils;
import com.feedzai.commons.sql.abstraction.util.MathUtil;
import static com.feedzai.commons.sql.abstraction.util.StringUtil.*;

import oracle.jdbc.OraclePreparedStatement;
import oracle.jdbc.OracleTypes;

import java.io.StringReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import static java.lang.String.format;

/**
 * Oracle specific database implementation.
 */
public class OracleEngine extends DatabaseEngineImpl {

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
     * Creates a new Oracle connection.
     *
     * @param properties The properties for the database connection.
     * @throws DatabaseEngineException When the connection fails.
     */
    public OracleEngine(Properties properties) throws DatabaseEngineException {
        super(ORACLE_DRIVER, properties, Dialect.ORACLE);
    }

    @Override
    protected int entityToPreparedStatement(final DbEntity entity, final PreparedStatement ps, final EntityEntry entry, final boolean useAutoInc) throws DatabaseEngineException {
        int i = 1;
        for (DbColumn column : entity.getColumns()) {
            if (column.isAutoInc() && useAutoInc) {
                continue;
            }

            try {
                Object val = entry.get(column.getName());
                switch (column.getDbColumnType()) {
                    case BLOB:
                        ps.setBytes(i, objectToArray(val));

                        break;
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
                            ps.setObject(i, "1");
                        } else {
                            ps.setObject(i, "0");
                        }

                        break;
                    default:
                        ps.setObject(i, val);
                }
            } catch (Exception ex) {
                throw new DatabaseEngineException("Error while mapping variables to database", ex);
            }

            i++;
        }

        return i - 1;
    }

    /**
     * Overrides {@link DatabaseEngineImpl#setTransactionIsolation()} This is because
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
                column.add(c.getDefaultValue().translateOracle(properties));
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
        if (entity.getPkFields().length == 0) {
            return;
        }

        List<String> pks = new ArrayList<String>();
        for (String pk : entity.getPkFields()) {
            pks.add(quotize(pk));
        }

        final String pkName = MathUtil.md5(format("PK_%s", entity.getName()), properties.getMaxIdentifierSize());

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
        List<DbIndex> indexes = entity.getIndexes();

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
            final String idxName = MathUtil.md5(format("%s_%s_IDX", entity.getName(), join(columnsForName, "_")), properties.getMaxIdentifierSize());
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

            final String sequenceName = MathUtil.md5(format("%s_%s_SEQ", entity.getName(), column.getName()), properties.getMaxIdentifierSize());

            List<String> createSequence = new ArrayList<String>();
            createSequence.add("CREATE SEQUENCE ");
            createSequence.add(quotize(sequenceName));
            createSequence.add("MINVALUE 1");
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
                final String sequenceName = MathUtil.md5(format("%s_%s_SEQ", entity.getName(), column.getName()), properties.getMaxIdentifierSize());
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

            final String sequenceName = MathUtil.md5(format("%s_%s_SEQ", entity.getName(), column.getName()), properties.getMaxIdentifierSize());

            final String stmt = format("DROP SEQUENCE %s", quotize(sequenceName));

            Statement drop = null;
            try {
                drop = conn.createStatement();
                logger.trace(stmt);
                drop.executeUpdate(stmt);
            } catch (SQLException ex) {
                if (ex.getMessage().startsWith(SEQUENCE_DOES_NOT_EXIST)) {
                    logger.debug(dev, "Sequence '{}' does not exist", sequenceName);
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
                column.add(c.getDefaultValue().translateOracle(properties));
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
        return TypeTranslationUtils.translateOracleType(c, properties);
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
                final String sequenceName = MathUtil.md5(format("%s_%s_SEQ", name, me.getAutoIncColumn()), properties.getMaxIdentifierSize());
                // calculate the difference between the max value of the auto increment column to advance the sequence in order to avoid further
                // collisions in the primary key (i.e. if the user forced an id higher than the next sequence value).
                final String sql = "select (select max(\"" + me.getAutoIncColumn() + "\") from \"" + name + "\") - \"" + sequenceName + "\".NEXTVAL FROM dual";
                generatedKeys = getConnection().createStatement().executeQuery(sql);
                if (generatedKeys.next()) {
                    ret = generatedKeys.getLong(1);
                    // if the difference between the nextval of the sequence is greater than 0 the sequence needs to be advanced that amount.
                    // if the difference is negative we need to rewind the sequence.
                    if (ret != 0) {
                        getConnection().createStatement().executeQuery("ALTER SEQUENCE \"" + sequenceName + "\" INCREMENT BY " + ret);
                        getConnection().createStatement().executeQuery("SELECT \"" + sequenceName + "\".NEXTVAL FROM DUAL");
                        getConnection().createStatement().executeQuery("ALTER SEQUENCE \"" + sequenceName + "\" INCREMENT BY 1");
                    }
                }
            }

            return ret == 0 ? null : ret;
        } catch (Exception ex) {
            ex.printStackTrace();
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
                            quotize(MathUtil.md5("FK_" + table + quotizedLocalColumnsSting + quotizedForeignColumnsString, properties.getMaxIdentifierSize())),
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
                    logger.debug(dev, "Foreign key for table '{}' already exists. Error code: {}.", entity.getName(), ex.getMessage());
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
                final String dataPrecision = rsColumns.getString("DATA_PRECISION");

                final DbColumnType value = toPdbType(dataPrecision == null ? rsColumns.getString("DATA_TYPE") : (rsColumns.getString("DATA_TYPE") + dataPrecision));
                if (value != DbColumnType.UNMAPPED) {
                    metaMap.put(rsColumns.getString("COLUMN_NAME"), value);
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
        if (type.equals("NUMBER")) {
            return DbColumnType.INT;
        }

        if (type.equals("CHAR")) {
            return DbColumnType.BOOLEAN;
        }

        if (type.equals("FLOAT126")) {
            return DbColumnType.DOUBLE;
        }

        if (type.equals("NUMBER19")) {
            return DbColumnType.LONG;
        }
        return DbColumnType.UNMAPPED;
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
