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
import com.feedzai.commons.sql.abstraction.dml.result.H2ResultIterator;
import com.feedzai.commons.sql.abstraction.dml.result.ResultIterator;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineDriver;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineImpl;
import com.feedzai.commons.sql.abstraction.engine.MappedEntity;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.feedzai.commons.sql.abstraction.util.TypeTranslationUtils;
import com.feedzai.commons.sql.abstraction.util.MathUtil;
import static com.feedzai.commons.sql.abstraction.util.StringUtil.*;

import java.io.StringReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import static java.lang.String.format;

/**
 * H2 specific database implementation.
 *
 * @author joao.silva
 */
public class H2Engine extends DatabaseEngineImpl {

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
     * Table or view does not exist.
     */
    public static final String CONSTRAINT_NAME_ALREADY_EXISTS = "90045";

    /**
     * Creates a new PostgreSql connection.
     *
     * @param properties The properties for the database connection.
     * @throws DatabaseEngineException When the connection fails.
     */
    public H2Engine(Properties properties) throws DatabaseEngineException {
        super(H2_DRIVER, properties, Dialect.H2);
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
                            ps.setCharacterStream(i, sr);
                        }
                        else {
                            throw new DatabaseEngineException("Cannot convert " + val.getClass().getSimpleName() + " to String. CLOB columns only accept Strings.");
                        }
                        break;
                    case BOOLEAN:
                        Boolean b = (Boolean) val;
                        if (b == null) {
                            ps.setObject(i, null);
                        }
                        else {
                            ps.setObject(i, b);
                        }

                        break;
                    default:
                        ps.setObject(i, val);
                }
            }
            catch (Exception ex) {
                throw new DatabaseEngineException("Error while mapping variables to database", ex);
            }

            i++;
        }

        return i - 1;
    }

    @Override
    protected void createTable(final DbEntity entity) throws DatabaseEngineException {

        List<String> createTable = new ArrayList<String>();

        createTable.add("CREATE TABLE");
        createTable.add(quotize(entity.getName()));
        List<String> columns = new ArrayList<String>();
        List<String> pkFields = Arrays.asList(entity.getPkFields());
        for (DbColumn c : entity.getColumns()) {
            List<String> column = new ArrayList<String>();
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
                column.add(c.getDefaultValue().translateH2(properties));
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
        }
        catch (SQLException ex) {
            if (ex.getSQLState().startsWith(NAME_ALREADY_EXISTS)) {
                logger.debug(dev, "'{}' is already defined", entity.getName());
            }
            else {
                throw new DatabaseEngineException("Something went wrong handling statement", ex);
            }
        }
        finally {
            try {
                if (s != null) {
                    s.close();
                }
            }
            catch (Exception e) {
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
        }
        catch (SQLException ex) {
            if (ex.getSQLState().startsWith(TABLE_CAN_ONLY_HAVE_ONE_PRIMARY_KEY) || ex.getSQLState().startsWith(CONSTRAINT_NAME_ALREADY_EXISTS)) {
                logger.debug(dev, "'{}' already has a primary key", entity.getName());
            }
            else {
                throw new DatabaseEngineException("Something went wrong handling statement", ex);
            }
        }
        finally {
            try {
                if (s != null) {
                    s.close();
                }
            }
            catch (Exception e) {
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
            }
            catch (SQLException ex) {
                if (ex.getSQLState().startsWith(INDEX_ALREADY_EXISTS)) {
                    logger.debug(dev, "'{}' is already defined", idxName);
                }
                else {
                    throw new DatabaseEngineException("Something went wrong handling statement", ex);
                }
            }
            finally {
                try {
                    if (s != null) {
                        s.close();
                    }
                }
                catch (Exception e) {
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


        PreparedStatement ps, psWithAutoInc;
        try {

            ps = conn.prepareStatement(statement, Statement.RETURN_GENERATED_KEYS);
            psWithAutoInc = conn.prepareStatement(statementWithAutoInt);

            return new MappedEntity().setInsert(ps).setInsertWithAutoInc(psWithAutoInc);
        }
        catch (SQLException ex) {
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
        }
        catch (SQLException ex) {
            if (ex.getSQLState().startsWith(TABLE_OR_VIEW_DOES_NOT_EXIST)) {
                logger.debug(dev, "Table '{}' does not exist", entity.getName());
            }
            else {
                throw new DatabaseEngineException("Error dropping table", ex);
            }
        }
        finally {
            try {
                if (drop != null) {
                    drop.close();
                }
            }
            catch (Exception e) {
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
        }
        catch (SQLException ex) {
            if (ex.getSQLState().startsWith(TABLE_OR_VIEW_DOES_NOT_EXIST)) {
                logger.debug(dev, "Table '{}' does not exist", entity.getName());
            }
            else {
                throw new DatabaseEngineException("Error dropping column", ex);
            }
        }
        finally {
            try {
                if (drop != null) {
                    drop.close();
                }
            }
            catch (Exception e) {
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
                List<String> column = new ArrayList<String>();
                column.add(quotize(c.getName()));
                column.add(translateType(c));

                for (DbColumnConstraint cc : c.getColumnConstraints()) {
                    column.add(cc.translate());
                }

                if (c.isDefaultValueSet()) {
                    column.add("DEFAULT");
                    column.add(c.getDefaultValue().translateH2(properties));
                }

                final String query = format("ALTER TABLE %s ADD COLUMN %s", quotize(entity.getName()), join(column, " "));
                logger.trace(query);
                s.executeUpdate(query);
            }

        }
        catch (SQLException ex) {
            throw new DatabaseEngineException("Something went wrong handling statement", ex);
        }
        finally {
            try {
                if (s != null) {
                    s.close();
                }
            }
            catch (Exception e) {
                logger.trace("Error closing statement.", e);
            }
        }

    }

    @Override
    protected String translateType(DbColumn c) throws DatabaseEngineException {
        return TypeTranslationUtils.translateH2Type(c, properties);
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
                ps = entities.get(name).getInsert();
            }
            else {
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
        }
        catch (Exception ex) {
            throw new DatabaseEngineException("Something went wrong persisting the entity", ex);
        }
        finally {
            try {
                if (generatedKeys != null) {
                    generatedKeys.close();
                }
            }
            catch (Exception e) {
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
            }
            catch (SQLException ex) {
                if (ex.getSQLState().equals(CONSTRAINT_NAME_ALREADY_EXISTS)) {
                    logger.debug(dev, "Foreign key for table '{}' already exists. Error code: {}.", entity.getName(), ex.getSQLState());
                }
                else {
                    throw new DatabaseEngineException(format("Could not add Foreign Key to entity %s. Error code: %s.", entity.getName(), ex.getSQLState()), ex);
                }
            }
            finally {
                try {
                    if (alterTableStmt != null) {
                        alterTableStmt.close();
                    }
                }
                catch (Exception e) {
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
            s.executeQuery("select 1");

            return true;
        }
        catch (SQLException e) {
            logger.debug("Connection is down.", e);
            return false;
        }
        finally {
            try {
                if (s != null) {
                    s.close();
                }
            }
            catch (Exception e) {
                logger.trace("Error closing statement.", e);
            }
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
}
