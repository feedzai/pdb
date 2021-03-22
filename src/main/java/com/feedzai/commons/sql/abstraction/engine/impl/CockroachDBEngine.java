/*
 * Copyright 2019 Feedzai
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
import com.feedzai.commons.sql.abstraction.engine.AbstractTranslator;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineDriver;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.MappedEntity;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.handler.OperationFault;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.feedzai.commons.sql.abstraction.util.StringUtils.md5;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * CockroachDB specific database implementation.
 *
 * @author Mário Pereira (mario.arzileiro@feedzai.com)
 * @since 2.5.0
 */
public class CockroachDBEngine extends PostgreSqlEngine {

    /**
     * The Cockroach JDBC (PostgreSQL) driver.
     */
    protected static final String COCKROACHDB_DRIVER = DatabaseEngineDriver.COCKROACHDB.driver();

    /**
     * Constraint name already exists.
     *
     * Note: CockroachDB v19.2.x returns this SQL state when trying to add a foreign key relation that already exists.
     * This constant is needed because PosgtgreSQL uses code 42710 - ERRCODE_DUPLICATE_OBJECT instead.
     * In this situation CockroachDB v19.2.x uses code 23503 - ERRCODE_FOREIGN_KEY_VIOLATION (a column in a table has
     * a value that is not present in the column of other table in the foreign key relation);
     * in the same situation CockroachDB v19.1.x returns state 42830 - ERRCODE_INVALID_FOREIGN_KEY (which can occur for
     * example when the types of columns in the relation have incompatible types).
     * @see <a href="https://github.com/cockroachdb/cockroach/issues/42858">CockroachDB issue #42858</a>
     */
    public static final String CONSTRAINT_NAME_ALREADY_EXISTS_COCKROACH = "23503";

    /**
     * Creates a new CockroachDB connection.
     *
     * @param properties The properties for the database connection.
     * @throws DatabaseEngineException When the connection fails.
     */
    public CockroachDBEngine(final PdbProperties properties) throws DatabaseEngineException {
        super(properties, COCKROACHDB_DRIVER);
    }

    @Override
    protected void createTable(final DbEntity entity) throws DatabaseEngineException {

        List<String> createTable = new ArrayList<>();

        createTable.add("CREATE TABLE");
        createTable.add(quotize(entity.getName()));

        // COLUMNS
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
        createTable.add("(" + join(columns, ", "));
        // COLUMNS end


        // PRIMARY KEY
        List<String> pks = new ArrayList<>();
        for (String pk : entity.getPkFields()) {
            pks.add(quotize(pk));
        }

        if (!pks.isEmpty()) {
            createTable.add(",");

            final String pkName = md5(format("PK_%s", entity.getName()), properties.getMaxIdentifierSize());

            createTable.add("CONSTRAINT");
            createTable.add(quotize(pkName));
            createTable.add("PRIMARY KEY");
            createTable.add("(" + join(pks, ", ") + ")");
        }
        // PK end

        createTable.add(")");

        final String createTableStatement = join(createTable, " ");

        logger.trace(createTableStatement);

        Statement s = null;
        try {
            s = conn.createStatement();
            s.executeUpdate(createTableStatement);
        } catch (final SQLException ex) {
            if (ex.getSQLState().equals(NAME_ALREADY_EXISTS)) {
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
        // Do nothing because Primary Keys are added at creation table time.
    }

    @Override
    public Class<? extends AbstractTranslator> getTranslatorClass() {
        return CockroachDBTranslator.class;
    }

    @Override
    protected void updatePersistAutoIncSequence(final MappedEntity mappedEntity, long currentAutoIncVal) {
        executeUpdateSilently(format(
                "SELECT setval('%s', %d, false)",
                getQuotizedSequenceName(mappedEntity.getEntity(), mappedEntity.getAutoIncColumn()),
                currentAutoIncVal + 1
        ));
    }

    protected void addFks(final DbEntity entity, final Set<DbFk> fks) throws DatabaseEngineException {
        try {
            super.addFks(entity, fks);
        } catch (final DatabaseEngineException ex) {
            if (ex.getCause() instanceof SQLException) {
                final SQLException sqlException = (SQLException) ex.getCause();
                if (sqlException.getSQLState().equals(CONSTRAINT_NAME_ALREADY_EXISTS_COCKROACH)) {
                    logger.debug(dev, "Foreign key for table '{}' already exists. Error code: {}.", entity.getName(), sqlException.getSQLState());
                    return;
                }
            }
            throw ex;
        }
    }

    @Override
    protected void addSequences(final DbEntity entity) throws DatabaseEngineException {
        for (final DbColumn column : entity.getColumns()) {
            if (!column.isAutoInc()) {
                continue;
            }

            final String sequenceName = getQuotizedSequenceName(entity, column.getName());

            final StringBuilder createSequence = new StringBuilder()
                    .append("CREATE SEQUENCE ")
                    .append(sequenceName)
                    .append(" MINVALUE 0 MAXVALUE ");
            switch (column.getDbColumnType()) {
                case INT:
                    createSequence.append(Integer.MAX_VALUE);
                    break;
                case LONG:
                    createSequence.append(Long.MAX_VALUE);
                    break;
                default:
                    throw new DatabaseEngineException("Auto incrementation is only supported on INT and LONG");
            }
            createSequence.append(" START 1 INCREMENT 1;");

            createSequence.append("ALTER TABLE ")
                    .append(quotize(entity.getName()))
                    .append(" ALTER COLUMN ")
                    .append(quotize(column.getName()))
                    .append(" SET DEFAULT nextval('").append(sequenceName).append("')");

            final String statement = createSequence.toString();

            logger.trace(statement);

            Statement s = null;
            try {
                s = conn.createStatement();
                s.executeUpdate(statement);
            } catch (final SQLException ex) {
                if (ex.getSQLState().equals(NAME_ALREADY_EXISTS)) {
                    logger.debug(dev, "Sequence {} is already defined", sequenceName);
                    handleOperation(
                            new OperationFault(entity.getName(), OperationFault.Type.SEQUENCE_ALREADY_EXISTS), ex
                    );
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
    protected void dropSequences(final DbEntity entity) throws DatabaseEngineException {
        for (final DbColumn column : entity.getColumns()) {
            if (!column.isAutoInc()) {
                continue;
            }

            final String sequenceName = getQuotizedSequenceName(entity, column.getName());
            final String stmt = format("DROP SEQUENCE %s", sequenceName);

            Statement drop = null;
            try {
                drop = conn.createStatement();
                logger.trace(stmt);
                drop.executeUpdate(stmt);
            } catch (final SQLException ex) {
                if (ex.getSQLState().equals(TABLE_OR_VIEW_DOES_NOT_EXIST)) {
                    logger.debug(dev, "Sequence {} does not exist", sequenceName);
                    handleOperation(
                            new OperationFault(entity.getName(), OperationFault.Type.SEQUENCE_DOES_NOT_EXIST), ex
                    );
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
    public synchronized Map<String, DbColumnType> getMetadata(final String schemaPattern,
                                                              final String tableNamePattern) throws DatabaseEngineException {
        final Map<String, DbColumnType> metaMap = new LinkedHashMap<>();

        ResultSet rsColumns = null;
        try {
            getConnection();

            final DatabaseMetaData meta = this.conn.getMetaData();
            rsColumns = meta.getColumns(null, schemaPattern, tableNamePattern, null);
            while (rsColumns.next()) {
                final String columnName = rsColumns.getString("COLUMN_NAME");
                if (columnName.equals("rowid")) {
                    continue;
                }
                metaMap.put(columnName, toPdbType(rsColumns.getInt("DATA_TYPE"), rsColumns.getString("TYPE_NAME")));
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
        }
    }

    /**
     * Gets a name to use in a sequence, already quotized.
     *
     * @param entity  The entity for which the sequence will be used.
     * @param colName The name of the column that will be using the sequence.
     * @return the quotized sequence name.
     */
    private String getQuotizedSequenceName(final DbEntity entity, final String colName) {
        return quotize(md5(format("%s_%s_SEQ", entity.getName(), colName), properties.getMaxIdentifierSize()));
    }
}
