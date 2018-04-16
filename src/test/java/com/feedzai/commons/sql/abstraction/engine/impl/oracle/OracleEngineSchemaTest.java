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
package com.feedzai.commons.sql.abstraction.engine.impl.oracle;

import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.dml.Expression;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineSchemaTest;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.BLOB;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.CLOB;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.L;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.column;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.in;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.k;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.select;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.table;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.COMPRESS_LOBS;
import static com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineSchemaTest.Ieee754Support.SUPPORTED_STRINGS;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Rafael Marmelo (rafael.marmelo@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class OracleEngineSchemaTest extends AbstractEngineSchemaTest {


    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("oracle");
    }

    @Override
    protected Ieee754Support getIeee754Support() {
        return SUPPORTED_STRINGS;
    }

    @Override
    protected String getTestSchema() {
        // to have lowercase schema, Oracle needs it to be quoted
        return "\"myschema\"";
    }

    @Override
    protected void defineUDFGetOne(final DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate(
            "CREATE OR REPLACE FUNCTION GetOne " +
            "RETURN INTEGER " +
            "AS " +
            "BEGIN " +
            "    RETURN 1;" +
            "END GetOne;"
        );
    }

    @Override
    protected void defineUDFTimesTwo(final DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate(
            "CREATE OR REPLACE FUNCTION " + getTestSchema() + ".TimesTwo (n IN INTEGER) " +
            "RETURN INTEGER " +
            "AS " +
            "BEGIN " +
            "    RETURN n * 2;" +
            "END TimesTwo;"
        );
    }

    @Override
    protected void createSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException {
        // create user (=schema) with all privileges granted
        // schema needs to be quotized to have proper case; assume it already is if it starts with double quote
        engine.executeUpdate(
            "GRANT ALL PRIVILEGES TO " + (schema.startsWith("\"") ? schema : quotize(schema))
                + " IDENTIFIED BY " + engine.getProperties().getPassword() + " WITH ADMIN OPTION"
        );
    }

    @Override
    protected void dropSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException {
        // schema needs to be quotized to have proper case; assume it already is if it starts with double quote
        engine.executeUpdate(
                "DECLARE\n" +
                "   not_exists EXCEPTION;" +
                "   PRAGMA EXCEPTION_INIT(not_exists, -01918);" +
                "BEGIN\n" +
                "   EXECUTE IMMEDIATE 'DROP USER " + (schema.startsWith("\"") ? schema : quotize(schema)) + " CASCADE';" +
                "EXCEPTION\n" +
                "   WHEN not_exists THEN null; -- ignore the error\n" +
                "END;"
        );
    }

    /**
     * Checks that system generated columns with name starting with SYS_ in the
     * Oracle engine do not appear in the table metadata.
     *
     * @throws Exception propagates any Exception thrown by the test
     */
    @Test
    public void testSystemGeneratedColumns() throws Exception {

        try (final DatabaseEngine engine = DatabaseFactory.getConnection(properties)) {
            final DbEntity entity = dbEntity()
                    .name("TEST_SYS_COL")
                    // Simulates a system generated column
                    .addColumn("SYS_COL1", INT)
                    .addColumn("COL1", INT)
                    .pkFields("COL1")
                    .build();
            engine.addEntity(entity);

            assertFalse(
                    "The simulated system generated column should not appear in the table metadata",
                    engine.getMetadata("TEST_SYS_COL").containsKey("SYS_COL1")
            );
            assertTrue(
                    "The regular column should appear in the table metadata",
                    engine.getMetadata("TEST_SYS_COL").containsKey("COL1")
            );
        }
    }

    /**
     * This method tests that, when the {@link PdbProperties pdb.compress_lobs} is true (default value is false),
     * the LOB columns are compressed: Both BLOB and CLOB types are tested.
     *
     * @throws Exception if anything goes wrong with the test
     */
    @Test
    public void testCompressLobs() throws Exception {
        final String tablespace = "TEST_TABLESPACE";
        final String tableName = "TEST_TABLE";
        final String tableUserLobs = "USER_LOBS";

        final String idColumn = "ID";
        final String blobColumn = "BLOB_COLUMN";
        final String clobColumn = "CLOB_COLUMN";

        final String compression = "COMPRESSION";
        final String secureFile = "SECUREFILE";

        properties.setProperty(COMPRESS_LOBS, Boolean.toString(true));

        final DatabaseEngine engine = DatabaseFactory.getConnection(properties);

        createUserTablespace(tablespace, engine.getProperties().getUsername(), engine);

        final DbEntity entity = dbEntity()
                .name(tableName)
                .addColumn(idColumn, INT)
                .addColumn(blobColumn, BLOB)
                .addColumn(clobColumn, CLOB)
                .pkFields(idColumn)
                .build();

        engine.addEntity(entity);

        assertTrue("ID column should exist", engine.getMetadata(tableName).containsKey(idColumn));
        assertTrue("BLOB_COLUMN column should exist", engine.getMetadata(tableName).containsKey(blobColumn));
        assertTrue("CLOB_COLUMN column should exist", engine.getMetadata(tableName).containsKey(clobColumn));

        // Now, test that the both columns are configured with secure file and compression is enable with "medium"
        final Expression query =
                select(column(compression), column(secureFile))
                        .from(table(tableUserLobs))
                        .where(in(column("COLUMN_NAME"), L(k(clobColumn), k(blobColumn))));

        final List<Map<String, ResultColumn>> results = engine.query(query);

        assertEquals("Check that two lines are returned",2, results.size());

        for (final Map<String, ResultColumn> result : results) {
            assertEquals("Check that compression is defined as MEDIUM", result.get(compression).toString(), "MEDIUM");
            assertEquals("Check that secure file is enabled", result.get(secureFile).toString(), "YES");
        }
    }

    /**
     * Helper method that creates and sets a new tablespace for a user.
     *
     * @param tablespace the name of the tablespace
     * @param user       the user for which its default tablespace will be assigned
     * @param engine     the database engine used to create the tablespace
     * @throws DatabaseEngineException  if there is a problem when executing the update tablespace query
     */
    private void createUserTablespace(final String tablespace,
                                      final String user,
                                      final DatabaseEngine engine) throws DatabaseEngineException {

        final String createTablespace = String.format("CREATE TABLESPACE %s DATAFILE 'tbs_f1.dat' SIZE 40M", tablespace);
        final String updateTablespace = String.format("ALTER USER %s DEFAULT TABLESPACE %s", user, tablespace);

        engine.query(createTablespace);
        engine.query(updateTablespace);
    }
}
