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
package com.feedzai.commons.sql.abstraction.engine.impl.abs;

import com.feedzai.commons.sql.abstraction.ddl.DbColumnType;
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.dml.Expression;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.all;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.select;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.table;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests persistence of long values. This is a regression test for https://github.com/feedzai/pdb/issues/27,
 * where long values such as 876534351009985545l are not read as they were stored due to the intermediate
 * use of a double when reading from a ResultSet.
 *
 * @author Paulo Leitao (paulo.leitao@feedzai.com)
 *
 * @since 2.1.5
 */
@RunWith(Parameterized.class)
public class LongOverflowTest {

    /*
     * Test table name and columns.
     */
    private static final String TEST_TABLE = "TEST_OVFL_TBL";
    private static final String PK_COL = "PK_COL";
    private static final String LONG_COL = "LONG_COL";
    private static final String DBL_COL_1 = "DBL_COL_1";
    private static final String DBL_COL_2 = "DBL_COL_2";

    /*
     * Values for the test columns.
     */
    private static final long PK_VALUE = 1;

    /**
     * A long that was causing an overflow prior to the fix.
     */
    private static final long ERROR_VALUE = 876534351009985545L;

    /**
     * A double with no decimal digits.
     */
    private static final double DBL_INT_VALUE = 13.0d;

    /**
     * A double with decimal digits, when read with toLong() the value (long) 13.6 must be read.
     */
    private static final double DBL_FRAC_VALUE = 13.6d;


    private DatabaseEngine dbEngine;

    /**
     * Configurations the test will run with, set them in connection.properties and ensure
     * they are included in the -Dinstances VM args.
     *
     * @return  The configurations under which the test runs.
     */
    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations();
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;


    @Before
    public void createTestTable() throws Exception {
        // Connect to db
        Properties dbProps = new Properties() {
            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "create-drop");
            }
        };
        dbEngine = DatabaseFactory.getConnection(dbProps);

        // Create table
        DbEntity testEntity = new DbEntity.Builder()
                .name(TEST_TABLE)
                .addColumn(PK_COL, DbColumnType.LONG)
                .addColumn(LONG_COL, DbColumnType.LONG)
                .addColumn(DBL_COL_1, DbColumnType.DOUBLE)
                .addColumn(DBL_COL_2, DbColumnType.DOUBLE)
                .pkFields(PK_COL)
                .build();
        dbEngine.addEntity(testEntity);

        dbEngine.beginTransaction();
    }

    /**
     * Scenario for an insert using persist().
     */
    @Test
    public void testLongOverflowNormal() throws DatabaseEngineException {
        dbEngine.persist(TEST_TABLE, getTestEntry());
        dbEngine.commit();
        checkInsertedValue();
    }

    /**
     * Scenario for an insert using prepared statements / setParameters().
     */
    @Test
    public void testLongOverflowPreparedStatement1() throws Exception {
        String PS_NAME = "MyPS";
        final String ec = dbEngine.escapeCharacter();
        final String insertQuery = "INSERT INTO " + quotize(TEST_TABLE, ec) +
            "(" + quotize(PK_COL, ec) + ", " + quotize(LONG_COL, ec) + ", " + quotize(DBL_COL_1, ec) + ", " + quotize(DBL_COL_2, ec) +
            ") VALUES (?,?,?,?)";

        dbEngine.createPreparedStatement(PS_NAME, insertQuery);
        dbEngine.clearParameters(PS_NAME);
        dbEngine.setParameters(PS_NAME, PK_VALUE, ERROR_VALUE, DBL_INT_VALUE, DBL_FRAC_VALUE);
        dbEngine.executePS(PS_NAME);
        dbEngine.commit();
        checkInsertedValue();
    }

    /**
     * Scenario for an insert using prepared statements / setParameter().
     */
    @Test
    public void testLongOverflowPreparedStatement2() throws Exception {
        String PS_NAME = "MyPS";
        final String ec = dbEngine.escapeCharacter();
        final String insertQuery = "INSERT INTO " + quotize(TEST_TABLE, ec) +
            "(" + quotize(PK_COL, ec) + ", " + quotize(LONG_COL, ec) + ", " + quotize(DBL_COL_1, ec) + ", " + quotize(DBL_COL_2, ec) +
            ") VALUES (?,?,?,?)";

        dbEngine.createPreparedStatement(PS_NAME, insertQuery);
        dbEngine.clearParameters(PS_NAME);
        dbEngine.setParameter(PS_NAME, 1, PK_VALUE);
        dbEngine.setParameter(PS_NAME, 2, ERROR_VALUE);
        dbEngine.setParameter(PS_NAME, 3, DBL_INT_VALUE);
        dbEngine.setParameter(PS_NAME, 4, DBL_FRAC_VALUE);
        dbEngine.executePS(PS_NAME);
        dbEngine.commit();
        checkInsertedValue();
    }

    /**
     * Scenario for an insert using batch updates.
     */
    @Test
    public void testLongOverflowBatch() throws DatabaseEngineException {
        dbEngine.addBatch(TEST_TABLE, getTestEntry());
        dbEngine.flush();
        dbEngine.commit();
        checkInsertedValue();
    }

    /**
     * Creates the EntityEntry used in all tests.
     *
     * @return  The created EntityEntry.
     */
    private EntityEntry getTestEntry() {
        return new EntityEntry.Builder()
                .set(PK_COL, PK_VALUE)
                .set(LONG_COL, ERROR_VALUE)
                .set(DBL_COL_1, DBL_INT_VALUE)
                .set(DBL_COL_2, DBL_FRAC_VALUE)
                .build();
    }

    /**
     * Checks that the test table has a single entry and with the values as expected.
     */
    private void checkInsertedValue() throws DatabaseEngineException {
        Expression query = select(all()).from(table(TEST_TABLE));
        List<Map<String, ResultColumn>> results = dbEngine.query(query);
        assertEquals(1, results.size());
        Map<String, ResultColumn> firstRow = results.get(0);
        assertNotNull(firstRow);
        assertEquals(ERROR_VALUE, firstRow.get(LONG_COL).toLong().longValue());
        assertEquals((long) DBL_INT_VALUE, firstRow.get(DBL_COL_1).toLong().longValue());
        assertEquals((long) DBL_FRAC_VALUE, firstRow.get(DBL_COL_2).toLong().longValue());
    }

    @After
    public void cleanup() {
        dbEngine.close();
    }

}
