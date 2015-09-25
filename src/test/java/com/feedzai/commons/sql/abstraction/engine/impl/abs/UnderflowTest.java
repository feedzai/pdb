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
import com.feedzai.commons.sql.abstraction.engine.*;
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

import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests to ensure values less than 1.0e-131 are stored as 0. This is necessary because
 * values less than 1.0e-131 provoke an underflow error in the Oracle JDBC driver when
 * used in bind parameters. This is an oracle-specific issue but the test is applicable
 * to any database server.
 *
 * @author Paulo Leitao (paulo.leitao@feedzai.com)
 *
 * @since 2.1.4
 */
@RunWith(Parameterized.class)
public class UnderflowTest {

    /*
     * Test table properties, a table with a PK and two double colums.
     */
    private static String TEST_TABLE = "TEST_TBL";
    private static final String PK_COL = "PK_COL";
    private static final String ERROR_COL = "ERROR_COL";
    private static final String NORMAL_COL = "VALUE_COL";

    /*
     * Values for the test columns.
     */
    private static final long PK_VALUE = 10001234;
    private static final double ERROR_VALUE = 1.0e-131;     // Causes underflow exception if not persisted as 0
    private static final double NORMAL_VALUE = 15.0;        // Should be persisted as is

    private DatabaseEngine dbEngine;

    /**
     * Configurations the test will run with, set them in connection.properties and ensure
     * they are included in the -Dinstances VM args.
     *
     * @return  The configurations under which the test runs.
     */
    @Parameterized.Parameters
    public static Collection<Object[]> data() throws Exception {
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
                .addColumn(ERROR_COL, DbColumnType.DOUBLE)
                .addColumn(NORMAL_COL, DbColumnType.DOUBLE)
                .pkFields(PK_COL)
                .build();
        dbEngine.addEntity(testEntity);
    }

    /**
     * Scenario for an insert using persist().
     */
    @Test
    public void testUnderflowNormal() throws DatabaseFactoryException, DatabaseEngineException {
        EntityEntry.Builder entryBuilder = new EntityEntry.Builder();
        dbEngine.persist(TEST_TABLE, getTestEntry());
        dbEngine.commit();
        checkInsertedValue();
    }

    /**
     * Scenario for an insert using prepared statements / setParameters().
     */
    @Test
    public void testUnderflowPreparedStatement1() throws Exception {
        String PS_NAME = "MyPS";
        String insertQuery = "insert into TEST_TBL(PK_COL,ERROR_COL,VALUE_COL) values (?,?,?)";
        dbEngine.createPreparedStatement(PS_NAME, insertQuery);
        dbEngine.clearParameters(PS_NAME);
        dbEngine.setParameters(PS_NAME, PK_VALUE, ERROR_VALUE, NORMAL_VALUE);
        dbEngine.executePS(PS_NAME);
        dbEngine.commit();
        checkInsertedValue();
    }

    /**
     * Scenario for an insert using prepared statements / setParameter().
     */
    @Test
    public void testUnderflowPreparedStatement2() throws Exception {
        String PS_NAME = "MyPS";
        String insertQuery = "insert into TEST_TBL(PK_COL,ERROR_COL,VALUE_COL) values (?,?,?)";
        dbEngine.createPreparedStatement(PS_NAME, insertQuery);
        dbEngine.clearParameters(PS_NAME);
        dbEngine.setParameter(PS_NAME, 1, PK_VALUE);
        dbEngine.setParameter(PS_NAME, 2, ERROR_VALUE);
        dbEngine.setParameter(PS_NAME, 3, NORMAL_VALUE);
        dbEngine.executePS(PS_NAME);
        dbEngine.commit();
        checkInsertedValue();
    }

    /**
     * Scenario for an insert using batch updates.
     */
    @Test
    public void testUnderflowBatch() throws DatabaseFactoryException, DatabaseEngineException {
        EntityEntry.Builder entryBuilder = new EntityEntry.Builder();
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
                .set(ERROR_COL, ERROR_VALUE)
                .set(NORMAL_COL, NORMAL_VALUE)
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
        assertEquals(ERROR_VALUE, firstRow.get(ERROR_COL).toDouble(), 1.0e131);
        assertEquals(NORMAL_VALUE, firstRow.get(NORMAL_COL).toDouble(), 0);
    }

    @After
    public void cleanup() throws DatabaseEngineException {
        dbEngine.close();
    }

}
