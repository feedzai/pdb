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

import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.engine.*;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.DOUBLE;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * @author Rafael Marmelo (rafael.marmelo@feedzai.com)
 * @since 2.0.0
 */
public abstract class AbstractEngineSchemaTest {

    protected DatabaseEngine engine;
    protected Properties properties;

    private static String TABLE_NAME = "TEST_DOUBLE_COLUMN";
    private static String ID_COL = "ID";
    private static String DBL_COL = "DBL_COL";
    private static int PK_VALUE = 1;

    protected abstract String getSchema();

    protected abstract String getDefaultSchema();

    @Before
    public abstract void init() throws Exception;


    //
    // these tests use the default schema
    //

    @Test
    public void udfGetOneTest() throws DatabaseEngineException, DatabaseFactoryException {
        // engine using the default schema
        engine = DatabaseFactory.getConnection(properties);
        defineUDFGetOne(engine);

        List<Map<String, ResultColumn>> query = engine.query(select(udf("GetOne").alias("ONE")));
        assertEquals("result ok?", 1, (int) query.get(0).get("ONE").toInt());
    }


    //
    // these tests use a given schema
    //

    @Test
    public void udfTimesTwoTest() throws DatabaseEngineException, DatabaseFactoryException {
        // engine using the default schema
        this.properties.setProperty(SCHEMA, getSchema());
        engine = DatabaseFactory.getConnection(properties);

        defineUDFTimesTwo(engine);

        List<Map<String, ResultColumn>> query = engine.query(select(udf("TimesTwo", k(10)).alias("TIMESTWO")));
        assertEquals("result ok?", 20, (int) query.get(0).get("TIMESTWO").toInt());
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testPersisttNan() throws Exception {
        testPersistSpecialValues("NaN");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testPersisttNanDouble() throws Exception {
        testPersistSpecialValues(Double.NaN);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPStNan() throws Exception {
        testInsertSpecialValuesByPS("NaN");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPStNanDouble() throws Exception {
        testInsertSpecialValuesByPS(Double.NaN);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPS2tNan() throws Exception {
        testInsertSpecialValuesByPS2("NaN");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPS2tNanDouble() throws Exception {
        testInsertSpecialValuesByPS2(Double.NaN);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testInsertBatchtNan() throws Exception {
        testInsertSpecialValuesByBatch("NaN");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testInsertBatchtNanDouble() throws Exception {
        testInsertSpecialValuesByBatch(Double.NaN);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testPersistInfinity() throws Exception {
        testPersistSpecialValues("Infinity");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testPersistInfinityDouble() throws Exception {
        testPersistSpecialValues(Double.POSITIVE_INFINITY);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testPersistInfinityDoubleNegative() throws Exception {
        testPersistSpecialValues(Double.NEGATIVE_INFINITY);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPSInfinity() throws Exception {
        testInsertSpecialValuesByPS("Infinity");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPSInfinityDouble() throws Exception {
        testInsertSpecialValuesByPS(Double.POSITIVE_INFINITY);
    }


    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPSInfinityDoubleNegative() throws Exception {
        testInsertSpecialValuesByPS(Double.NEGATIVE_INFINITY);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPS2Infinity() throws Exception {
        testInsertSpecialValuesByPS2("Infinity");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPS2InfinityDouble() throws Exception {
        testInsertSpecialValuesByPS2(Double.POSITIVE_INFINITY);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPS2InfinityDoubleNegative() throws Exception {
        testInsertSpecialValuesByPS2(Double.NEGATIVE_INFINITY);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertBatchInfinity() throws Exception {
        testInsertSpecialValuesByBatch("Infinity");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertBatchInfinityDouble() throws Exception {
        testInsertSpecialValuesByBatch(Double.POSITIVE_INFINITY);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertBatchInfinityDoubleNegative() throws Exception {
        testInsertSpecialValuesByBatch(Double.NEGATIVE_INFINITY);
    }

    /**
     * The 'randomString' is not a special value for the BINARY_DOUBLE type so it should throw an error.
     */
    @Test(expected=Exception.class)
    public void testPersistRandomValuesDoNoWorkInBinaryDoubleColumn() throws Exception {
        testPersistSpecialValues("randomString");
    }

    /**
     * The 'randomString' is not a special value for the BINARY_DOUBLE type so it should throw an error.
     */
    @Test(expected=Exception.class)
    public void testInsertPSRandomValuesDoNoWorkInBinaryDoubleColumn() throws Exception {
        testInsertSpecialValuesByPS("randomString");
    }

    /**
     * The 'randomString' is not a special value for the BINARY_DOUBLE type so it should throw an error.
     */
    @Test(expected=Exception.class)
    public void testInsertPS2RandomValuesDoNoWorkInBinaryDoubleColumn() throws Exception {
        testInsertSpecialValuesByPS2("randomString");
    }

    /**
     * The 'randomString' is not a special value for the BINARY_DOUBLE type so it should throw an error.
     */
    @Test(expected=Exception.class)
    public void testInsertBatchRandomValuesDoNoWorkInBinaryDoubleColumn() throws Exception {
        testInsertSpecialValuesByBatch("randomString");
    }

    /**
     * Auxiliary method to persist a provided special value.
     *
     * @param columnValue The column value.
     */
    protected void testPersistSpecialValues(final Object columnValue) throws DatabaseEngineException, DatabaseFactoryException {
        final DatabaseEngine engine = DatabaseFactory.getConnection(properties);
        try {
            final DbEntity entity = createSpecialValuesEntity();
            engine.addEntity(entity);
            final EntityEntry entry = createSpecialValueEntry(columnValue);
            engine.persist(entity.getName(), entry);
            checkResult(engine, entity.getName(), columnValue);
        } finally {
            engine.close();
        }
    }

    /**
     * Auxiliary method to insert a provided special value using a prepared statement.
     *
     * @param columnValue The column value.
     */
    protected void testInsertSpecialValuesByPS(final Object columnValue) throws DatabaseEngineException, DatabaseFactoryException, NameAlreadyExistsException, ConnectionResetException {
        final DatabaseEngine engine = DatabaseFactory.getConnection(properties);
        final String PSName = "PS_DUMMY";
        final String preparedStatementQuery = "insert into TEST_DOUBLE_COLUMN(ID, DBL_COL) values (?,?)";
        try {
            final DbEntity entity = createSpecialValuesEntity();
            engine.addEntity(entity);
            engine.createPreparedStatement(PSName, preparedStatementQuery);
            engine.clearParameters(PSName);
            engine.setParameters(PSName, PK_VALUE, columnValue);
            engine.executePS(PSName);
            engine.commit();
            checkResult(engine, TABLE_NAME, columnValue);
        } finally {
            engine.close();
        }
    }

    /**
     * Auxiliary method to insert a provided special value using a prepared statement.
     *
     * @param columnValue The column value.
     */
    protected void testInsertSpecialValuesByPS2(final Object columnValue) throws DatabaseEngineException, DatabaseFactoryException, NameAlreadyExistsException, ConnectionResetException {
        final DatabaseEngine engine = DatabaseFactory.getConnection(properties);
        final String PSName = "PS_DUMMY";
        final String preparedStatementQuery = "insert into TEST_DOUBLE_COLUMN(ID, DBL_COL) values (?,?)";
        try {
            final DbEntity entity = createSpecialValuesEntity();
            engine.addEntity(entity);
            engine.createPreparedStatement(PSName, preparedStatementQuery);
            engine.clearParameters(PSName);
            engine.setParameter(PSName, 1, PK_VALUE);
            engine.setParameter(PSName, 2, columnValue);
            engine.executePS(PSName);
            engine.commit();
            checkResult(engine, TABLE_NAME, columnValue);
        } finally {
            engine.close();
        }

    }

    /**
     * Auxiliary method to insert a provided special value using a batch.
     *
     * @param columnValue The column value.
     */
    protected void testInsertSpecialValuesByBatch(final Object columnValue) throws DatabaseFactoryException, DatabaseEngineException {
        final DatabaseEngine engine = DatabaseFactory.getConnection(properties);
        try {
            final DbEntity entity = createSpecialValuesEntity();
            engine.addEntity(entity);
            engine.addBatch(TABLE_NAME, createSpecialValueEntry(columnValue));
            engine.flush();
            engine.commit();
            checkResult(engine, TABLE_NAME, columnValue);
        } finally {
            engine.close();
        }
    }

    /**
     * Auxiliary method that checks that the inserted value is indeed the provided column value.
     *
     * @param engine      The database engine.
     * @param entityName  The entity name to check.
     * @param columnValue The column value persisted in storage.
     */
    protected void checkResult(final DatabaseEngine engine, final String entityName, final Object columnValue) throws DatabaseEngineException {
        if (columnValue instanceof Double) {
            checkResultDouble(engine, entityName, (double) columnValue);
            return;
        }
        final List<Map<String, ResultColumn>> dbl = engine.query(select(column(DBL_COL)).from(table(entityName)));
        final ResultColumn result = dbl.get(0).get(DBL_COL);
        assertTrue("Should be equal to '"+ columnValue +"'. But was: " + result.toString(), result.toString().equals(columnValue));
    }

    /**
     * Auxiliary method that checks that the inserted value is indeed the provided column value.
     *
     * @param engine      The database engine.
     * @param entityName  The entity name to check.
     * @param columnValue The column value persisted in storage.
     */
    protected void checkResultDouble(final DatabaseEngine engine, final String entityName, final double columnValue) throws DatabaseEngineException {
        final List<Map<String, ResultColumn>> dbl = engine.query(select(column(DBL_COL)).from(table(entityName)));
        final ResultColumn result = dbl.get(0).get(DBL_COL);
        assertTrue("Should be equal to '"+ columnValue +"'. But was: " + result.toString(), result.toDouble().equals(columnValue));
    }

    /**
     * Auxiliary method that creates the entity that will receive the special values.
     *
     * @return The {@link DbEntity} to insert the values.
     */
    protected DbEntity createSpecialValuesEntity() {
        return dbEntity()
                .name(TABLE_NAME)
                .addColumn(ID_COL, INT)
                .addColumn(DBL_COL, DOUBLE)
                .pkFields(ID_COL)
                .build();
    }

    /**
     * Auxiliary method that creates the entry to insert in the special values entity.
     *
     * @param columnValue The value to insert.
     * @return The {@link EntityEntry} to insert in storage.
     */
    protected EntityEntry createSpecialValueEntry(final Object columnValue) {
        return entry()
                .set(ID_COL, 1)
                .set(DBL_COL, columnValue)
                .build();
    }

    protected void defineUDFGetOne(DatabaseEngine engine) throws DatabaseEngineException {
    }

    protected void defineUDFTimesTwo(DatabaseEngine engine) throws DatabaseEngineException {
    }
}
