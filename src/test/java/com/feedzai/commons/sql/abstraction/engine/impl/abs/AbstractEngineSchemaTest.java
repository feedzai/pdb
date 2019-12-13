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
import com.feedzai.commons.sql.abstraction.ddl.DbEntityType;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.engine.ConnectionResetException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import com.feedzai.commons.sql.abstraction.engine.NameAlreadyExistsException;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.google.common.collect.Sets;
import mockit.Deencapsulation;
import org.assertj.core.api.MapAssert;
import org.assertj.core.data.MapEntry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.DOUBLE;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.all;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.column;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.entry;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.k;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.select;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.table;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.udf;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineSchemaTest.Ieee754Support.SUPPORTED_STRINGS;
import static com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineSchemaTest.Ieee754Support.UNSUPPORTED;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assume.assumeThat;

/**
 * @author Rafael Marmelo (rafael.marmelo@feedzai.com)
 * @since 2.0.0
 */
public abstract class AbstractEngineSchemaTest {

    /**
     * An enumeration of the level of support of IEEE 754 non-number values a {@link DatabaseEngine} provides.
     *
     * Values in question: {@link Double#NaN}, {@link Double#NEGATIVE_INFINITY} and {@link Double#POSITIVE_INFINITY}.
     */
    public enum Ieee754Support {
        /**
         * IEEE 754 non-number values are unsupported.
         */
        UNSUPPORTED,

        /**
         * IEEE 754 non-number values are supported.
         */
        SUPPORTED,

        /**
         * IEEE 754 non-number values are supported, even when provided as {@link String}
         *
         * Most engines don't support this because the destination column for the values will be numeric, of type
         * {@link com.feedzai.commons.sql.abstraction.ddl.DbColumnType#DOUBLE}.
         */
        SUPPORTED_STRINGS
    }

    private static final String IEE754_SUPPORT_MESSAGE = "Test not supported for this engine - skipped";

    protected Properties properties;

    @Parameterized.Parameter
    public DatabaseConfiguration config;

    private static final String TABLE_NAME = "TEST_DOUBLE_COLUMN";
    private static final String ID_COL = "ID";
    private static final String DBL_COL = "DBL_COL";
    private static final int PK_VALUE = 1;

    @Before
    public void init() {
        properties = new Properties() {
            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "drop-create");
                if (config.schema != null) {
                    setProperty(SCHEMA, config.schema);
                }
            }
        };
    }

    /**
     * This method tells what kind of support the current engine (and the corresponding test class extending this one)
     * has for IEEE 754 non-number values.
     *
     * Default is assuming it is {@link Ieee754Support#UNSUPPORTED}, unless otherwise stated by overriding this method.
     *
     * @return the level of {@link Ieee754Support}.
     */
    protected Ieee754Support getIeee754Support() {
        return UNSUPPORTED;
    }


    /**
     * Gets the test schema to be used in tests that need a schema other then the default/configured.
     *
     * @return The test schema.
     * @since 2.1.13
     */
    protected String getTestSchema() {
        return "myschema";
    }

    /**
     * Tests a query including an UDF {@link com.feedzai.commons.sql.abstraction.dml.Expression},
     * using the default schema.
     *
     * @throws Exception If something goes wrong with the test.
     */
    @Test
    public void udfGetOneTest() throws Exception {
        // engine using the default schema
        try (final DatabaseEngine engine = DatabaseFactory.getConnection(properties)) {
            defineUDFGetOne(engine);

            final List<Map<String, ResultColumn>> query = engine.query(select(udf("GetOne").alias("ONE")));
            assertEquals("result ok?", 1, (int) query.get(0).get("ONE").toInt());
        }
    }

    /**
     * Tests a query including an UDF {@link com.feedzai.commons.sql.abstraction.dml.Expression},
     * using the schema defined in the properties.
     *
     * @throws Exception If something goes wrong with the test.
     */
    @Test
    public void udfTimesTwoTest() throws Exception {
        dropCreateTestSchema();

        final Properties otherProperties = (Properties) properties.clone();
        otherProperties.setProperty(SCHEMA, getTestSchema());

        // engine using the defined test schema
        try (final DatabaseEngine engine = DatabaseFactory.getConnection(otherProperties)) {
            defineUDFTimesTwo(engine);

            final List<Map<String, ResultColumn>> query = engine.query(select(udf("TimesTwo", k(10)).alias("TIMESTWO")));
            assertEquals("result ok?", 20, (int) query.get(0).get("TIMESTWO").toInt());
        }
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testPersistNan() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), is(SUPPORTED_STRINGS));
        testPersistSpecialValues("NaN");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testPersistNanDouble() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), not(UNSUPPORTED));
        testPersistSpecialValues(Double.NaN);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPSNan() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), is(SUPPORTED_STRINGS));
        testInsertSpecialValuesByPS("NaN");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPSNanDouble() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), not(UNSUPPORTED));
        testInsertSpecialValuesByPS(Double.NaN);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPS2Nan() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), is(SUPPORTED_STRINGS));
        testInsertSpecialValuesByPS2("NaN");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPS2NanDouble() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), not(UNSUPPORTED));
        testInsertSpecialValuesByPS2(Double.NaN);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testInsertBatchNan() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), is(SUPPORTED_STRINGS));
        testInsertSpecialValuesByBatch("NaN");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'NaN' should be inserted into the database without any error.
     */
    @Test
    public void testInsertBatchNanDouble() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), not(UNSUPPORTED));
        testInsertSpecialValuesByBatch(Double.NaN);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testPersistInfinity() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), is(SUPPORTED_STRINGS));
        testPersistSpecialValues("Infinity");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testPersistInfinityDouble() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), not(UNSUPPORTED));
        testPersistSpecialValues(Double.POSITIVE_INFINITY);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testPersistInfinityDoubleNegative() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), not(UNSUPPORTED));
        testPersistSpecialValues(Double.NEGATIVE_INFINITY);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPSInfinity() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), is(SUPPORTED_STRINGS));
        testInsertSpecialValuesByPS("Infinity");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPSInfinityDouble() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), not(UNSUPPORTED));
        testInsertSpecialValuesByPS(Double.POSITIVE_INFINITY);
    }


    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPSInfinityDoubleNegative() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), not(UNSUPPORTED));
        testInsertSpecialValuesByPS(Double.NEGATIVE_INFINITY);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPS2Infinity() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), is(SUPPORTED_STRINGS));
        testInsertSpecialValuesByPS2("Infinity");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPS2InfinityDouble() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), not(UNSUPPORTED));
        testInsertSpecialValuesByPS2(Double.POSITIVE_INFINITY);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertPS2InfinityDoubleNegative() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), not(UNSUPPORTED));
        testInsertSpecialValuesByPS2(Double.NEGATIVE_INFINITY);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertBatchInfinity() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), is(SUPPORTED_STRINGS));
        testInsertSpecialValuesByBatch("Infinity");
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertBatchInfinityDouble() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), not(UNSUPPORTED));
        testInsertSpecialValuesByBatch(Double.POSITIVE_INFINITY);
    }

    /**
     * After changing the oracle double data type from DOUBLE PRECISION to BINARY_DOUBLE the special
     * value 'Infinity' should be inserted into the database without any error.
     */
    @Test
    public void testInsertBatchInfinityDoubleNegative() throws Exception {
        assumeThat(IEE754_SUPPORT_MESSAGE, getIeee754Support(), not(UNSUPPORTED));
        testInsertSpecialValuesByBatch(Double.NEGATIVE_INFINITY);
    }

    /**
     * The 'randomString' is not a special value for the BINARY_DOUBLE type so it should throw an error.
     */
    @Test(expected = Exception.class)
    public void testPersistRandomValuesDoNoWorkInBinaryDoubleColumn() throws Exception {
        testPersistSpecialValues("randomString");
    }

    /**
     * The 'randomString' is not a special value for the BINARY_DOUBLE type so it should throw an error.
     */
    @Test(expected = Exception.class)
    public void testInsertPSRandomValuesDoNoWorkInBinaryDoubleColumn() throws Exception {
        testInsertSpecialValuesByPS("randomString");
    }

    /**
     * The 'randomString' is not a special value for the BINARY_DOUBLE type so it should throw an error.
     */
    @Test(expected = Exception.class)
    public void testInsertPS2RandomValuesDoNoWorkInBinaryDoubleColumn() throws Exception {
        testInsertSpecialValuesByPS2("randomString");
    }

    /**
     * The 'randomString' is not a special value for the BINARY_DOUBLE type so it should throw an error.
     */
    @Test(expected = Exception.class)
    public void testInsertBatchRandomValuesDoNoWorkInBinaryDoubleColumn() throws Exception {
        testInsertSpecialValuesByBatch("randomString");
    }

    /**
     * Auxiliary method to persist a provided special value.
     *
     * @param columnValue The column value.
     */
    protected void testPersistSpecialValues(final Object columnValue) throws DatabaseEngineException, DatabaseFactoryException {
        try (final DatabaseEngine engine = DatabaseFactory.getConnection(properties)) {
            final DbEntity entity = createSpecialValuesEntity();
            engine.addEntity(entity);

            final EntityEntry entry = createSpecialValueEntry(columnValue);
            engine.persist(entity.getName(), entry);
            checkResult(engine, entity.getName(), columnValue);
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
        final String ec = engine.escapeCharacter();
        final String preparedStatementQuery = "INSERT INTO " + quotize(TABLE_NAME, ec) +
                "(" + quotize(ID_COL, ec) + ", " + quotize(DBL_COL, ec) + ") VALUES (?,?)";

        try {
            final DbEntity entity = createSpecialValuesEntity();
            engine.addEntity(entity);
            engine.beginTransaction();
            engine.createPreparedStatement(PSName, preparedStatementQuery);
            engine.clearParameters(PSName);
            engine.setParameters(PSName, PK_VALUE, columnValue);
            engine.executePS(PSName);
            engine.commit();
            checkResult(engine, TABLE_NAME, columnValue);

        } finally {
            if (engine.isTransactionActive()) {
                engine.rollback();
            }
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
        final String ec = engine.escapeCharacter();
        final String preparedStatementQuery = "INSERT INTO " + quotize(TABLE_NAME, ec) +
            "(" + quotize(ID_COL, ec) + ", " + quotize(DBL_COL, ec) + ") VALUES (?,?)";

        try {
            final DbEntity entity = createSpecialValuesEntity();
            engine.addEntity(entity);
            engine.beginTransaction();
            engine.createPreparedStatement(PSName, preparedStatementQuery);
            engine.clearParameters(PSName);
            engine.setParameter(PSName, 1, PK_VALUE);
            engine.setParameter(PSName, 2, columnValue);
            engine.executePS(PSName);
            engine.commit();
            checkResult(engine, TABLE_NAME, columnValue);

        } finally {
            if (engine.isTransactionActive()) {
                engine.rollback();
            }
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
            engine.beginTransaction();
            engine.addBatch(TABLE_NAME, createSpecialValueEntry(columnValue));
            engine.flush();
            engine.commit();
            checkResult(engine, TABLE_NAME, columnValue);

        } finally {
            if (engine.isTransactionActive()) {
                engine.rollback();
            }
            engine.close();
        }
    }

    /**
     * Tests that the default option for the ALLOW_COLUMN_DROP option is false.
     *
     * @throws Exception in case something goes wrong in the test.
     * @since 2.1.8
     */
    @Test
    public void testDefaultAllowColumnDrop() throws Exception {
        // copy to make sure we don't have an allow column drop defined
        final Properties defaultAllowColumnDropProperties = new Properties();
        defaultAllowColumnDropProperties.putAll(properties);
        defaultAllowColumnDropProperties.remove(PdbProperties.ALLOW_COLUMN_DROP);
        // use only a create to avoid dropping the table when adding.
        defaultAllowColumnDropProperties.put(PdbProperties.SCHEMA_POLICY, "create");

        final Set<String> result = runAllowColumnDropCheck(defaultAllowColumnDropProperties, false);
        // confirm that column is not dropped because default ALLOW_COLUMN_DROP is false.
        assertEquals("Check that a select star query returns both columns", Sets.newHashSet(ID_COL, DBL_COL), result);
    }

    /**
     * Tests that ALLOW_COLUMN_DROP option {@code true} works as intended.
     *
     * @throws Exception in case something goes wrong in the test.
     * @since 2.5.0
     */
    @Test
    public void testAllowColumnDropTrue() throws Exception {
        // copy to make sure we don't have an allow column drop defined
        final Properties allowColumnDropProperties = new Properties();
        allowColumnDropProperties.putAll(properties);
        allowColumnDropProperties.setProperty(PdbProperties.ALLOW_COLUMN_DROP, Boolean.TRUE.toString());
        // use only a create to avoid dropping the table when adding.
        allowColumnDropProperties.put(PdbProperties.SCHEMA_POLICY, "create");

        final Set<String> result = runAllowColumnDropCheck(allowColumnDropProperties, true);
        // confirm that the column is dropped.
        assertEquals("Check that a select star query returns only ID_COL columns", Sets.newHashSet(ID_COL), result);
    }

    /**
     * Creates a table, inserts data, and does an update on the table/Entity that doesn't have the second column.
     *
     * @param properties              The properties for the engine to use.
     * @param expectedAllowColumnDrop The expected value for "allowColumnDrop".
     * @return a set of columns present in the result of a "SELECT all" query (columns currently present in the table).
     * @throws Exception in case something goes wrong.
     */
    private Set<String> runAllowColumnDropCheck(final Properties properties,
                                                final boolean expectedAllowColumnDrop) throws Exception {
        final DatabaseEngine engine = DatabaseFactory.getConnection(properties);

        assertEquals("allowColumnDrop doesn't have the expected value",
                expectedAllowColumnDrop, engine.getProperties().allowColumnDrop());

        // guarantee that is deleted and doesn't come from previous tests.
        final DbEntity entity = createSpecialValuesEntity();
        engine.updateEntity(entity);
        engine.dropEntity(entity);
        engine.addEntity(entity);

        try {
            engine.beginTransaction();
            engine.addBatch(TABLE_NAME, createSpecialValueEntry(10));
            engine.flush();
            engine.commit();
            checkResult(engine, TABLE_NAME, 10d);

            engine.removeEntity(TABLE_NAME);
            engine.updateEntity(dbEntity().name(TABLE_NAME).addColumn(ID_COL, INT).pkFields(ID_COL).build());

            return engine.query(
                    select(all())
                            .from(table(TABLE_NAME))
                            .limit(1)
            ).get(0).keySet();

        } finally {
            // drop the entity, not needed anymore
            engine.dropEntity(entity);

            if (engine.isTransactionActive()) {
                engine.rollback();
            }
            engine.close();
        }
    }

    /**
     * Tests that an entity can be created/dropped/checked in different schemas without conflicts, and the operation
     * works as expected.
     *
     * @throws DatabaseEngineException  If anything goes wrong with database operations.
     * @throws DatabaseFactoryException If anything goes wrong connecting to the database.
     * @since 2.1.13
     */
    @Test
    public void testCreateSameEntityDifferentSchemas() throws DatabaseEngineException, DatabaseFactoryException {
        assertNotEquals("Other 'test schema' and original schema should be different", config.schema, getTestSchema());

        final DbEntity entity = dbEntity()
            .name("TEST1")
            .addColumn("COL1", INT)
            .pkFields("COL1")
            .build();

        final DbEntity otherEntity = dbEntity()
            .name("TEST2")
            .addColumn("COL1", INT)
            .pkFields("COL1")
            .build();

        dropCreateTestSchema();

        final Properties otherProperties = (Properties) properties.clone();
        otherProperties.setProperty(SCHEMA, getTestSchema());

        try (final DatabaseEngine originalEngine = DatabaseFactory.getConnection(properties);
             final DatabaseEngine otherEngine = DatabaseFactory.getConnection(otherProperties)) {
            originalEngine.dropEntity(entity);
            originalEngine.dropEntity(otherEntity);

            checkEntity("* Asserting test1 table is not present in any schema...",
                entity, originalEngine, otherEngine, false, false);

            originalEngine.addEntity(entity);
            checkEntity("* Created test1 table in original schema - asserting it is there and not in the other schema...",
                entity, originalEngine, otherEngine, true, false);

            otherEngine.addEntity(entity);
            checkEntity("* Created test1 table in other schema - asserting it is there and still is in the original schema...",
                entity, originalEngine, otherEngine, true, true);

            otherEngine.addEntity(otherEntity);
            checkEntity("* Created test2 table in other schema - asserting it is there and not in the original schema...",
                otherEntity, originalEngine, otherEngine, false, true);

            otherEngine.dropEntity(entity);
            checkEntity("* Dropped test1 table in other schema - asserting it is not there, but is still in the original schema...",
                entity, originalEngine, otherEngine, true, false);

            originalEngine.dropEntity(entity);
            checkEntity("* Dropped test1 table in original schema - asserting it is not present in any schema...",
                entity, originalEngine, otherEngine, false, false);
        }
    }

    private static void checkEntity(final String reasonMessage, final DbEntity entity,
                                    final DatabaseEngine originalEngine,
                                    final DatabaseEngine otherEngine,
                                    final boolean isPresentOriginal,
                                    final boolean isPresentOther) throws DatabaseEngineException {
        final String name = entity.getName();
        final MapEntry[] tableEntry = {
                MapEntry.entry(name, DbEntityType.TABLE),
                // this second entry is needed because MySQL returns tables as SYSTEM_TABLEs (when they are created in default schema)
                MapEntry.entry(name, DbEntityType.SYSTEM_TABLE)
        };
        final String originalSchema = Deencapsulation.getField(originalEngine, "currentSchema");
        final String otherSchema = Deencapsulation.getField(otherEngine, "currentSchema");

        // check metadata in the schema of the "original engine"
        final MapAssert<String, DbColumnType> originalMetadataAssert = assertThat(originalEngine.getMetadata(name))
                .as("%s%n--> Metadata for table '%s' should%s be present in original schema.",
                        reasonMessage, name, isPresentOriginal ? "" : " not");

        final MapAssert<String, DbColumnType> originalMetadataAssertOther = assertThat(otherEngine.getMetadata(originalSchema, name))
                .as("%s%n--> Metadata for table '%s' when checked from other engine should match metadata checked from original engine.",
                        reasonMessage, name);

        // check entities in the schema of the "original engine"
        final MapAssert<String, DbEntityType> originalEntitiesAssert = assertThat(originalEngine.getEntities())
                .as("%s%n--> Table '%s' should%s be present in original schema.",
                        reasonMessage, name, isPresentOriginal ? "" : " not");

        final MapAssert<String, DbEntityType> originalEntitiesAssertOther = assertThat(otherEngine.getEntities(originalSchema))
                .as("%s%n--> The presence of table '%s' when checked from other engine should match the info obtained from original engine.",
                        reasonMessage, name);

        if (isPresentOriginal) {
            originalMetadataAssert.hasSize(1);
            originalMetadataAssertOther.containsOnly(originalEngine.getMetadata(name).entrySet().toArray(new Map.Entry[0]));
            originalEntitiesAssert.containsAnyOf(tableEntry);
            originalEntitiesAssertOther.containsOnly(originalEngine.getEntities().entrySet().toArray(new Map.Entry[0]));
        } else {
            originalMetadataAssert.isEmpty();
            originalMetadataAssertOther.isEmpty();
            originalEntitiesAssert.doesNotContain(tableEntry);
            originalEntitiesAssertOther.doesNotContain(tableEntry);
        }

        // check metadata in the schema of the "other engine"
        final MapAssert<String, DbColumnType> otherMetadataAssert = assertThat(otherEngine.getMetadata(name))
                .as("%s%n--> Metadata for table '%s' should%s be present in the other schema.",
                        reasonMessage, name, isPresentOther ? "" : " not");

        final MapAssert<String, DbColumnType> otherMetadataAssertOther = assertThat(originalEngine.getMetadata(otherSchema, name))
                .as("%s%n--> Metadata for table '%s' when checked from original engine should match metadata checked from other engine.",
                        reasonMessage, name);

        // check entities in the schema of the "other engine"
        final MapAssert<String, DbEntityType> otherEntitiesAssert = assertThat(otherEngine.getEntities())
                .as("%s%n--> Table '%s' should%s be present in other schema.",
                        reasonMessage, name, isPresentOther ? "" : " not");

        final MapAssert<String, DbEntityType> otherEntitiesAssertOther = assertThat(originalEngine.getEntities(otherSchema))
                .as("%s%n--> The presence of table '%s' when checked from original engine should match the info obtained from other engine.",
                        reasonMessage, name);

        if (isPresentOther) {
            otherMetadataAssert.hasSize(1);
            otherMetadataAssertOther.containsOnly(otherEngine.getMetadata(name).entrySet().toArray(new Map.Entry[0]));
            otherEntitiesAssert.containsAnyOf(tableEntry);
            otherEntitiesAssertOther.containsOnly(otherEngine.getEntities().entrySet().toArray(new Map.Entry[0]));
        } else {
            otherMetadataAssert.isEmpty();
            otherMetadataAssertOther.isEmpty();
            otherEntitiesAssert.doesNotContain(tableEntry);
            otherEntitiesAssertOther.doesNotContain(tableEntry);
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
        assertEquals("Should be equal to '" + columnValue + "'. But was: " + result.toString(), columnValue, result.toString());
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
        assertEquals("Should be equal to '" + columnValue + "'. But was: " + result.toString(), columnValue, result.toDouble(), 0);
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

    /**
     * Defines the UDF "GetOne" in the database engine.
     *
     * @param engine The database engine.
     * @throws DatabaseEngineException If anything goes wrong creating the UDF.
     */
    protected void defineUDFGetOne(final DatabaseEngine engine) throws DatabaseEngineException {
    }

    /**
     * Defines the UDF "TimesTwo" in the database engine.
     *
     * This UDF is supposed to be created in the {@link #getTestSchema() test schema} (if the database supports it).
     *
     * @param engine The database engine.
     * @throws DatabaseEngineException If anything goes wrong creating the UDF.
     */
    protected void defineUDFTimesTwo(final DatabaseEngine engine) throws DatabaseEngineException {
    }

    /**
     * Creates the {@link #getTestSchema() test schema} in the database, dropping it first if necessary.
     *
     * @throws DatabaseEngineException  If anything goes wrong creating the schema.
     * @throws DatabaseFactoryException If anything goes wrong connecting to the database.
     * @since 2.1.13
     */
    public void dropCreateTestSchema() throws DatabaseEngineException, DatabaseFactoryException {
        try (final DatabaseEngine engine = DatabaseFactory.getConnection(properties)) {
            dropSchema(engine, getTestSchema());
            createSchema(engine, getTestSchema());
        }
    }

    /**
     * Creates a schema in the database.
     *
     * This method is expected to create the necessary permissions for the current user and/or the user associated
     * with the schema to login and create tables in it.
     *
     * @param engine The database engine.
     * @param schema The schema to create.
     * @throws DatabaseEngineException If anything goes wrong creating the schema.
     * @since 2.1.13
     */
    protected abstract void createSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException;

    /**
     * Drops a schema from the database (won't fail if schema doesn't exist or if it isn't empty).
     *
     * @param engine The database engine.
     * @param schema The schema to drop.
     * @throws DatabaseEngineException If anything goes wrong dropping the schema.
     * @since 2.1.13
     */
    protected abstract void dropSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException;
}
