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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.collect.ImmutableSet;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import mockit.Expectations;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Verifications;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import com.feedzai.commons.sql.abstraction.ddl.AlterColumn;
import com.feedzai.commons.sql.abstraction.ddl.DbColumn;
import com.feedzai.commons.sql.abstraction.ddl.DbColumnConstraint;
import com.feedzai.commons.sql.abstraction.ddl.DbColumnType;
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.ddl.Rename;
import com.feedzai.commons.sql.abstraction.dml.Expression;
import com.feedzai.commons.sql.abstraction.dml.K;
import com.feedzai.commons.sql.abstraction.dml.Query;
import com.feedzai.commons.sql.abstraction.dml.Truncate;
import com.feedzai.commons.sql.abstraction.dml.Update;
import com.feedzai.commons.sql.abstraction.dml.Values;
import com.feedzai.commons.sql.abstraction.dml.With;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.dml.result.ResultIterator;
import com.feedzai.commons.sql.abstraction.engine.AbstractDatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.ConnectionResetException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import com.feedzai.commons.sql.abstraction.engine.MappedEntity;
import com.feedzai.commons.sql.abstraction.engine.NameAlreadyExistsException;
import com.feedzai.commons.sql.abstraction.engine.OperationNotSupportedRuntimeException;
import com.feedzai.commons.sql.abstraction.engine.impl.cockroach.SkipTestCockroachDB;
import com.feedzai.commons.sql.abstraction.engine.testconfig.BlobTest;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnConstraint.NOT_NULL;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.BLOB;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.BOOLEAN;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.CLOB;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.DOUBLE;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.LONG;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.STRING;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.L;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.all;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.avg;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.between;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.caseWhen;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.cast;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.ceiling;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.coalesce;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.column;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.concat;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.count;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.createView;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbColumn;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbFk;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.delete;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.div;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dropPK;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.entry;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.eq;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.f;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.floor;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.in;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.k;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.like;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.lit;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.lower;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.max;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.min;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.mod;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.neq;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.notBetween;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.or;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.select;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.stddev;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.stringAgg;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.sum;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.table;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.udf;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.union;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.update;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.upper;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.values;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.with;
import static com.feedzai.commons.sql.abstraction.engine.EngineTestUtils.buildEntity;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class EngineGeneralTest {


    private static final double DELTA = 1e-7;

    protected DatabaseEngine engine;
    protected Properties properties;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations();
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

    @BeforeClass
    public static void initStatic() {
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.TRACE);

    }

    @Before
    public void init() throws DatabaseFactoryException {
        properties = new Properties() {

            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "drop-create");
            }
        };

        engine = DatabaseFactory.getConnection(properties);
    }

    @After
    public void cleanup() {
        engine.close();
    }

    @Test
    public void createEntityTest() throws DatabaseEngineException {

        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1")
                .build();

        engine.addEntity(entity);
    }

    @Test
    public void createEntityWithTwoColumnsBeingPKTest() throws DatabaseEngineException {

        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1", "COL3")
                .build();

        engine.addEntity(entity);
    }

    @Test(expected = DatabaseEngineException.class)
    public void createEntityAlreadyExistsTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1", "COL3")
                .build();

        engine.addEntity(entity);

        try {
            engine.addEntity(entity);
        } catch (final DatabaseEngineException e) {
            assertEquals("", "Entity 'TEST' is already defined", e.getMessage());
            throw e;
        }
    }

    @Test
    public void createUniqueIndexTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1", "COL3")
                .addIndex(true, "COL4")
                .build();

        engine.addEntity(entity);
    }

    @Test
    public void createIndexWithTwoColumnsTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1", "COL3")
                .addIndex("COL4", "COL3")
                .build();

        engine.addEntity(entity);
    }

    @Test
    public void createTwoIndexesTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1", "COL3")
                .addIndex("COL4")
                .addIndex("COL3")
                .build();

        engine.addEntity(entity);
    }

    @Test
    public void createEntityWithTheSameNameButLowerCasedTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1", "COL3")
                .build();

        engine.addEntity(entity);

        DbEntity entity2 = dbEntity()
                .name("test")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1", "COL3")
                .build();

        engine.addEntity(entity2);

    }

    @Test
    public void createEntityWithSequencesTest() throws DatabaseEngineException {

        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT, true)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1")
                .build();

        engine.addEntity(entity);
    }

    @Test
    public void createEntityWithIndexesTest() throws DatabaseEngineException {

        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT, true)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .addIndex("COL4")
                .pkFields("COL1")
                .build();

        engine.addEntity(entity);
    }

    @Test
    public void insertWithControlledTransactionTest() throws Exception {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        EntityEntry entry = entry().set("COL1", 2).set("COL2", false).set("COL3", 2D).set("COL4", 3L).set("COL5", "ADEUS").build();

        engine.beginTransaction();

        try {

            engine.persist("TEST", entry);
            engine.commit();
        } finally {
            if (engine.isTransactionActive()) {
                engine.rollback();
            }
        }

        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")));

        assertTrue("COL1 exists", query.get(0).containsKey("COL1"));
        assertEquals("COL1 ok?", 2, (int) query.get(0).get("COL1").toInt());

        assertTrue("COL2 exists", query.get(0).containsKey("COL2"));
        assertFalse("COL2 ok?", query.get(0).get("COL2").toBoolean());

        assertTrue("COL3 exists", query.get(0).containsKey("COL3"));
        assertEquals("COL3 ok?", 2D, query.get(0).get("COL3").toDouble(), 0);

        assertTrue("COL4 exists", query.get(0).containsKey("COL4"));
        assertEquals("COL4 ok?", 3L, (long) query.get(0).get("COL4").toLong());

        assertTrue("COL5 exists", query.get(0).containsKey("COL5"));
        assertEquals("COL5  ok?", "ADEUS", query.get(0).get("COL5").toString());
    }

    @Test
    public void insertWithAutoCommitTest() throws Exception {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        EntityEntry entry = entry().set("COL1", 2).set("COL2", false).set("COL3", 2D).set("COL4", 3L).set("COL5", "ADEUS")
                .build();

        engine.persist("TEST", entry);

        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")));

        assertTrue("COL1 exists", query.get(0).containsKey("COL1"));
        assertEquals("COL1 ok?", 2, (int) query.get(0).get("COL1").toInt());

        assertTrue("COL2 exists", query.get(0).containsKey("COL2"));
        assertFalse("COL2 ok?", query.get(0).get("COL2").toBoolean());

        assertTrue("COL3 exists", query.get(0).containsKey("COL3"));
        assertEquals("COL3 ok?", 2D, query.get(0).get("COL3").toDouble(), 0);

        assertTrue("COL4 exists", query.get(0).containsKey("COL4"));
        assertEquals("COL4 ok?", 3L, (long) query.get(0).get("COL4").toLong());

        assertTrue("COL5 exists", query.get(0).containsKey("COL5"));
        assertEquals("COL5  ok?", "ADEUS", query.get(0).get("COL5").toString());
    }

    @Test
    public void insertWithControlledTransactionUsingSequenceTest() throws Exception {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT, true)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        EntityEntry entry = entry().set("COL2", false).set("COL3", 2D).set("COL4", 3L).set("COL5", "ADEUS")
                .build();

        engine.beginTransaction();

        try {

            engine.persist("TEST", entry);
            engine.commit();
        } finally {
            if (engine.isTransactionActive()) {
                engine.rollback();
            }
        }
        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")));

        assertTrue("COL1 exists", query.get(0).containsKey("COL1"));
        assertEquals("COL1 ok?", 1, (int) query.get(0).get("COL1").toInt());

        assertTrue("COL2 exists", query.get(0).containsKey("COL2"));
        assertFalse("COL2 ok?", query.get(0).get("COL2").toBoolean());

        assertTrue("COL3 exists", query.get(0).containsKey("COL3"));
        assertEquals("COL3 ok?", 2D, query.get(0).get("COL3").toDouble(), 0);

        assertTrue("COL4 exists", query.get(0).containsKey("COL4"));
        assertEquals("COL4 ok?", 3L, (long) query.get(0).get("COL4").toLong());

        assertTrue("COL5 exists", query.get(0).containsKey("COL5"));
        assertEquals("COL5  ok?", "ADEUS", query.get(0).get("COL5").toString());
    }

    @Test
    public void queryWithIteratorWithDataTest() throws Exception {
        test5Columns();

        EntityEntry entry = entry().set("COL1", 1).set("COL2", false).set("COL3", 2D).set("COL4", 3L).set("COL5", "ADEUS")
                .build();
        engine.persist("TEST", entry);

        ResultIterator it = engine.iterator(select(all()).from(table("TEST")));

        Map<String, ResultColumn> res;
        res = it.next();
        assertNotNull("result is not null", res);
        assertTrue("COL1 exists", res.containsKey("COL1"));
        assertEquals("COL1 ok?", 1, (int) res.get("COL1").toInt());

        assertTrue("COL2 exists", res.containsKey("COL2"));
        assertFalse("COL2 ok?", res.get("COL2").toBoolean());

        assertTrue("COL3 exists", res.containsKey("COL3"));
        assertEquals("COL3 ok?", 2D, res.get("COL3").toDouble(), 0);

        assertTrue("COL4 exists", res.containsKey("COL4"));
        assertEquals("COL4 ok?", 3L, (long) res.get("COL4").toLong());

        assertTrue("COL5 exists", res.containsKey("COL5"));
        assertEquals("COL5  ok?", "ADEUS", res.get("COL5").toString());

        assertNull("no more data to consume?", it.next());

        assertTrue("result set is closed?", it.isClosed());
        assertNull("next on a closed result set must return null", it.next());

        // calling close on a closed result set has no effect.
        it.close();
    }

    @Test
    public void queryWithIteratorWithNoDataTest() throws Exception {
        test5Columns();

        ResultIterator it = engine.iterator(select(all()).from(table("TEST")));

        assertNull("result is null", it.next());

        assertNull("no more data to consume?", it.next());

        assertTrue("result set is closed?", it.isClosed());
        assertNull("next on a closed result set must return null", it.next());

        // calling close on a closed result set has no effect.
        it.close();
    }

    /**
     * Tests that an iterator created in a try-with-resources' resource specification header is automatically closed
     * once the block is exited from.
     *
     * @throws Exception If an unexpected error occurs.
     *
     * @since 2.1.12
     */
    @Test
    public void queryWithIteratorInTryWithResources() throws Exception {
        test5Columns();

        final EntityEntry entry = entry()
                .set("COL1", 1)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS")
                .build();
        engine.persist("TEST", entry);

        final ResultIterator resultIterator;
        try (final ResultIterator it = engine.iterator(select(all()).from(table("TEST")))) {

            resultIterator = it;

            assertFalse(
                    "Result iterator should not be closed before exiting try-with-resources block",
                    resultIterator.isClosed()
            );
        }

        assertTrue(
                "Result iterator should be closed after exiting try-with-resources block",
                resultIterator.isClosed()
        );
    }

    @Test
    public void batchInsertTest() throws Exception {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        engine.beginTransaction();

        try {
            EntityEntry entry = entry().set("COL1", 2).set("COL2", false).set("COL3", 2D).set("COL4", 3L).set("COL5", "ADEUS")
                    .build();

            engine.addBatch("TEST", entry);

            entry = entry().set("COL1", 3).set("COL2", true).set("COL3", 3D).set("COL4", 4L).set("COL5", "OLA")
                    .build();

            engine.addBatch("TEST", entry);

            engine.flush();

            engine.commit();
        } finally {
            if (engine.isTransactionActive()) {
                engine.rollback();
            }
        }

        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")).orderby(column("COL1").asc()));

        // 1st
        assertTrue("COL1 exists", query.get(0).containsKey("COL1"));
        assertEquals("COL1 ok?", 2, (int) query.get(0).get("COL1").toInt());

        assertTrue("COL2 exists", query.get(0).containsKey("COL2"));
        assertFalse("COL2 ok?", query.get(0).get("COL2").toBoolean());

        assertTrue("COL3 exists", query.get(0).containsKey("COL3"));
        assertEquals("COL3 ok?", 2D, query.get(0).get("COL3").toDouble(), 0);

        assertTrue("COL4 exists", query.get(0).containsKey("COL4"));
        assertEquals("COL4 ok?", 3L, (long) query.get(0).get("COL4").toLong());

        assertTrue("COL5 exists", query.get(0).containsKey("COL5"));
        assertEquals("COL5  ok?", "ADEUS", query.get(0).get("COL5").toString());

        // 2nd

        assertTrue("COL1 exists", query.get(1).containsKey("COL1"));
        assertEquals("COL1 ok?", 3, (int) query.get(1).get("COL1").toInt());

        assertTrue("COL2 exists", query.get(1).containsKey("COL2"));
        assertTrue("COL2 ok?", query.get(1).get("COL2").toBoolean());

        assertTrue("COL3 exists", query.get(1).containsKey("COL3"));
        assertEquals("COL3 ok?", 3D, query.get(1).get("COL3").toDouble(), 0);

        assertTrue("COL4 exists", query.get(1).containsKey("COL4"));
        assertEquals("COL4 ok?", 4L, (long) query.get(1).get("COL4").toLong());

        assertTrue("COL5 exists", query.get(1).containsKey("COL5"));
        assertEquals("COL5  ok?", "OLA", query.get(1).get("COL5").toString());
    }

    @Test
    public void batchInsertAutocommitTest() throws Exception {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        EntityEntry entry = entry().set("COL1", 2).set("COL2", false).set("COL3", 2D).set("COL4", 3L).set("COL5", "ADEUS")
                .build();

        engine.addBatch("TEST", entry);

        entry = entry().set("COL1", 3).set("COL2", true).set("COL3", 3D).set("COL4", 4L).set("COL5", "OLA")
                .build();

        engine.addBatch("TEST", entry);

        // autocommit set to true.
        engine.flush();


        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")).orderby(column("COL1").asc()));

        // 1st
        assertTrue("COL1 exists", query.get(0).containsKey("COL1"));
        assertEquals("COL1 ok?", 2, (int) query.get(0).get("COL1").toInt());

        assertTrue("COL2 exists", query.get(0).containsKey("COL2"));
        assertFalse("COL2 ok?", query.get(0).get("COL2").toBoolean());

        assertTrue("COL3 exists", query.get(0).containsKey("COL3"));
        assertEquals("COL3 ok?", 2D, query.get(0).get("COL3").toDouble(), 0);

        assertTrue("COL4 exists", query.get(0).containsKey("COL4"));
        assertEquals("COL4 ok?", 3L, (long) query.get(0).get("COL4").toLong());

        assertTrue("COL5 exists", query.get(0).containsKey("COL5"));
        assertEquals("COL5  ok?", "ADEUS", query.get(0).get("COL5").toString());

        // 2nd

        assertTrue("COL1 exists", query.get(1).containsKey("COL1"));
        assertEquals("COL1 ok?", 3, (int) query.get(1).get("COL1").toInt());

        assertTrue("COL2 exists", query.get(1).containsKey("COL2"));
        assertTrue("COL2 ok?", query.get(1).get("COL2").toBoolean());

        assertTrue("COL3 exists", query.get(1).containsKey("COL3"));
        assertEquals("COL3 ok?", 3D, query.get(1).get("COL3").toDouble(), 0);

        assertTrue("COL4 exists", query.get(1).containsKey("COL4"));
        assertEquals("COL4 ok?", 4L, (long) query.get(1).get("COL4").toLong());

        assertTrue("COL5 exists", query.get(1).containsKey("COL5"));
        assertEquals("COL5  ok?", "OLA", query.get(1).get("COL5").toString());
    }

    /**
     * Tests that on a rollback situation, the prepared statement batches are cleared.
     *
     * The steps performed on this test are:
     * <ol>
     *     <li>Add batch to transaction and purposely fail to flush</li>
     *     <li>Ensure the existence of the Exception and rollback transaction</li>
     *     <li>Flush again successfully and ensure that the DB table doesn't have any rows</li>
     * </ol>
     *
     * This is a regression test.
     *
     * @throws DatabaseEngineException If there is a problem on {@link DatabaseEngine} operations.
     * @since 2.1.12
     */
    @Test
    public void batchInsertRollback() throws DatabaseEngineException {
        final CountDownLatch latch = new CountDownLatch(1);

        final DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .build();

        new MockUp<AbstractDatabaseEngine>() {
            @Mock
            public synchronized void flush(final Invocation invocation) throws DatabaseEngineException {
                if (latch.getCount() == 1) {
                    throw new DatabaseEngineException("");
                }
                invocation.proceed();
            }
        };

        DatabaseEngineException expectedException = null;

        engine.addEntity(entity);
        engine.beginTransaction();

        try {
            final EntityEntry entry = entry().set("COL1", 1).build();

            engine.addBatch("TEST", entry);
            engine.flush();
            fail("Was expecting the flush operation to fail");
        } catch (final DatabaseEngineException e) {
            expectedException = e;
        } finally {
            if (engine.isTransactionActive()) {
                engine.rollback();
            }
        }

        // Ensure we had an exception and therefore we didn't insert anything on the DB and that we cleared the batches.
        assertNotNull("DB returned exception when flushing", expectedException);

        latch.countDown();
        engine.beginTransaction();
        engine.flush();
        engine.commit();

        final List<Map<String, ResultColumn>> query = engine.query(select(all())
                                                                           .from(table("TEST"))
                                                                           .orderby(column("COL1").asc()));

        // Previously, we rolled back the transaction; now we are trying the flush an empty transaction.
        // Therefore, we shouldn't have any rows on the table.
        assertEquals("There are no rows on table TEST", 0, query.size());
    }

    @Test
    public void blobTest() throws DatabaseEngineException {
        final double[] original = new double[]{5, 6, 7};
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BLOB)
                .build();

        engine.addEntity(entity);
        EntityEntry entry = entry()
                .set("COL1", 2)
                .set("COL2", original)
                .build();

        engine.persist("TEST", entry);

        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")));

        int i = 0;
        for (double d : original) {
            assertEquals("arrays are equal?", d, query.get(0).get("COL2").<double[]>toBlob()[i++], 0D);
        }
    }

    @Test
    public void limitNumberOfRowsTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS");

        for (int i = 0; i < 10; i++) {
            entry.set("COL1", i);
            engine.persist("TEST", entry
                    .build());
        }
        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")).limit(5));

        assertEquals("number of rows ok?", 5, query.size());
    }

    @Test
    public void limitAndOffsetNumberOfRowsTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS");

        for (int i = 0; i < 20; i++) {
            entry.set("COL1", i);
            engine.persist("TEST", entry
                    .build());
        }

        int limit = 5;
        int offset = 7;
        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")).limit(limit).offset(offset));
        assertEquals("number of rows ok?", limit, query.size());
        for (int i = offset, j = 0; i < offset + limit; i++, j++) {
            assertEquals("Check correct row", i, query.get(j).get("COL1").toInt().intValue());
        }
    }

    @Test
    public void limitOffsetAndOrderNumberOfRowsTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .addColumn("COL6", INT)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS")
                .set("COL6", 20);

        for (int i = 0; i < 20; i++) {
            entry.set("COL1", i);
            entry.set("COL6", 20 - i);
            engine.persist("TEST", entry
                    .build());
        }

        int limit = 5;
        int offset = 7;
        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")).limit(limit).offset(offset).orderby(column("COL6").asc()));
        assertEquals("number of rows ok?", limit, query.size());
        for (int i = offset, j = 0; i < offset + limit; i++, j++) {
            assertEquals("Check correct row col1", 19 - i, query.get(j).get("COL1").toInt().intValue());
            assertEquals("Check correct row col6", i + 1, query.get(j).get("COL6").toInt().intValue());
        }
    }

    @Test
    public void limitOffsetAndOrder2NumberOfRowsTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", STRING)
                .addColumn("COL3", INT)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder entry = entry()
                .set("COL1", 0)
                .set("COL2", "A")
                .set("COL3", 6);
        engine.persist("TEST", entry.build());


        entry.set("COL1", 1);
        entry.set("COL2", "B");
        entry.set("COL3", 5);
        engine.persist("TEST", entry.build());

        entry.set("COL1", 2);
        entry.set("COL2", "C");
        entry.set("COL3", 4);
        engine.persist("TEST", entry.build());

        entry.set("COL1", 3);
        entry.set("COL2", "D");
        entry.set("COL3", 3);
        engine.persist("TEST", entry.build());

        entry.set("COL1", 4);
        entry.set("COL2", "E");
        entry.set("COL3", 2);
        engine.persist("TEST", entry.build());

        entry.set("COL1", 5);
        entry.set("COL2", "F");
        entry.set("COL3", 1);
        engine.persist("TEST", entry.build());

        entry.set("COL1", 6);
        entry.set("COL2", "G");
        entry.set("COL3", 0);
        engine.persist("TEST", entry.build());

        int limit = 2;
        int offset = 3;
        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")).limit(limit).offset(offset));
        assertEquals("number of rows ok?", limit, query.size());

        assertEquals("Check correct row col2", "D", query.get(0).get("COL2").toString());
        assertEquals("Check correct row col2", "E", query.get(1).get("COL2").toString());

        query = engine.query(select(all()).from(table("TEST")).limit(limit).offset(offset).orderby(column("COL2").desc()));
        assertEquals("number of rows ok?", limit, query.size());

        assertEquals("Check correct row col2", "D", query.get(0).get("COL2").toString());
        assertEquals("Check correct row col2", "C", query.get(1).get("COL2").toString());
    }

    @Test
    public void offsetLessThanZero() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .addColumn("COL6", INT)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS")
                .set("COL6", 20);

        for (int i = 0; i < 20; i++) {
            entry.set("COL1", i);
            entry.set("COL6", 20 - i);
            engine.persist("TEST", entry.build());
        }

        int limit = 5;
        int offset = -1;
        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")).limit(limit).offset(offset).orderby(column("COL6").asc()));
        assertEquals("number of rows ok?", limit, query.size());
        for (int i = 0, j = 0; i < 5; i++, j++) {
            assertEquals("Check correct row col1", 19 - i, query.get(j).get("COL1").toInt().intValue());
            assertEquals("Check correct row col6", i + 1, query.get(j).get("COL6").toInt().intValue());
        }
    }

    @Test
    public void offsetBiggerThanSize() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .addColumn("COL6", INT)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS")
                .set("COL6", 20);

        for (int i = 0; i < 20; i++) {
            entry.set("COL1", i);
            entry.set("COL6", 20 - i);
            engine.persist("TEST", entry
                    .build());
        }

        int limit = 5;
        int offset = 20;
        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")).limit(limit).offset(offset));
        assertEquals("number of rows ok?", 0, query.size());
    }

    @Test
    public void limitZeroOrNegative() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .addColumn("COL6", INT)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS")
                .set("COL6", 20);

        for (int i = 0; i < 20; i++) {
            entry.set("COL1", i);
            entry.set("COL6", 20 - i);
            engine.persist("TEST", entry
                    .build());
        }

        int limit = 0;
        int offset = 1;
        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")).limit(limit).offset(offset));
        assertEquals("number of rows ok?", 19, query.size());

        limit = -1;
        offset = 1;
        query = engine.query(select(all()).from(table("TEST")).limit(limit).offset(offset));
        assertEquals("number of rows ok?", 19, query.size());
    }

    @Test
    public void offsetOnlyNumberOfRowsTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .addColumn("COL6", INT)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS")
                .set("COL6", 2);

        for (int i = 0; i < 20; i++) {
            entry.set("COL1", i);
            entry.set("COL6", 20 - i);
            engine.persist("TEST", entry
                    .build());
        }

        int offset = 7;
        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")).offset(offset));
        assertEquals("number of rows ok?", 20 - offset, query.size());
        for (int i = offset, j = 0; i < 20; i++, j++) {
            assertEquals("Check correct row 1", i, query.get(j).get("COL1").toInt().intValue());
        }

        query = engine.query(select(all()).from(table("TEST")).offset(offset).orderby(column("COL6").asc()));
        assertEquals("number of rows ok?", 20 - offset, query.size());
        for (int i = offset, j = 0; i < 20; i++, j++) {
            assertEquals("Check correct row 6", offset + 1 + j, query.get(j).get("COL6").toInt().intValue());
        }
    }

    @Test
    public void stddevTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS");

        for (int i = 0; i < 10; i++) {
            entry.set("COL1", i);
            engine.persist("TEST", entry
                    .build());
        }
        List<Map<String, ResultColumn>> query = engine.query(select(stddev(column("COL1")).alias("STDDEV")).from(table("TEST")));

        assertEquals("result ok?", 3.0276503540974917D, query.get(0).get("STDDEV").toDouble(), 0.0001D);
    }

    @Test
    public void sumTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS");

        for (int i = 0; i < 10; i++) {
            entry.set("COL1", i);
            engine.persist("TEST", entry
                    .build());
        }
        List<Map<String, ResultColumn>> query = engine.query(select(sum(column("COL1")).alias("SUM")).from(table("TEST")));

        assertEquals("result ok?", 45, (int) query.get(0).get("SUM").toInt());
    }

    @Test
    public void countTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS");

        for (int i = 0; i < 10; i++) {
            entry.set("COL1", i);
            engine.persist("TEST", entry
                    .build());
        }
        List<Map<String, ResultColumn>> query = engine.query(select(count(column("COL1")).alias("COUNT")).from(table("TEST")));

        assertEquals("result ok?", 10, (int) query.get(0).get("COUNT").toInt());
    }

    @Test
    public void avgTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS");

        for (int i = 0; i < 10; i++) {
            entry.set("COL1", i);
            engine.persist("TEST", entry
                    .build());
        }
        List<Map<String, ResultColumn>> query = engine.query(select(avg(column("COL1")).alias("AVG")).from(table("TEST")));

        assertEquals("result ok?", 4.5D, query.get(0).get("AVG").toDouble(), 0);
    }

    @Test
    public void maxTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS");

        for (int i = 0; i < 10; i++) {
            entry.set("COL1", i);
            engine.persist("TEST", entry
                    .build());
        }
        List<Map<String, ResultColumn>> query = engine.query(select(max(column("COL1")).alias("MAX")).from(table("TEST")));

        assertEquals("result ok?", 9, (int) query.get(0).get("MAX").toInt());
    }

    @Test
    public void minTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS");

        for (int i = 0; i < 10; i++) {
            entry.set("COL1", i);
            engine.persist("TEST", entry
                    .build());
        }
        List<Map<String, ResultColumn>> query = engine.query(select(min(column("COL1")).alias("MIN")).from(table("TEST")));

        assertEquals("result ok?", 0, (int) query.get(0).get("MIN").toInt());
    }

    @Test
    public void floorTest() throws DatabaseEngineException {
        test5Columns();

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2.5D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS");

        for (int i = 0; i < 10; i++) {
            entry.set("COL1", i);
            engine.persist("TEST", entry
                    .build());
        }

        List<Map<String, ResultColumn>> query = engine.query(select(floor(column("COL3")).alias("FLOOR")).from(table("TEST")));

        assertEquals("result ok?", 2.0, query.get(0).get("FLOOR").toDouble(), DELTA);
    }

    @Test
    public void ceilingTest() throws DatabaseEngineException {
        test5Columns();

        EntityEntry.Builder entry = entry()
                .set("COL1", 2)
                .set("COL2", false)
                .set("COL3", 2.5D)
                .set("COL4", 3L)
                .set("COL5", "ADEUS");

        for (int i = 0; i < 10; i++) {
            entry.set("COL1", i);
            engine.persist("TEST", entry
                    .build());
        }

        List<Map<String, ResultColumn>> query = engine.query(select(ceiling(column("COL3")).alias("CEILING")).from(table("TEST")));

        assertEquals("result ok?", 3.0, query.get(0).get("CEILING").toDouble(), DELTA);
    }

    @Test
    public void twoIntegerDivisionMustReturnADoubleTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", INT)
                .build();

        engine.addEntity(entity);

        EntityEntry.Builder ee = entry()
                .set("COL1", 1)
                .set("COL2", 2);

        engine.persist("TEST", ee
                .build());

        List<Map<String, ResultColumn>> query = engine.query(select(div(column("COL1"), column("COL2")).alias("DIV")).from(table("TEST")));

        assertEquals("", 0.5D, query.get(0).get("DIV").toDouble(), 0);
    }

    @Test
    public void selectWithoutFromTest() throws DatabaseEngineException {
        List<Map<String, ResultColumn>> query = engine.query(select(k(1).alias("constant")));

        assertEquals("constant ok?", 1, (int) query.get(0).get("constant").toInt());
    }

    @Test(expected = DatabaseEngineException.class)
    public void createEntityWithNullNameTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name(null)
                .addColumn("COL1", INT)
                .addColumn("COL2", INT)
                .build();

        try {
            engine.addEntity(entity);
        } catch (final DatabaseEngineException de) {
            assertEquals("exception ok?", "You have to define the entity name", de.getMessage());
            throw de;
        }
    }

    @Test(expected = DatabaseEngineException.class)
    public void createEntityWithNoNameTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("")
                .addColumn("COL1", INT)
                .addColumn("COL2", INT)
                .build();

        try {
            engine.addEntity(entity);
        } catch (final DatabaseEngineException de) {
            assertEquals("exception ok?", "You have to define the entity name", de.getMessage());
            throw de;
        }
    }

    @Test(expected = DatabaseEngineException.class)
    public void createEntityWithNameThatExceedsTheMaximumAllowedTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("0123456789012345678901234567891")
                .addColumn("COL1", INT)
                .addColumn("COL2", INT)
                .build();

        try {
            engine.addEntity(entity);
        } catch (final DatabaseEngineException de) {
            assertEquals("exception ok?", "Entity '0123456789012345678901234567891' exceeds the maximum number of characters (30)", de.getMessage());
            throw de;
        }
    }

    @Test(expected = DatabaseEngineException.class)
    public void createEntityWithColumnThatDoesNotHaveNameTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("entname")
                .addColumn("", INT)
                .addColumn("COL2", INT)
                .build();

        try {
            engine.addEntity(entity);
        } catch (final DatabaseEngineException de) {
            assertEquals("exception ok?", "Column in entity 'entname' must have a name", de.getMessage());
            throw de;
        }
    }

    @Test(expected = DatabaseEngineException.class)
    public void createEntityWithMoreThanOneAutoIncColumn() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("entname")
                .addColumn("COL1", INT, true)
                .addColumn("COL2", INT, true)
                .build();

        try {
            engine.addEntity(entity);
        } catch (final DatabaseEngineException de) {
            assertEquals("exception ok?", "You can only define one auto incremented column", de.getMessage());
            throw de;
        }
    }

    @Test
    public void getGeneratedKeysFromAutoIncTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT, true)
                .addColumn("COL2", INT)
                .build();


        engine.addEntity(entity);

        EntityEntry ee = entry()
                .set("COL2", 2)
                .build();

        Long persist = engine.persist("TEST", ee);

        assertEquals("ret ok?", new Long(1), persist);
    }

    @Test
    public void getGeneratedKeysFromAutoInc2Test() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT, true)
                .addColumn("COL2", INT)
                .build();


        engine.addEntity(entity);

        EntityEntry ee = entry()
                .set("COL2", 2)
                .build();

        Long persist = engine.persist("TEST", ee);

        assertEquals("ret ok?", new Long(1), persist);

        ee = entry()
                .set("COL2", 2)
                .build();

        persist = engine.persist("TEST", ee);

        assertEquals("ret ok?", new Long(2), persist);
    }

    @Test
    public void getGeneratedKeysFromAutoIncWithTransactionTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT, true)
                .addColumn("COL2", INT)
                .build();


        engine.addEntity(entity);

        engine.beginTransaction();

        try {
            EntityEntry ee = entry()
                    .set("COL2", 2)
                    .build();

            Long persist = engine.persist("TEST", ee);

            assertEquals("ret ok?", new Long(1), persist);

            ee = entry()
                    .set("COL2", 2)
                    .build();

            persist = engine.persist("TEST", ee);

            assertEquals("ret ok?", new Long(2), persist);

            engine.commit();
        } finally {
            if (engine.isTransactionActive()) {
                engine.rollback();
            }
        }
    }

    /**
     * Tests that when persisting an entity in table that does not contain any auto generated values, the
     * {@link DatabaseEngine#persist(String, EntityEntry)} method returns {@code null}.
     *
     * @throws DatabaseEngineException If any error occurs.
     */
    @Test
    public void getGeneratedKeysWithNoAutoIncTest() throws DatabaseEngineException {
        final DbEntity entity = dbEntity()
            .name("TEST")
            .addColumn("COL1", STRING)
            .addColumn("COL2", STRING)
            // Set the two columns as fields of primary key, so they belong to the generated keys.
            .pkFields(ImmutableSet.of("COL1", "COL2"))
            .build();

        this.engine.addEntity(entity);

        final EntityEntry ee = entry()
                .set("COL1", "VAL1")
                .set("COL2", "VAL2")
                .build();

        assertThat(this.engine.persist("TEST", ee))
            .as("The auto generated value should be null!")
            .isNull();
    }

    /**
     * Tests that when trying to add {@link DbEntity} with multiple columns with auto incremented values, the
     * {@link DatabaseEngine#addEntity(DbEntity)} method throws a {@link DatabaseEngineException}.
     */
    @Test
    public void addMultipleAutoIncColumnsTest() {
        final DbEntity entity = dbEntity()
            .name("TEST")
            .addColumn("COL1", INT, true)
            .addColumn("COL2", INT, true)
            .build();

        assertThatCode(() -> this.engine.addEntity(entity))
            .as("The DatabaseEngine should not allow to setup a DbEntity with multiple auto incremented columns")
            .isInstanceOf(DatabaseEngineException.class);

    }

    @Test
    public void abortTransactionTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", INT)
                .build();


        engine.addEntity(entity);

        engine.beginTransaction();
        try {
            EntityEntry ee = entry()
                    .set("COL1", 1)
                    .set("COL2", 2)
                    .build();

            engine.persist("TEST", ee);

            throw new Exception();
        } catch (final Exception e) {

        } finally {
            assertTrue("tx active?", engine.isTransactionActive());

            engine.rollback();

            assertFalse("tx active?", engine.isTransactionActive());

            assertEquals("ret 0?", 0, engine.query(select(all()).from(table("TEST"))).size());
        }
    }

    @Test
    public void OneToNTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST1")
                .addColumn("COL1", INT, true)
                .pkFields("COL1")
                .build();

        engine.addEntity(entity);

        entity = dbEntity()
                .name("TEST2")
                .addColumn("COL1", INT, true)
                .addColumn("COL2", INT)
                .addFk(dbFk()
                                .addColumn("COL2")
                                .foreignTable("TEST1")
                                .addForeignColumn("COL1")
                                .build()
                )
                .pkFields("COL1")
                .build();

        engine.addEntity(entity);
    }

    @Test
    public void NtoNTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("USER")
                .addColumn("COL1", INT, true)
                .pkFields("COL1")
                .build();

        engine.addEntity(entity);

        entity = dbEntity()
                .name("ROLE")
                .addColumn("COL1", INT, true)
                .pkFields("COL1")
                .build();

        engine.addEntity(entity);

        entity = dbEntity()
                .name("USER_ROLE")
                .addColumn("COL1", INT)
                .addColumn("COL2", INT)
                .addFk(dbFk()
                                .addColumn("COL1")
                                .foreignTable("USER")
                                .addForeignColumn("COL1")
                                .build(),
                        dbFk()
                                .addColumn("COL2")
                                .foreignTable("ROLE")
                                .addForeignColumn("COL1")
                                .build()
                )
                .pkFields("COL1", "COL2")
                .build();

        engine.addEntity(entity);
    }

    @Test
    public void NtoNOneToNTest() throws DatabaseEngineException {
        userRolePermissionSchema();
    }

    @Test
    public void createEntityDropItAndCreateItAgainTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("USER")
                .addColumn("COL1", INT, true)
                .pkFields("COL1")
                .build();

        engine.addEntity(entity);
        DbEntity removeEntity = engine.removeEntity("USER");

        assertNotNull(removeEntity);

        engine.addEntity(entity);
    }

    @Test
    public void dropEntityThatDoesNotExistTest() {
        DbEntity removeEntity = engine.removeEntity("TABLETHATDOESNOTEXIST");

        assertNull(removeEntity);
    }

    @Test
    public void joinsTest() throws DatabaseEngineException {

        userRolePermissionSchema();

        engine.query(
                select(all())
                        .from(
                                table("USER").alias("a").innerJoin(table("USER_ROLE").alias("b"), eq(column("a", "COL1"), column("b", "COL1")))
                        )
        );

        engine.query(
                select(all())
                        .from(
                                table("USER").alias("a")
                                        .innerJoin(table("USER_ROLE").alias("b"), eq(column("a", "COL1"), column("b", "COL1")))
                                        .innerJoin(table("ROLE").alias("c"), eq(column("b", "COL2"), column("c", "COL1")))
                        )
        );

        engine.query(
                select(all())
                        .from(
                                table("USER").alias("a").rightOuterJoin(table("USER_ROLE").alias("b"), eq(column("a", "COL1"), column("b", "COL1")))
                        )
        );

        engine.query(
                select(all())
                        .from(
                                table("USER").alias("a").leftOuterJoin(table("USER_ROLE").alias("b"), eq(column("a", "COL1"), column("b", "COL1")))
                        )
        );
    }

    @Test
    public void joinATableWithQueryTest() throws DatabaseEngineException {
        userRolePermissionSchema();

        engine.query(
                select(all())
                        .from(
                                table("USER").alias("a")
                                        .innerJoin(
                                                select(column("COL1"))
                                                        .from(table("USER")).alias("b")
                                                , eq(column("a", "COL1"), column("b", "COL1"))
                                        )
                        )
        );
    }

    @Test
    public void joinAQueryWithATableTest() throws DatabaseEngineException {
        userRolePermissionSchema();

        engine.query(
                select(all())
                        .from(
                                select(column("COL1"))
                                        .from(table("USER")).alias("b")
                                        .innerJoin(
                                                table("USER").alias("a")
                                                , eq(column("a", "COL1"), column("b", "COL1"))
                                        )
                        )
        );
    }

    @Test
    public void joinTwoQueriesTest() throws DatabaseEngineException {
        userRolePermissionSchema();

        engine.query(
                select(all())
                        .from(
                                select(column("COL1"))
                                        .from(table("USER")).alias("a")
                                        .innerJoin(
                                                select(column("COL1"))
                                                        .from(table("USER")).alias("b")
                                                , eq(column("a", "COL1"), column("b", "COL1"))
                                        )
                        )
        );
    }

    @Test
    public void joinThreeQueriesTest() throws DatabaseEngineException {
        userRolePermissionSchema();

        engine.query(
                select(all())
                        .from(
                                select(column("COL1"))
                                        .from(table("USER")).alias("a")
                                        .innerJoin(
                                                select(column("COL1"))
                                                        .from(table("USER")).alias("b")
                                                , eq(column("a", "COL1"), column("b", "COL1"))
                                        )
                                        .rightOuterJoin(
                                                select(column("COL1"))
                                                        .from(table("USER")).alias("c")
                                                , eq(column("a", "COL1"), column("c", "COL1"))
                                        )
                        )
        );
    }

    @Test
    @Category(SkipTestCockroachDB.class)
    // unimplemented in CockroachDB: views do not currently support * expressions
    // https://github.com/cockroachdb/cockroach/issues/10028
    public void createAndDropViewTest() throws DatabaseEngineException {
        test5Columns();

        engine.executeUpdate(
                createView("VN").as(select(all()).from(table("TEST")))
        );

        engine.dropView("VN");
    }

    @Test
    @Category(SkipTestCockroachDB.class)
    // unimplemented in CockroachDB: views do not currently support * expressions
    // https://github.com/cockroachdb/cockroach/issues/10028
    public void createOrReplaceViewTest() throws DatabaseEngineException {
        test5Columns();

        engine.executeUpdate(
                createView("VN").as(select(all()).from(table("TEST"))).replace()
        );

        engine.dropView("VN");
    }

    @Test
    public void distinctTest() throws DatabaseEngineException {
        test5Columns();

        engine.query(
                select(all()).distinct()
                        .from(table("TEST"))
        );
    }

    @Test
    public void distinctAndLimitTogetherTest() throws DatabaseEngineException {
        test5Columns();

        engine.query(
                select(all()).distinct()
                        .from(table("TEST")).limit(2)
        );
    }

    @Test
    public void notEqualTest() throws DatabaseEngineException {
        test5Columns();

        engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(neq(column("COL1"), k(1)))
        );
    }

    @Test
    public void inTest() throws DatabaseEngineException {
        test5Columns();

        engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(
                                in(
                                        L(column("COL1")),
                                        L((k(1)))
                                )
                        )
        );
    }

    @Test
    public void inSelectTest() throws DatabaseEngineException {
        test5Columns();

        engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(
                                in(
                                        L(column("COL1")),
                                        select(column("COL1")).from(table("TEST"))
                                )
                        )
        );
    }

    @Test
    public void booleanTrueComparisonTest() throws DatabaseEngineException {
        test5Columns();

        EntityEntry entry1 = entry()
                .set("COL1", 1)
                .set("COL2", true)
                .set("COL3", 1)
                .set("COL4", 1)
                .set("COL5", "val 1")
                .build();
        engine.persist("TEST", entry1, false);

        EntityEntry entry2 = entry()
                .set("COL1", 1)
                .set("COL2", false)
                .set("COL3", 1)
                .set("COL4", 1)
                .set("COL5", "val 1")
                .build();
        engine.persist("TEST", entry2, false);

        List<Map<String, ResultColumn>> rows = engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(
                                eq(column("COL2"), k(true))
                        )
        );

        assertEquals(1, rows.size());
    }

    @Test
    public void booleanFalseComparisonTest() throws DatabaseEngineException {
        test5Columns();

        EntityEntry entry1 = entry()
                .set("COL1", 1)
                .set("COL2", true)
                .set("COL3", 1)
                .set("COL4", 1)
                .set("COL5", "val 1")
                .build();
        engine.persist("TEST", entry1, false);

        EntityEntry entry2 = entry()
                .set("COL1", 1)
                .set("COL2", false)
                .set("COL3", 1)
                .set("COL4", 1)
                .set("COL5", "val 1")
                .build();
        engine.persist("TEST", entry2, false);

        List<Map<String, ResultColumn>> rows = engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(
                                eq(column("COL2"), k(false))
                        )
        );

        assertEquals(1, rows.size());
    }

    @Test
    public void coalesceTest() throws DatabaseEngineException {
        test5Columns();

        engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(
                                eq(coalesce(column("COL2"), k(false)), k(false))
                        )
        );
    }

    @Test
    public void multipleCoalesceTest() throws DatabaseEngineException {
        test5Columns();

        engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(
                                eq(coalesce(column("COL2"), k(false), k(true)), k(false))
                        )
        );
    }

    @Test
    public void betweenTest() throws DatabaseEngineException {
        test5Columns();

        engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(
                                between(column("COL1"), k(1), k(2))
                        )
        );
    }



    @Test
    public void testCast() throws DatabaseEngineException {

        final Query query =
                select(cast(k("22"), INT).alias("int"),
                        cast(k(22), STRING).alias("string"),
                        cast(k("1"), BOOLEAN).alias("bool"),
                        cast(k("22"), DOUBLE).alias("double"),
                        cast(k(22), LONG).alias("long"));

        // CAST not supported.
        if (config.engine.contains("MySqlEngine")) {
            exception.expect(OperationNotSupportedRuntimeException.class);
        }

        final List<Map<String, ResultColumn>> result = engine.query(query);

        assertEquals("Result must be 22", new Integer(22),
                result.get(0).get("int").toInt());
        assertEquals("Result must be '22'", "22",
                result.get(0).get("string").toString());
        assertEquals("Result must be true", true,
                result.get(0).get("bool").toBoolean());
        assertEquals("Result must be 22.0", new Double(22),
                result.get(0).get("double").toDouble());
        assertEquals("Result must be 22", new Long(22),
                result.get(0).get("long").toLong());
    }

    @Test
    public void testCastColumns() throws DatabaseEngineException {

        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "1")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "2")
                .build());
        engine.persist("TEST", entry().set("COL1", 3).set("COL5", "3")
                .build());
        engine.persist("TEST", entry().set("COL1", 4).set("COL5", "4")
                .build());

        final Query query =
                select(cast(column("COL1"), STRING).alias("string"),
                        cast(column("COL5"), INT).alias("int"))
                .from(table("TEST"));

        // CAST not supported.
        if (config.engine.contains("MySqlEngine")) {
            exception.expect(OperationNotSupportedRuntimeException.class);
        }

        final List<Map<String, ResultColumn>> result = engine.query(query);

        assertEquals("Result must be 1", new Integer(1),
                result.get(0).get("int").toInt());
        assertEquals("Result must be 2", new Integer(2),
                result.get(1).get("int").toInt());
        assertEquals("Result must be 3", new Integer(3),
                result.get(2).get("int").toInt());
        assertEquals("Result must be 4", new Integer(4),
                result.get(3).get("int").toInt());
        assertEquals("Result must be '1'", "1",
                result.get(0).get("string").toString());
        assertEquals("Result must be '2'", "2",
                result.get(1).get("string").toString());
        assertEquals("Result must be '3'", "3",
                result.get(2).get("string").toString());
        assertEquals("Result must be '4'", "4",
                result.get(3).get("string").toString());
    }

    @Test(expected = OperationNotSupportedRuntimeException.class)
    public void testCastUnsupported() throws DatabaseEngineException {
        // Check if exception is thrown when trying to cast for an unsupported type.
        engine.query(select(cast(k("22"), BLOB)));
    }

    @Test
    public void testWith() throws DatabaseEngineException {

        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "manuel")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "ana")
                .build());
        engine.persist("TEST", entry().set("COL1", 3).set("COL5", "rita")
                .build());
        engine.persist("TEST", entry().set("COL1", 4).set("COL5", "rui")
                .build());

        final With with = with("friends", select(all())
                                                .from(table("TEST")))
                .then(
                        select(column("COL5").alias("name"))
                        .from(table("friends"))
                        .where(eq(column("COL1"), k(1))));

        // MySQL does not support With
        if (config.engine.contains("MySqlEngine")) {
            exception.expect(OperationNotSupportedRuntimeException.class);
        }

        final List<Map<String, ResultColumn>> result = engine.query(with);

        assertEquals("Name must be 'manuel'", "manuel", result.get(0).get("name").toString());
    }

    @Test
    public void testWithAll() throws DatabaseEngineException {

        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "manuel")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "ana")
                .build());
        engine.persist("TEST", entry().set("COL1", 3).set("COL5", "rita")
                .build());
        engine.persist("TEST", entry().set("COL1", 4).set("COL5", "rui")
                .build());

        final With with =
                with("friends",
                        select(all())
                        .from(table("TEST")))
                .then(
                        select(column("COL5").alias("name"))
                        .from(table("friends"))
                        .orderby(column("COL5")));

        // MySQL does not support With
        if (config.engine.contains("MySqlEngine")) {
            exception.expect(OperationNotSupportedRuntimeException.class);
        }

        final List<Map<String, ResultColumn>> result = engine.query(with);

        assertEquals("Name must be 'ana'", "ana", result.get(0).get("name").toString());
        assertEquals("Name must be 'manuel'", "manuel", result.get(1).get("name").toString());
        assertEquals("Name must be 'rita'", "rita", result.get(2).get("name").toString());
        assertEquals("Name must be 'rui'", "rui", result.get(3).get("name").toString());
    }

    @Test
    public void testWithMultiple() throws DatabaseEngineException {

        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "manuel")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "ana")
                .build());
        engine.persist("TEST", entry().set("COL1", 3).set("COL5", "rita")
                .build());
        engine.persist("TEST", entry().set("COL1", 4).set("COL5", "rui")
                .build());

        final With with =
                with("friendsA",
                        select(all())
                        .from(table("TEST"))
                        .where(or(eq(column("COL1"), k(1)), eq(column("COL1"), k(2)))))

                .andWith("friendsB",
                        select(all())
                        .from(table("TEST"))
                        .where(or(eq(column("COL1"), k(3)), eq(column("COL1"), k(4)))))
                .then(
                        union(select(all()).from(table("friendsA")),
                              select(all()).from(table("friendsB"))));

        // MySQL does not support With
        if (config.engine.contains("MySqlEngine")) {
            exception.expect(OperationNotSupportedRuntimeException.class);
        }

        final List<Map<String, ResultColumn>> result = engine.query(with);

        final List<String> resultSorted = result.stream()
                .map(row -> row.get("COL5").toString())
                .sorted()
                .collect(Collectors.toList());

        assertEquals("Name must be 'ana'", "ana", resultSorted.get(0));
        assertEquals("Name must be 'manuel'", "manuel", resultSorted.get(1));
        assertEquals("Name must be 'rita'", "rita", resultSorted.get(2));
        assertEquals("Name must be 'rui'", "rui", resultSorted.get(3));
    }

    @Test
    public void testCaseWhen() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "xpto")
                .build());
        engine.persist("TEST", entry().set("COL1", 3).set("COL5", "xpto")
                .build());
        engine.persist("TEST", entry().set("COL1", 4).set("COL5", "teste")
                .build());

        List<Map<String, ResultColumn>> result = engine.query(
                select(caseWhen().when(eq(column("COL5"), k("teste")), k("LOL")).alias("case"))
                        .from(table("TEST")));

        assertEquals("COL5 must be LOL", "LOL", result.get(0).get("case").toString());
        assertEquals("COL5 must be LOL", "LOL", result.get(3).get("case").toString());
    }

    @Test
    public void testCaseWhenElse() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "xpto")
                .build());
        engine.persist("TEST", entry().set("COL1", 3).set("COL5", "xpto")
                .build());
        engine.persist("TEST", entry().set("COL1", 4).set("COL5", "teste")
                .build());

        List<Map<String, ResultColumn>> result = engine.query(
                select(caseWhen().when(eq(column("COL5"), k("teste")), k("LOL"))
                               .otherwise(k("ROFL")).alias("case"))
                        .from(table("TEST"))
        );

        assertEquals("COL5 must be LOL", "LOL", result.get(0).get("case").toString());
        assertEquals("COL5 must be ROFL", "ROFL", result.get(1).get("case").toString());
        assertEquals("COL5 must be ROFL", "ROFL", result.get(2).get("case").toString());
        assertEquals("COL5 must be LOL", "LOL", result.get(3).get("case").toString());
    }

    @Test
    public void testCaseMultipleWhenElse() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "xpto")
                .build());
        engine.persist("TEST", entry().set("COL1", 3).set("COL5", "xpto")
                .build());
        engine.persist("TEST", entry().set("COL1", 4).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 5).set("COL5", "pomme de terre")
                .build());

        List<Map<String, ResultColumn>> result = engine.query(
                select(caseWhen().when(eq(column("COL5"), k("teste")), k("LOL"))
                                .when(eq(column("COL5"), k("pomme de terre")), k("KEK"))
                                .otherwise(k("ROFL")).alias("case"))
                        .from(table("TEST"))
        );

        assertEquals("COL5 must be LOL", "LOL", result.get(0).get("case").toString());
        assertEquals("COL5 must be ROFL", "ROFL", result.get(1).get("case").toString());
        assertEquals("COL5 must be ROFL", "ROFL", result.get(2).get("case").toString());
        assertEquals("COL5 must be LOL", "LOL", result.get(3).get("case").toString());
        assertEquals("COL5 must be KEK", "KEK", result.get(4).get("case").toString());
    }

    @Test
    public void testConcat() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "xpto")
                .build());
        engine.persist("TEST", entry().set("COL1", 3).set("COL5", "xpto")
                .build());
        engine.persist("TEST", entry().set("COL1", 4).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 5).set("COL5", "pomme de terre")
                .build());

        final Query query =
                select(
                        concat(k("."),
                               column("COL5"),
                               cast(column("COL1"), STRING)).alias("concat"))
                        .from(table("TEST"));

        final List<Map<String, ResultColumn>> result = engine.query(query);

        assertEquals("Concat must be 'teste.1'", "teste.1", result.get(0).get("concat").toString());
        assertEquals("Concat must be 'xpto.2'", "xpto.2", result.get(1).get("concat").toString());
        assertEquals("Concat must be 'xpto.3'", "xpto.3", result.get(2).get("concat").toString());
        assertEquals("Concat must be 'teste.4'", "teste.4", result.get(3).get("concat").toString());
        assertEquals("Concat must be 'pomme de terre.5'", "pomme de terre.5", result.get(4).get("concat").toString());
    }

    /**
     * Reproduces an issue when using CASE ... WHEN expressions in SqlServer and MySql.
     * <p>
     * Since we don't have the type information for a column that is generated from the result of a WHEN expression,
     * we need to rely on the user calling one of the ResultColumn.toXXX methods to understand what the user is
     * expecting. In the case of ResultColumn.toBoolean(), we're first checking if the result is of boolean type,
     * as happens normally when the driver knows that the column is of type boolean, but then we also try to parse the
     * underlying database boolean representation. This is necessary because in WHEN expressions, the driver doesn't
     * know the expected return type.
     * <p>
     * I also tried to fix this using {@code cast(1 as BIT)}, which seemed more appropriate because we would be hinting
     * the driver about the type, but it's not possible to follow this approach in MySql because we cannot cast to
     * tinyint(1), which is the native type for booleans in MySql.
     *
     * @throws DatabaseEngineException propagate
     */
    @Test
    public void testCaseToBoolean() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL2", false).build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL2", true).set("COL5", "xpto").build());

        final Query query = select(
                column("COL2"),
                caseWhen()
                        .when(column("COL5").isNotNull(), k(true))
                        .otherwise(k(false))
                        .alias("COL5_NOT_NULL"))
                .from(table("TEST"))
                .orderby(column("COL1").asc());

        final List<Map<String, ResultColumn>> result = engine.query(query);

        assertFalse("COL2 should be false", result.get(0).get("COL2").toBoolean());
        assertFalse("COL5_NOT_NULL should be false", result.get(0).get("COL5_NOT_NULL").toBoolean());
        assertTrue("COL2 should be true", result.get(1).get("COL2").toBoolean());
        assertTrue("COL5_NOT_NULL should be true", result.get(1).get("COL5_NOT_NULL").toBoolean());
    }

    @Test
    public void testUnion() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "a")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "b")
                .build());
        engine.persist("TEST", entry().set("COL1", 3).set("COL5", "c")
                .build());
        engine.persist("TEST", entry().set("COL1", 4).set("COL5", "d")
                .build());
        engine.persist("TEST", entry().set("COL1", 5).set("COL5", "d")
                .build());

        final String[] letters = new String[] {"a", "b", "c", "d", "d"};
        final Collection<Expression> queries = Arrays.stream(letters)
                .map(literal ->
                        select(column("COL5"))
                        .from(table("TEST"))
                        .where(eq(column("COL5"), k(literal))))
                .collect(Collectors.toList());

        final Expression query = union(queries);
        final List<Map<String, ResultColumn>> result = engine.query(query);

        assertEquals("Must return 4 results due to distinct property", 4, result.size());

        final List<String> resultSorted = result.stream()
                .map(row -> row.get("COL5").toString())
                .sorted()
                .collect(Collectors.toList());

        assertEquals("COL5 must be a", "a", resultSorted.get(0));
        assertEquals("COL5 must be b", "b", resultSorted.get(1));
        assertEquals("COL5 must be c", "c", resultSorted.get(2));
        assertEquals("COL5 must be d", "d", resultSorted.get(3));
    }

    @Test
    public void testUnionAll() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "a")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "b")
                .build());
        engine.persist("TEST", entry().set("COL1", 3).set("COL5", "c")
                .build());
        engine.persist("TEST", entry().set("COL1", 4).set("COL5", "d")
                .build());
        engine.persist("TEST", entry().set("COL1", 5).set("COL5", "d")
                .build());

        final int[] ids = new int[] {1, 2, 3, 4, 5};
        final Collection<Expression> queries = Arrays.stream(ids)
                .mapToObj(literal ->
                        select(column("COL5"))
                        .from(table("TEST"))
                        .where(eq(column("COL1"), k(literal))))
                .collect(Collectors.toList());

        final Expression query = union(queries).all();
        final List<Map<String, ResultColumn>> result = engine.query(query);

        assertEquals("Must return 5 results", 5, result.size());

        final List<String> resultSorted = result.stream()
                .map(row -> row.get("COL5").toString())
                .sorted()
                .collect(Collectors.toList());

        assertEquals("COL5 must be a", "a", resultSorted.get(0));
        assertEquals("COL5 must be b", "b", resultSorted.get(1));
        assertEquals("COL5 must be c", "c", resultSorted.get(2));
        assertEquals("COL5 must be d", "d", resultSorted.get(3));
        assertEquals("COL5 must be d", "d", resultSorted.get(4));
    }

    @Test
    public void testValues() throws DatabaseEngineException {
        final Values values =
                values("id", "name")
                    .row(k(1), k("ana"))
                    .row(k(2), k("fred"))
                    .row(k(3), k("manuel"))
                    .row(k(4), k("rita"));

        final List<Map<String, ResultColumn>> result = engine.query(values);

        final List<Integer> ids = result.stream()
                .map(row -> row.get("id").toInt())
                .sorted()
                .collect(Collectors.toList());

        final List<String> names = result.stream()
                .map(row -> row.get("name").toString())
                .sorted()
                .collect(Collectors.toList());

        assertEquals("id must be 1", new Integer(1), ids.get(0));
        assertEquals("id must be 2", new Integer(2), ids.get(1));
        assertEquals("id must be 3", new Integer(3), ids.get(2));
        assertEquals("id must be 4", new Integer(4), ids.get(3));

        assertEquals("name must be 'ana'", "ana", names.get(0));
        assertEquals("name must be 'fred'", "fred", names.get(1));
        assertEquals("name must be 'manuel'", "manuel", names.get(2));
        assertEquals("name must be 'rita'", "rita", names.get(3));
    }

    @Test(expected = DatabaseEngineRuntimeException.class)
    public void testValuesNoAliases() throws DatabaseEngineException {
        final Values values =
                values()
                    .row(k(1), k("ana"))
                    .row(k(2), k("fred"))
                    .row(k(3), k("manuel"))
                    .row(k(4), k("rita"));
        try {
            engine.query(values);
        } catch (DatabaseEngineRuntimeException e) {
            assertEquals("Values requires aliases to avoid ambiguous columns names.", e.getMessage());
            throw e;
        }
    }

    @Test
    public void testLargeValues() throws DatabaseEngineException {
        final Values values = values("long", "uuid");

        for (int i = 0 ; i < 256 ; i++) {
            values.row(k(ThreadLocalRandom.current().nextLong()),
                    k(UUID.randomUUID().toString()));
        }

        // If it crashes, the test will fail.
        engine.query(values);
    }

    @Test
    public void betweenWithSelectTest() throws DatabaseEngineException {
        test5Columns();

        engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(
                                between(select(column("COL1")).from(table("TEST")).enclose(), k(1), k(2))
                        )
        );
    }

    @Test
    public void betweenEnclosedTest() throws DatabaseEngineException {
        test5Columns();

        engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(
                                between(column("COL1"), k(1), k(2)).enclose()
                        )
        );
    }

    @Test
    public void notBetweenTest() throws DatabaseEngineException {
        test5Columns();

        engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(
                                notBetween(column("COL1"), k(1), k(2)).enclose()
                        )
        );
    }

    @Test
    public void modTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", INT)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        EntityEntry entry = entry()
                .set("COL1", 12)
                .set("COL2", false)
                .set("COL3", 2D)
                .set("COL4", 5)
                .set("COL5", "ADEUS")
                .build();

        engine.persist("TEST", entry);

        List<Map<String, ResultColumn>> query = engine.query(select(mod(column("COL1"), column("COL4")).alias("MODULO")).from(table("TEST")));

        assertEquals("result ok?", 2, (int) query.get(0).get("MODULO").toInt());

    }

    @Test
    public void subSelectTest() throws DatabaseEngineException {
        List<Map<String, ResultColumn>> query = engine.query(
                select(
                        k(1000).alias("timestamp"),
                        column("sq_1", "one").alias("first"),
                        column("sq_1", "two").alias("second"),
                        column("sq_1", "three").alias("third"))
                        .from(
                                select(
                                        k(1).alias("one"),
                                        k(2L).alias("two"),
                                        k(3.0).alias("three")).alias("sq_1")
                        )
        );

        assertEquals("result ok?", 1000, (long) query.get(0).get("timestamp").toLong());
        assertEquals("result ok?", 1, (int) query.get(0).get("first").toInt());
        assertEquals("result ok?", 2L, (long) query.get(0).get("second").toLong());
        assertEquals("result ok?", 3.0, query.get(0).get("third").toDouble(), 0.0);
    }

    @Test
    public void update1ColTest() throws DatabaseEngineException {
        test5Columns();

        engine.persist("TEST", entry().set("COL1", 5)
                .build());

        engine.executeUpdate(
                update(table("TEST"))
                        .set(eq(column("COL1"), k(1)))
        );
    }

    @Test
    public void update2ColTest() throws DatabaseEngineException {
        test5Columns();

        engine.persist("TEST", entry().set("COL1", 5)
                .build());

        engine.executeUpdate(
                update(table("TEST"))
                        .set(
                                eq(column("COL1"), k(1)),
                                eq(column("COL5"), k("ola")))
        );
    }

    @Test
    public void updateWithAliasTest() throws DatabaseEngineException {
        test5Columns();

        engine.persist("TEST", entry().set("COL1", 5)
                .build());

        engine.executeUpdate(
                update(table("TEST").alias("T"))
                        .set(
                                eq(column("COL1"), k(1)),
                                eq(column("COL5"), k("ola")))
        );
    }

    @Test
    public void updateWithWhereTest() throws DatabaseEngineException {
        test5Columns();

        engine.persist("TEST", entry().set("COL1", 5)
                .build());

        engine.executeUpdate(
                update(table("TEST").alias("T"))
                        .set(
                                eq(column("COL1"), k(1)),
                                eq(column("COL5"), k("ola")))
                        .where(eq(column("COL1"), k(5)))
        );
    }

    @Test
    public void deleteTest() throws DatabaseEngineException {
        test5Columns();

        engine.persist("TEST", entry().set("COL1", 5)
                .build());

        engine.executeUpdate(
                delete(table("TEST"))
        );
    }

    @Test
    public void deleteWithWhereTest() throws DatabaseEngineException {
        test5Columns();

        engine.persist("TEST", entry().set("COL1", 5)
                .build());

        engine.executeUpdate(
                delete(table("TEST"))
                        .where(eq(column("COL1"), k(5)))
        );
    }

    @Test
    public void deleteCheckReturnTest() throws DatabaseEngineException {
        test5Columns();

        engine.persist("TEST", entry().set("COL1", 5)
                .build());
        engine.persist("TEST", entry().set("COL1", 6)
                .build());

        int rowsDeleted = engine.executeUpdate(
                delete(table("TEST"))
        );

        assertEquals(2, rowsDeleted);
    }

    @Test
    public void executePreparedStatementTest() throws DatabaseEngineException, NameAlreadyExistsException, ConnectionResetException {
        test5Columns();

        EntityEntry ee = entry()
                .set("COL1", 1)
                .set("COL2", true)
                .build();

        engine.persist("TEST", ee);

        String ec = engine.escapeCharacter();
        engine.createPreparedStatement("test", "SELECT * FROM " + quotize("TEST", ec) + " WHERE " + quotize("COL1", ec) + " = ?");
        engine.setParameters("test", 1);
        engine.executePS("test");
        List<Map<String, ResultColumn>> res = engine.getPSResultSet("test");

        assertEquals("col1 ok?", 1, (int) res.get(0).get("COL1").toInt());
        assertTrue("col2 ok?", res.get(0).get("COL2").toBoolean());
    }

    @Test
    public void executePreparedStatementUpdateTest() throws DatabaseEngineException, NameAlreadyExistsException, ConnectionResetException {
        test5Columns();

        EntityEntry ee = entry()
                .set("COL1", 1)
                .set("COL2", true)
                .build();

        engine.persist("TEST", ee);

        engine.createPreparedStatement("test", update(table("TEST")).set(eq(column("COL1"), lit("?"))));
        engine.setParameters("test", 2);
        engine.executePSUpdate("test");

        List<Map<String, ResultColumn>> res = engine.query("SELECT * FROM " + quotize("TEST", engine.escapeCharacter()));

        assertEquals("col1 ok?", 2, (int) res.get(0).get("COL1").toInt());
        assertTrue("col2 ok?", res.get(0).get("COL2").toBoolean());
    }

    @Test
    public void metadataTest() throws DatabaseEngineException {
        DbEntity entity =
                dbEntity()
                        .name("TEST")
                        .addColumn("COL1", INT)
                        .addColumn("COL2", BOOLEAN)
                        .addColumn("COL3", DOUBLE)
                        .addColumn("COL4", LONG)
                        .addColumn("COL5", STRING)
                        .addColumn("COL6", BLOB)
                        .build();

        engine.addEntity(entity);

        final Map<String, DbColumnType> metaMap = new LinkedHashMap<>();
        metaMap.put("COL1", INT);
        metaMap.put("COL2", BOOLEAN);
        metaMap.put("COL3", DOUBLE);
        metaMap.put("COL4", LONG);
        metaMap.put("COL5", STRING);
        metaMap.put("COL6", BLOB);

        assertEquals("meta ok?", metaMap, engine.getMetadata("TEST"));
    }

    @Test
    public void getMetadataOnATableThatDoesNotExistTest() throws DatabaseEngineException {
        assertTrue("get metadata on table that does not exist is empty", engine.getMetadata("TableThatDoesNotExist").isEmpty());
    }

    @Test
    public void testSqlInjection1() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);

        EntityEntry entry = entry().set("COL1", 2).set("COL2", false).set("COL3", 2D).set("COL4", 3L).set("COL5", "ADEUS")
                .build();
        engine.persist("TEST", entry);
        entry = entry().set("COL1", 2).set("COL2", false).set("COL3", 2D).set("COL4", 3L).set("COL5", "ADEUS2")
                .build();
        engine.persist("TEST", entry);

        List<Map<String, ResultColumn>> result = engine.query(select(all()).from(table("TEST")).where(eq(column("COL5"), k("ADEUS' or 1 = 1 " + engine.commentCharacter()))));

        assertEquals("Testing sql injection", 0, result.size());
    }

    @Test
    public void testBlob() throws Exception {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", STRING)
                .addColumn("COL2", BLOB)
                .build();

        engine.addEntity(entity);

        EntityEntry entry = entry().set("COL1", "CENINHAS").set("COL2", new BlobTest(1, "name"))
                .build();

        engine.persist("TEST", entry);

        List<Map<String, ResultColumn>> result = engine.query(select(all()).from(table("TEST")));
        assertEquals("CENINHAS", result.get(0).get("COL1").toString());
        assertEquals(new BlobTest(1, "name"), result.get(0).get("COL2").<BlobTest>toBlob());

        BlobTest updBlob = new BlobTest(2, "cenas");

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(updBlob);

        Update upd = update(table("TEST")).set(eq(column("COL2"), lit("?"))).where(eq(column("COL1"), k("CENINHAS")));

        engine.createPreparedStatement("testBlob", upd);

        engine.setParameters("testBlob", bos.toByteArray());

        engine.executePSUpdate("testBlob");

        result = engine.query(select(all()).from(table("TEST")));
        assertEquals("CENINHAS", result.get(0).get("COL1").toString());
        assertEquals(updBlob, result.get(0).get("COL2").<BlobTest>toBlob());
    }

    @Test
    public void testBlobSettingWithIndexTest() throws Exception {
        DbEntity entity = dbEntity().name("TEST").addColumn("COL1", STRING).addColumn("COL2", BLOB)
                .build();
        engine.addEntity(entity);
        EntityEntry entry = entry().set("COL1", "CENINHAS").set("COL2", new BlobTest(1, "name"))
                .build();
        engine.persist("TEST", entry);
        List<Map<String, ResultColumn>> result = engine.query(select(all()).from(table("TEST")));
        assertEquals("CENINHAS", result.get(0).get("COL1").toString());
        assertEquals(new BlobTest(1, "name"), result.get(0).get("COL2").<BlobTest>toBlob());

        BlobTest updBlob = new BlobTest(2, "cenas");

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(updBlob);

        Update upd = update(table("TEST")).set(eq(column("COL2"), lit("?"))).where(eq(column("COL1"), k("CENINHAS")));
        engine.createPreparedStatement("testBlob", upd);
        engine.setParameter("testBlob", 1, bos.toByteArray());
        engine.executePSUpdate("testBlob");
        result = engine.query(select(all()).from(table("TEST")));
        assertEquals("CENINHAS", result.get(0).get("COL1").toString());
        assertEquals(updBlob, result.get(0).get("COL2").<BlobTest>toBlob());
    }

    @Test
    public void testBlobByteArray() throws Exception {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", STRING)
                .addColumn("COL2", BLOB)
                .build();

        engine.addEntity(entity);

        // 10 mb
        byte[] bb = new byte[1024 * 1024 * 10];
        byte[] bb2 = new byte[1024 * 1024 * 10];
        for (int i = 0; i < bb.length; i++) {
            bb[i] = (byte) (Math.random() * 128);
            bb2[i] = (byte) (Math.random() * 64);
        }

        EntityEntry entry = entry().set("COL1", "CENINHAS").set("COL2", bb)
                .build();

        engine.persist("TEST", entry);

        List<Map<String, ResultColumn>> result = engine.query(select(all()).from(table("TEST")));
        assertEquals("CENINHAS", result.get(0).get("COL1").toString());
        assertArrayEquals(bb, result.get(0).get("COL2").toBlob());


        Update upd = update(table("TEST")).set(eq(column("COL2"), lit("?"))).where(eq(column("COL1"), k("CENINHAS")));

        engine.createPreparedStatement("upd", upd);

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(bb2);

        engine.setParameters("upd", bos.toByteArray());

        engine.executePSUpdate("upd");

        result = engine.query(select(all()).from(table("TEST")));
        assertEquals("CENINHAS", result.get(0).get("COL1").toString());
        assertArrayEquals(bb2, result.get(0).get("COL2").toBlob());

    }

    @Test
    public void testBlobString() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", STRING)
                .addColumn("COL2", BLOB)
                .build();

        engine.addEntity(entity);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4000; i++) {
            sb.append("a");
        }

        String bigString = sb.toString();
        EntityEntry entry = entry().set("COL1", "CENINHAS").set("COL2", bigString)
                .build();

        engine.persist("TEST", entry);

        List<Map<String, ResultColumn>> result = engine.query(select(all()).from(table("TEST")));
        assertEquals("CENINHAS", result.get(0).get("COL1").toString());
        assertEquals(bigString, result.get(0).get("COL2").<String>toBlob());
    }

    @Test
    public void testBlobJSON() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", STRING)
                .addColumn("COL2", BLOB)
                .build();

        engine.addEntity(entity);

        String bigString = "[{\"type\":\"placeholder\",\"conf\":{},\"row\":0,\"height\":280,\"width\":12}]";
        EntityEntry entry = entry().set("COL1", "CENINHAS").set("COL2", bigString)
                .build();

        engine.persist("TEST", entry);

        List<Map<String, ResultColumn>> result = engine.query(select(all()).from(table("TEST")));
        assertEquals("CENINHAS", result.get(0).get("COL1").toString());
        assertEquals(bigString, result.get(0).get("COL2").<String>toBlob());
    }

    @Test
    public void addDropColumnWithDropCreateTest() throws DatabaseEngineException {
        DbEntity.Builder entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT, true)
                .addColumn("COL2", BOOLEAN)
                .addColumn("USER", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1");
        engine.addEntity(entity
                .build());
        Map<String, DbColumnType> test = engine.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(BOOLEAN, test.get("COL2"));
        assertEquals(DOUBLE, test.get("USER"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));

        EntityEntry entry = entry().set("COL1", 1).set("COL2", true).set("USER", 2d).set("COL4", 1L).set("COL5", "c")
                .build();
        engine.persist("TEST", entry);

        entity.removeColumn("USER");
        entity.removeColumn("COL2");
        engine.updateEntity(entity
                .build());

        // as the fields were removed the entity mapping ignores the fields.
        entry = entry().set("COL1", 2).set("COL2", true).set("COL3", 2d).set("COL4", 1L).set("COL5", "c")
                .build();
        engine.persist("TEST", entry);


        test = engine.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));

        entity.addColumn("COL6", BLOB).addColumn("COL7", DOUBLE);
        engine.updateEntity(entity
                .build());

        entry = entry().set("COL1", 3).set("COL2", true).set("USER", 2d).set("COL4", 1L).set("COL5", "c").set("COL6", new BlobTest(1, "")).set("COL7", 2d)
                .build();
        engine.persist("TEST", entry);

        test = engine.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));
        assertEquals(BLOB, test.get("COL6"));
        assertEquals(DOUBLE, test.get("COL7"));

    }

    @Test
    public void addDropColumnTest() throws Exception {
        // First drop-create
        DbEntity.Builder entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT, true)
                .addColumn("COL2", BOOLEAN)
                .addColumn("USER", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1");
        engine.addEntity(entity.build());
        Map<String, DbColumnType> test = engine.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(BOOLEAN, test.get("COL2"));
        assertEquals(DOUBLE, test.get("USER"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));

        // Clone the connection with the create now.
        final DatabaseEngine engine2 = this.engine.duplicate(new Properties() {
            {
                setProperty(SCHEMA_POLICY, "create");
            }
        }, true);

        EntityEntry entry = entry().set("COL1", 1).set("COL2", true).set("USER", 2d).set("COL4", 1L).set("COL5", "c")
                .build();
        engine2.persist("TEST", entry);

        entity.removeColumn("USER");
        entity.removeColumn("COL2");
        engine2.updateEntity(entity.build());

        // as the fields were removed the entity mapping ignores the fields.
        System.out.println("> " + engine2.getMetadata("TEST"));
        entry = entry().set("COL1", 2).set("COL2", true).set("COL3", 2d).set("COL4", 1L).set("COL5", "c")
                .build();
        engine2.persist("TEST", entry);


        test = engine2.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));

        entity.addColumn("COL6", BLOB).addColumn("COL7", DOUBLE);
        engine2.updateEntity(entity.build());

        entry = entry().set("COL1", 3).set("COL2", true).set("USER", 2d).set("COL4", 1L).set("COL5", "c").set("COL6", new BlobTest(1, "")).set("COL7", 2d)
                .build();
        engine2.persist("TEST", entry);

        test = engine2.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));
        assertEquals(BLOB, test.get("COL6"));
        assertEquals(DOUBLE, test.get("COL7"));

    }

    /**
     * Tests that {@link AbstractDatabaseEngine#updateEntity(DbEntity)} with a "none" schema policy
     * still creates the in-memory {@link MappedEntity} with the prepared statements for the entities.
     */
    @Test
    public void updateEntityNoneSchemaPolicyCreatesInMemoryPreparedStmtsTest() throws DatabaseEngineException, DatabaseFactoryException {
        dropSilently("TEST");
        engine.removeEntity("TEST");

        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1")
                .build();

        engine.addEntity(entity);

        properties.setProperty(SCHEMA_POLICY, "none");
        DatabaseEngine schemaNoneEngine = DatabaseFactory.getConnection(properties);

        EntityEntry entry = entry()
                .set("COL1", 1)
                .set("COL2", true)
                .set("COL3", 1d)
                .set("COL4", 1L)
                .set("COL5", "1")
                .build();

        try {
            schemaNoneEngine.persist(entity.getName(), entry);
            fail("Should throw an exception if trying to persist an entity before calling addEntity/updateEntity a first time");
        } catch (final DatabaseEngineException e) {
            assertTrue("Should fail because the entity is still unknown to this DatabaseEngine instance",
                e.getMessage().contains("Unknown entity"));
        }

        schemaNoneEngine.updateEntity(entity);

        assertTrue("DatabaseEngine should be aware of the entity even with a NONE schema policy.", schemaNoneEngine.containsEntity(entity.getName()));

        // Persist the entry and make sure it was successful
        schemaNoneEngine.persist(entity.getName(), entry);
        List<Map<String, ResultColumn>> result = schemaNoneEngine.query(select(all()).from(table("TEST")));

        assertEquals("There should be only one entry in the table.", 1, result.size());

        Map<String, ResultColumn> resultEntry = result.get(0);

        assertEquals("COL1 was successfully inserted", 1, resultEntry.get("COL1").toInt().intValue());
        assertEquals("COL2 was successfully inserted", true, resultEntry.get("COL2").toBoolean());
        assertEquals("COL3 was successfully inserted", 1.0, resultEntry.get("COL3").toDouble(), 0);
        assertEquals("COL4 was successfully inserted", 1L, resultEntry.get("COL4").toLong().longValue());
        assertEquals("COL5 was successfully inserted", "1", resultEntry.get("COL5").toString());
    }

    /**
     * Tests that {@link AbstractDatabaseEngine#updateEntity(DbEntity)} with a "none" schema policy
     * doesn't execute DDL.
     */
    @Test
    public void updateEntityNoneSchemaPolicyDoesntExecuteDDL() throws DatabaseFactoryException {
        dropSilently("TEST");

        properties.setProperty(SCHEMA_POLICY, "none");
        DatabaseEngine schemaNoneEngine = DatabaseFactory.getConnection(properties);

        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1")
                .build();

        try {
            schemaNoneEngine.updateEntity(entity);
            schemaNoneEngine.query(select(all()).from(table(entity.getName())));
            fail("Should have failed because updateEntity with schema policy NONE doesn't execute DDL");
        } catch (final DatabaseEngineException e) {
            // Should fail because because updateEntity with schema policy NONE doesn't execute DDL
        }
    }

    @Test
    public void addDropColumnNonExistentDropCreateTest() throws DatabaseEngineException {
        dropSilently("TEST");
        engine.removeEntity("TEST");

        DbEntity.Builder entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1");
        engine.updateEntity(entity.build());

        Map<String, DbColumnType> test = engine.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(BOOLEAN, test.get("COL2"));
        assertEquals(DOUBLE, test.get("COL3"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));

        dropSilently("TEST");
        engine.removeEntity("TEST");

        entity.removeColumn("COL3");
        entity.removeColumn("COL2");
        engine.updateEntity(entity
                .build());

        test = engine.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));

        dropSilently("TEST");
        engine.removeEntity("TEST");

        entity.addColumn("COL6", BLOB).addColumn("COL7", DOUBLE, DbColumnConstraint.NOT_NULL);
        engine.updateEntity(entity
                .build());

        test = engine.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));
        assertEquals(BLOB, test.get("COL6"));
        assertEquals(DOUBLE, test.get("COL7"));
    }

    @Test
    public void addDropColumnNonExistentTest() throws Exception {
        dropSilently("TEST");
        engine.removeEntity("TEST");

        DatabaseEngine engine = this.engine.duplicate(new Properties() {
            {
                setProperty(SCHEMA_POLICY, "create");
            }
        }, true);

        DbEntity.Builder entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1");
        engine.updateEntity(entity.build());

        Map<String, DbColumnType> test = engine.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(BOOLEAN, test.get("COL2"));
        assertEquals(DOUBLE, test.get("COL3"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));

        dropSilently("TEST");
        engine.removeEntity("TEST");

        entity.removeColumn("COL3");
        entity.removeColumn("COL2");
        engine.updateEntity(entity.build());

        test = engine.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));

        dropSilently("TEST");
        engine.removeEntity("TEST");

        entity.addColumn("COL6", BLOB).addColumn("COL7", DOUBLE, DbColumnConstraint.NOT_NULL);
        engine.updateEntity(entity.build());

        test = engine.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));
        assertEquals(BLOB, test.get("COL6"));
        assertEquals(DOUBLE, test.get("COL7"));
    }

    @Test
    public void testInsertNullCLOB() throws Exception {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", STRING)
                .addColumn("COL2", CLOB)
                .build();
        engine.addEntity(entity);


        EntityEntry entry = entry().set("COL1", "CENINHAS")
                .build();

        engine.persist("TEST", entry);

        List<Map<String, ResultColumn>> result = engine.query(select(all()).from(table("TEST")));
        assertEquals("CENINHAS", result.get(0).get("COL1").toString());
        System.out.println(result.get(0).get("COL2"));
        assertNull(result.get(0).get("COL2").toString());
    }


    @Test
    public void testCLOB() throws Exception {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", STRING)
                .addColumn("COL2", CLOB)
                .build();

        engine.addEntity(entity);

        StringBuilder sb = new StringBuilder();
        StringBuilder sb1 = new StringBuilder();
        for (int x = 0; x < 500000; x++) {
            sb.append(x);
            sb1.append(x * 2);
        }
        String initialClob = sb.toString();
        String updateClob = sb1.toString();

        EntityEntry entry = entry().set("COL1", "CENINHAS").set("COL2", initialClob)
                .build();

        engine.persist("TEST", entry);

        List<Map<String, ResultColumn>> result = engine.query(select(all()).from(table("TEST")));


        assertEquals("CENINHAS", result.get(0).get("COL1").toString());
        assertEquals(initialClob, result.get(0).get("COL2").toString());

        Update upd = update(table("TEST")).set(eq(column("COL2"), lit("?"))).where(eq(column("COL1"), k("CENINHAS")));

        engine.createPreparedStatement("upd", upd);

        engine.setParameters("upd", updateClob);

        engine.executePSUpdate("upd");

        result = engine.query(select(all()).from(table("TEST")));
        assertEquals("CENINHAS", result.get(0).get("COL1").toString());
        assertEquals(updateClob, result.get(0).get("COL2").toString());

    }

    @Test
    public void testCLOBEncoding() throws Exception {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", STRING)
                .addColumn("COL2", CLOB)
                .build();

        engine.addEntity(entity);

        String initialClob = "áãç";
        String updateClob = "áãç_áãç";

        EntityEntry entry = entry().set("COL1", "CENINHAS").set("COL2", initialClob)
                .build();

        engine.persist("TEST", entry);

        List<Map<String, ResultColumn>> result = engine.query(select(all()).from(table("TEST")));
        assertEquals("CENINHAS", result.get(0).get("COL1").toString());
        assertEquals(initialClob, result.get(0).get("COL2").toString());

        Update upd = update(table("TEST")).set(eq(column("COL2"), lit("?"))).where(eq(column("COL1"), k("CENINHAS")));

        engine.createPreparedStatement("upd", upd);

        engine.setParameters("upd", updateClob);

        engine.executePSUpdate("upd");

        result = engine.query(select(all()).from(table("TEST")));
        assertEquals("CENINHAS", result.get(0).get("COL1").toString());
        assertEquals(updateClob, result.get(0).get("COL2").toString());

    }

    @Test
    public void testPersistOverrideAutoIncrement() throws Exception {
        DbEntity entity = dbEntity()
                .name("MYTEST")
                .addColumn("COL1", INT, true)
                .addColumn("COL2", STRING)
                .build();


        engine.addEntity(entity);

        EntityEntry ent = entry().set("COL2", "CENAS1")
                .build();
        engine.persist("MYTEST", ent);
        ent = entry().set("COL2", "CENAS2")
                .build();
        engine.persist("MYTEST", ent);

        ent = entry().set("COL2", "CENAS3").set("COL1", 3)
                .build();
        engine.persist("MYTEST", ent, false);

        ent = entry().set("COL2", "CENAS5").set("COL1", 5)
                .build();
        engine.persist("MYTEST", ent, false);


        ent = entry().set("COL2", "CENAS6")
                .build();
        engine.persist("MYTEST", ent);

        ent = entry().set("COL2", "CENAS7")
                .build();
        engine.persist("MYTEST", ent);

        final List<Map<String, ResultColumn>> query = engine.query("SELECT * FROM " + quotize("MYTEST", engine.escapeCharacter()));
        for (Map<String, ResultColumn> stringResultColumnMap : query) {
            assertTrue(stringResultColumnMap.get("COL2").toString().endsWith(stringResultColumnMap.get("COL1").toString()));
        }
        engine.close();
    }

    @Test
    public void testPersistOverrideAutoIncrement2() throws Exception {
        String APP_ID = "APP_ID";
        DbColumn APP_ID_COLUMN = new DbColumn.Builder().name(APP_ID).type(INT).build();
        String STM_TABLE = "FDZ_APP_STREAM";
        String STM_ID = "STM_ID";
        String STM_NAME = "STM_NAME";
        DbEntity STREAM = dbEntity().name(STM_TABLE)
                .addColumn(APP_ID_COLUMN)
                .addColumn(STM_ID, INT, true)
                .addColumn(STM_NAME, STRING, NOT_NULL)
                .pkFields(STM_ID, APP_ID)
                .build();

        engine.addEntity(STREAM);

        EntityEntry ent = entry().set(APP_ID, 1).set(STM_ID, 1).set(STM_NAME, "NAME1")
                .build();
        engine.persist(STM_TABLE, ent);

        ent = entry().set(APP_ID, 2).set(STM_ID, 1).set(STM_NAME, "NAME1")
                .build();
        engine.persist(STM_TABLE, ent, false);

        ent = entry().set(APP_ID, 2).set(STM_ID, 2).set(STM_NAME, "NAME2")
                .build();
        engine.persist(STM_TABLE, ent);

        ent = entry().set(APP_ID, 1).set(STM_ID, 10).set(STM_NAME, "NAME10")
                .build();
        engine.persist(STM_TABLE, ent, false);

        ent = entry().set(APP_ID, 1).set(STM_ID, 2).set(STM_NAME, "NAME11")
                .build();
        engine.persist(STM_TABLE, ent);

        ent = entry().set(APP_ID, 2).set(STM_ID, 11).set(STM_NAME, "NAME11")
                .build();
        engine.persist(STM_TABLE, ent, false);

        final List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table(STM_TABLE)));
        for (Map<String, ResultColumn> stringResultColumnMap : query) {
            System.out.println(stringResultColumnMap);
            assertTrue("Assert Stream Name with id", stringResultColumnMap.get(STM_NAME).toString().endsWith(stringResultColumnMap.get(STM_ID).toString()));
        }

    }

    @Test
    public void testPersistOverrideAutoIncrement3() throws Exception {
        DbEntity entity = dbEntity()
                .name("MYTEST")
                .addColumn("COL1", INT, true)
                .addColumn("COL2", STRING)
                .build();


        engine.addEntity(entity);

        EntityEntry ent = entry().set("COL2", "CENAS1").set("COL1", 1)
                .build();
        engine.persist("MYTEST", ent, false);

        ent = entry().set("COL2", "CENAS2")
                .build();
        engine.persist("MYTEST", ent);


        ent = entry().set("COL2", "CENAS5").set("COL1", 5)
                .build();
        engine.persist("MYTEST", ent, false);

        ent = entry().set("COL2", "CENAS6")
                .build();
        engine.persist("MYTEST", ent);

        final List<Map<String, ResultColumn>> query = engine.query("SELECT * FROM " + quotize("MYTEST", engine.escapeCharacter()));
        for (Map<String, ResultColumn> stringResultColumnMap : query) {
            System.out.println(stringResultColumnMap);
            assertTrue(stringResultColumnMap.get("COL2").toString().endsWith(stringResultColumnMap.get("COL1").toString()));
        }
        engine.close();
    }

    @Test
    public void testTruncateTable() throws Exception {
        test5Columns();

        engine.persist("TEST", entry().set("COL1", 5)
                .build());

        Truncate truncate = new Truncate(table("TEST"));

        engine.executeUpdate(truncate);

        final List<Map<String, ResultColumn>> test = engine.query(select(all()).from(table("TEST")));
        assertTrue("Test truncate query empty?", test.isEmpty());

    }

    @Test
    public void testRenameTables() throws Exception {
        String oldName = "TBL_OLD";
        String newName = "TBL_NEW";

        // Drop tables for sanity.
        dropSilently(oldName, newName);

        // Create the "old" table.
        DbEntity entity = dbEntity()
                .name(oldName)
                .addColumn("timestamp", INT)
                .build();
        engine.addEntity(entity);
        engine.persist(oldName, entry().set("timestamp", 20)
                .build());

        // Rename it
        Rename rename = new Rename(table(oldName), table(newName));
        engine.executeUpdate(rename);

        // Check whether the schema matches
        final Map<String, DbColumnType> metaMap = new LinkedHashMap<>();
        metaMap.put("timestamp", INT);
        assertEquals("Metamap ok?", metaMap, engine.getMetadata(newName));

        // Check the data
        List<Map<String, ResultColumn>> resultSet = engine.query(select(all()).from(table(newName)));
        assertEquals("Count ok?", 1, resultSet.size());

        assertEquals("Content ok?", 20, (int) resultSet.get(0).get("timestamp").toInt());

        dropSilently(newName);
    }

    /**
     * Drops a list of tables silently (i.e. if it fails, it will just keep on).
     *
     * @param tables The tables that we want to drop.
     */
    private void dropSilently(String... tables) {
        for (String table : tables) {
            try {
                engine.dropEntity(dbEntity().name(table).build());
            } catch (final Throwable e) {
            }
        }
    }

    @Test
    public void testLikeWithTransformation() throws Exception {
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 5).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 5).set("COL5", "TESTE")
                .build());
        engine.persist("TEST", entry().set("COL1", 5).set("COL5", "TeStE")
                .build());
        engine.persist("TEST", entry().set("COL1", 5).set("COL5", "tesTte")
                .build());

        List<Map<String, ResultColumn>> query = engine.query(
            select(all()).from(table("TEST")).where(like(udf("lower", column("COL5")), k("%teste%")))
        );
        assertEquals(3, query.size());
        query = engine.query(select(all()).from(table("TEST")).where(like(udf("lower", column("COL5")), k("%tt%"))));
        assertEquals(1, query.size());

    }

    @Test(expected = DatabaseEngineException.class)
    public void fkTestRemoveRowReferencedByForeignKey() throws DatabaseEngineException {
        DbEntity e1 = dbEntity()
                .name("TEST1")
                .addColumn("COL1", INT)
                .pkFields("COL1")
                .build();

        engine.addEntity(e1);

        DbEntity e2 = dbEntity().name("TEST2").addColumn("COL2", INT, true).addColumn("COL1", INT)
                .addFk(dbFk()
                        .addColumn("COL1")
                        .foreignTable("TEST1")
                        .addForeignColumn("COL1")
                        .build())
                .pkFields("COL2")
                .build();

        engine.addEntity(e2);

        engine.persist("TEST1", entry().set("COL1", 1)
                .build());
        engine.persist("TEST2", entry().set("COL1", 1)
                .build());
        engine.executeUpdate(delete(table("TEST1")));
    }

    @Test
    public void fkTestRemoveRowPreviouslyReferencedByForeignKey() throws DatabaseEngineException, DatabaseFactoryException {
        DbEntity e1 = dbEntity()
                .name("TEST1")
                .addColumn("COL1", INT)
                .pkFields("COL1")
                .build();

        engine.addEntity(e1);

        DbEntity e2 = dbEntity().name("TEST2").addColumn("COL2", INT, true).addColumn("COL1", INT)
                .addFk(dbFk()
                        .addColumn("COL1")
                        .foreignTable("TEST1")
                        .addForeignColumn("COL1")
                        .build())
                .pkFields("COL2")
                .build();

        engine.addEntity(e2);

        engine.persist("TEST1", entry().set("COL1", 1)
                .build());
        engine.persist("TEST2", entry().set("COL1", 1)
                .build());


        // Clear TEST2 FK's
        e2 = e2.newBuilder().clearFks().build();

        // PDB property SCHEMA_POLICY must not be drop-create, otherwise the entity
        // will be dropped and the update of the FK's isn't properly tested.
        engine.close();
        properties.setProperty(SCHEMA_POLICY, "create");
        engine = DatabaseFactory.getConnection(properties);

        engine.updateEntity(e2);

        engine.executeUpdate(delete(table("TEST1")));

        // Just to make sure that the table was not dropped on the previous update
        assertEquals(1, engine.query(select(all()).from(table("TEST2"))).size());
    }

    @Test
    public void createSequenceOnLongColumnTest() throws Exception {
        DbEntity entity =
                dbEntity()
                        .name("TEST")
                        .addColumn("COL1", INT)
                        .addColumn("COL2", BOOLEAN)
                        .addColumn("COL3", DOUBLE)
                        .addColumn("COL4", LONG, true)
                        .addColumn("COL5", STRING)
                        .build();
        engine.addEntity(entity);
        engine.persist("TEST", entry().set("COL1", 1).set("COL2", true)
                .build());
        List<Map<String, ResultColumn>> test = engine.query(select(all()).from(table("TEST")));
        assertEquals("col1 ok?", 1, (int) test.get(0).get("COL1").toInt());
        assertTrue("col2 ok?", test.get(0).get("COL2").toBoolean());
        assertEquals("col4 ok?", 1L, (long) test.get(0).get("COL4").toLong());

    }

    @Test
    public void insertWithNoAutoIncAndThatResumeTheAutoIncTest() throws DatabaseEngineException {
        DbEntity entity =
                dbEntity()
                        .name("TEST")
                        .addColumn("COL1", INT)
                        .addColumn("COL2", BOOLEAN)
                        .addColumn("COL3", DOUBLE)
                        .addColumn("COL4", LONG, true)
                        .addColumn("COL5", STRING)
                        .build();
        engine.addEntity(entity);
        engine.persist("TEST", entry().set("COL1", 1).set("COL2", true)
                .build());
        List<Map<String, ResultColumn>> test = engine.query(select(all()).from(table("TEST")).orderby(column("COL4")));
        assertEquals("col4 ok?", 1L, (long) test.get(0).get("COL4").toLong());

        engine.persist("TEST", entry().set("COL1", 1).set("COL2", true).set("COL4", 2)
                .build(), false);
        test = engine.query(select(all()).from(table("TEST")).orderby(column("COL4")));
        assertEquals("col4 ok?", 2L, (long) test.get(1).get("COL4").toLong());

        engine.persist("TEST", entry().set("COL1", 1).set("COL2", true).build());
        test = engine.query(select(all()).from(table("TEST")).orderby(column("COL4")));
        assertEquals("col4 ok?", 3L, (long) test.get(2).get("COL4").toLong());

        engine.persist("TEST", entry().set("COL1", 1).set("COL2", true).set("COL4", 4)
                .build(), false);
        test = engine.query(select(all()).from(table("TEST")).orderby(column("COL4")));
        assertEquals("col4 ok?", 4L, (long) test.get(3).get("COL4").toLong());

        engine.persist("TEST", entry().set("COL1", 1).set("COL2", true)
                .build());
        test = engine.query(select(all()).from(table("TEST")).orderby(column("COL4")));
        assertEquals("col4 ok?", 5L, (long) test.get(4).get("COL4").toLong());

        engine.persist("TEST", entry().set("COL1", 1).set("COL2", true).set("COL4", 6)
                .build(), false);
        test = engine.query(select(all()).from(table("TEST")).orderby(column("COL4")));
        assertEquals("col4 ok?", 6L, (long) test.get(5).get("COL4").toLong());

        engine.persist("TEST", entry().set("COL1", 1).set("COL2", true).set("COL4", 7)
                .build(), false);
        test = engine.query(select(all()).from(table("TEST")).orderby(column("COL4")));
        assertEquals("col4 ok?", 7L, (long) test.get(6).get("COL4").toLong());

        engine.persist("TEST", entry().set("COL1", 1).set("COL2", true)
                .build());
        test = engine.query(select(all()).from(table("TEST")).orderby(column("COL4")));
        assertEquals("col4 ok?", 8L, (long) test.get(7).get("COL4").toLong());
    }


    protected void test5Columns() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .build();

        engine.addEntity(entity);
    }

    protected void userRolePermissionSchema() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("USER")
                .addColumn("COL1", INT, true)
                .pkFields("COL1")
                .build();

        engine.addEntity(entity);

        entity = dbEntity()
                .name("ROLE")
                .addColumn("COL1", INT, true)
                .pkFields("COL1")
                .build();

        engine.addEntity(entity);

        entity = dbEntity()
                .name("USER_ROLE")
                .addColumn("COL1", INT)
                .addColumn("COL2", INT)
                .addFk(dbFk()
                                .addColumn("COL1")
                                .foreignTable("USER")
                                .addForeignColumn("COL1")
                                .build(),
                        dbFk()
                                .addColumn("COL2")
                                .foreignTable("ROLE")
                                .addForeignColumn("COL1")
                                .build()
                )
                .pkFields("COL1", "COL2")
                .build();

        engine.addEntity(entity);
    }

    @Test
    public void testAndWhere() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "TESTE")
                .build());
        engine.persist("TEST", entry().set("COL1", 3).set("COL5", "TeStE")
                .build());
        engine.persist("TEST", entry().set("COL1", 4).set("COL5", "tesTte")
                .build());

        final List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")).where(eq(column("COL1"), k(1))).andWhere(eq(column("COL5"), k("teste"))));

        assertEquals("Resultset must have only one result", 1, query.size());
        assertEquals("COL1 must be 1", 1, query.get(0).get("COL1").toInt().intValue());
        assertEquals("COL5 must be teste", "teste", query.get(0).get("COL5").toString());
    }

    @Test
    public void testAndWhereMultiple() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "TESTE")
                .build());
        engine.persist("TEST", entry().set("COL1", 3).set("COL5", "TeStE")
                .build());
        engine.persist("TEST", entry().set("COL1", 4).set("COL5", "tesTte")
                .build());

        final List<Map<String, ResultColumn>> query = engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(
                                or(
                                        eq(column("COL1"), k(1)),
                                        eq(column("COL1"), k(4))
                                )
                        )
                        .andWhere(
                                or(
                                        eq(column("COL5"), k("teste")),
                                        eq(column("COL5"), k("TESTE"))
                                )
                        )
        );

        assertEquals("Resultset must have only one result", 1, query.size());
        assertEquals("COL1 must be 1", 1, query.get(0).get("COL1").toInt().intValue());
        assertEquals("COL5 must be teste", "teste", query.get(0).get("COL5").toString());
    }

    @Test
    public void testAndWhereMultipleCheckAndEnclosed() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "TESTE")
                .build());
        engine.persist("TEST", entry().set("COL1", 3).set("COL5", "TeStE")
                .build());
        engine.persist("TEST", entry().set("COL1", 4).set("COL5", "tesTte")
                .build());

        final List<Map<String, ResultColumn>> query = engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(
                                or(
                                        eq(column("COL1"), k(1)),
                                        eq(column("COL1"), k(4))
                                )
                        )
                        .andWhere(
                                or(
                                        eq(column("COL5"), k("teste")),
                                        eq(column("COL5"), k("tesTte"))
                                )
                        )
        );

        assertEquals("Resultset must have only one result", 2, query.size());
        assertEquals("COL1 must be 1", 1, query.get(0).get("COL1").toInt().intValue());
        assertEquals("COL5 must be teste", "teste", query.get(0).get("COL5").toString());
        assertEquals("COL1 must be 1", 4, query.get(1).get("COL1").toInt().intValue());
        assertEquals("COL5 must be teste", "tesTte", query.get(1).get("COL5").toString());
    }

    @Test
    public void testStringAgg() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "TESTE")
                .build());
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "TeStE")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "tesTte")
                .build());

        final List<Map<String, ResultColumn>> query = engine.query(
                select(column("COL1"), stringAgg(column("COL5")).alias("agg"))
                        .from(table("TEST"))
                        .groupby(column("COL1"))
                        .orderby(column("COL1").asc())
        );

        assertEquals("Resultset must have only 2 results", 2, query.size());
        assertEquals("COL1 must be 1", 1, query.get(0).get("COL1").toInt().intValue());
        assertEquals("COL5 must be TESTE,teste", "TESTE,teste", query.get(0).get("agg").toString());
        assertEquals("COL1 must be 2", 2, query.get(1).get("COL1").toInt().intValue());
        assertEquals("COL5 must be TeStE,tesTte", "TeStE,tesTte", query.get(1).get("agg").toString());
    }

    @Test
    public void testStringAggDelimiter() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "TESTE")
                .build());
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "TeStE")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "tesTte")
                .build());

        final List<Map<String, ResultColumn>> query = engine.query(
                select(column("COL1"), stringAgg(column("COL5")).delimiter(';').alias("agg"))
                        .from(table("TEST"))
                        .groupby(column("COL1"))
                        .orderby(column("COL1").asc())
        );

        assertEquals("Resultset must have only 2 results", 2, query.size());
        assertEquals("COL1 must be 1", 1, query.get(0).get("COL1").toInt().intValue());
        assertEquals("COL5 must be TESTE;teste", "TESTE;teste", query.get(0).get("agg").toString());
        assertEquals("COL1 must be 2", 2, query.get(1).get("COL1").toInt().intValue());
        assertEquals("COL5 must be TeStE;tesTte", "TeStE;tesTte", query.get(1).get("agg").toString());
    }

    @Test
    public void testStringAggDistinct() throws DatabaseEngineException {
        if (!this.engine.isStringAggDistinctCapable()) {
            return;
        }
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "TeStE")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "tesTte")
                .build());

        final List<Map<String, ResultColumn>> query = engine.query(
                select(column("COL1"), stringAgg(column("COL5")).distinct().alias("agg"))
                        .from(table("TEST"))
                        .groupby(column("COL1"))
                        .orderby(column("COL1").asc())
        );

        assertEquals("Resultset must have only 2 results", 2, query.size());
        assertEquals("COL1 must be 1", 1, query.get(0).get("COL1").toInt().intValue());
        assertEquals("COL5 must be teste", "teste", query.get(0).get("agg").toString());
        assertEquals("COL1 must be 2", 2, query.get(1).get("COL1").toInt().intValue());
        assertEquals("COL5 must be TeStE,tesTte", "TeStE,tesTte", query.get(1).get("agg").toString());
    }

    @Test
    public void testStringAggNotStrings() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "TESTE")
                .build());
        engine.persist("TEST", entry().set("COL1", 1).set("COL5", "teste")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "TeStE")
                .build());
        engine.persist("TEST", entry().set("COL1", 2).set("COL5", "tesTte")
                .build());

        final List<Map<String, ResultColumn>> query = engine.query(
                select(column("COL1"), stringAgg(column("COL1")).alias("agg"))
                        .from(table("TEST"))
                        .groupby(column("COL1"))
                        .orderby(column("COL1").asc())
        );

        assertEquals("Resultset must have only 2 results", 2, query.size());
        assertEquals("COL1 must be 1", 1, query.get(0).get("COL1").toInt().intValue());
        assertEquals("COL5 must be 1,1", "1,1", query.get(0).get("agg").toString());
        assertEquals("COL1 must be 2", 2, query.get(1).get("COL1").toInt().intValue());
        assertEquals("COL5 must be 2,2", "2,2", query.get(1).get("agg").toString());
    }

    @Test
    @Category(SkipTestCockroachDB.class)
    public void dropPrimaryKeyWithOneColumnTest() throws Exception {
        DbEntity entity =
                dbEntity()
                        .name("TEST")
                        .addColumn("COL1", INT)
                        .addColumn("COL2", BOOLEAN)
                        .addColumn("COL3", DOUBLE)
                        .addColumn("COL4", LONG)
                        .addColumn("COL5", STRING)
                        .pkFields("COL1")
                        .build();
        engine.addEntity(entity);
        engine.executeUpdate(dropPK(table("TEST")));
    }

    @Test
    @Category(SkipTestCockroachDB.class)
    public void dropPrimaryKeyWithTwoColumnsTest() throws Exception {
        DbEntity entity =
                dbEntity()
                        .name("TEST")
                        .addColumn("COL1", INT)
                        .addColumn("COL2", BOOLEAN)
                        .addColumn("COL3", DOUBLE)
                        .addColumn("COL4", LONG)
                        .addColumn("COL5", STRING)
                        .pkFields("COL1", "COL4")
                        .build();
        engine.addEntity(entity);
        engine.executeUpdate(dropPK(table("TEST")));
    }

    @Test
    public void alterColumnWithConstraintTest() throws DatabaseEngineException {
        DbEntity entity =
                dbEntity()
                        .name("TEST")
                        .addColumn("COL1", INT)
                        .addColumn("COL2", BOOLEAN)
                        .addColumn("COL3", DOUBLE)
                        .addColumn("COL4", LONG)
                        .addColumn("COL5", STRING)
                        .build();

        engine.addEntity(entity);

        engine.executeUpdate(new AlterColumn(table("TEST"), new DbColumn.Builder().name("COL1").type(DbColumnType.INT).addConstraint(DbColumnConstraint
                .NOT_NULL)
                .build()));
    }

    @Test
    @Category(SkipTestCockroachDB.class)
    public void alterColumnToDifferentTypeTest() throws DatabaseEngineException {
        DbEntity entity =
                dbEntity()
                        .name("TEST")
                        .addColumn("COL1", INT)
                        .addColumn("COL2", BOOLEAN)
                        .addColumn("COL3", DOUBLE)
                        .addColumn("COL4", LONG)
                        .addColumn("COL5", STRING)
                        .build();

        engine.addEntity(entity);

        engine.executeUpdate(new AlterColumn(table("TEST"), dbColumn().name("COL1").type(DbColumnType.STRING)
                .build()));
    }

    @Test
    public void createTableWithDefaultsTest() throws DatabaseEngineException, DatabaseFactoryException {
        DbEntity.Builder entity =
                dbEntity()
                        .name("TEST")
                        .addColumn("COL1", INT, new K(1))
                        .addColumn("COL2", BOOLEAN, new K(false))
                        .addColumn("COL3", DOUBLE, new K(2.2d))
                        .addColumn("COL4", LONG, new K(3L))
                        .pkFields("COL1");

        engine.addEntity(entity.build());

        final String ec = engine.escapeCharacter();
        engine.executeUpdate("INSERT INTO " + quotize("TEST", ec) + " (" + quotize("COL1", ec) + ") VALUES (10)");

        List<Map<String, ResultColumn>> test = engine.query(select(all()).from(table("TEST")));
        assertEquals("Check size of records", 1, test.size());
        Map<String, ResultColumn> record = test.get(0);
        assertEquals("Check COL1", 10, record.get("COL1").toInt().intValue());
        assertEquals("Check COL2", false, record.get("COL2").toBoolean());
        assertEquals("Check COL3", 2.2d, record.get("COL3").toDouble(), 0);
        assertEquals("Check COL4", 3L, record.get("COL4").toLong().longValue());


        final DbEntity entity1 = entity.addColumn("COL5", STRING, new K("mantorras"), NOT_NULL)
                .addColumn("COL6", BOOLEAN, new K(true), NOT_NULL)
                .addColumn("COL7", INT, new K(7), NOT_NULL).build();

        final Properties propertiesCreate = new Properties();
        for (Map.Entry<Object, Object> prop : properties.entrySet()) {
            propertiesCreate.setProperty(prop.getKey().toString(), prop.getValue().toString());
        }
        propertiesCreate.setProperty(SCHEMA_POLICY, "create");

        final DatabaseEngine connection2 = DatabaseFactory.getConnection(propertiesCreate);
        connection2.updateEntity(entity1);

        test = connection2.query(select(all()).from(table("TEST")));
        assertEquals("Check size of records", 1, test.size());
        record = test.get(0);
        assertEquals("Check COL1", 10, record.get("COL1").toInt().intValue());
        assertEquals("Check COL2", false, record.get("COL2").toBoolean());
        assertEquals("Check COL3", 2.2d, record.get("COL3").toDouble(), 1e-9);
        assertEquals("Check COL4", 3L, record.get("COL4").toLong().longValue());
        assertEquals("Check COL5", "mantorras", record.get("COL5").toString());
        assertEquals("Check COL6", true, record.get("COL6").toBoolean());
        assertEquals("Check COL7", 7, record.get("COL7").toInt().intValue());
        connection2.close();
    }

    @Test
    public void defaultValueOnBooleanColumnsTest() throws DatabaseEngineException {
        DbEntity.Builder entity =
                dbEntity()
                        .name("TEST")
                        .addColumn("COL1", INT, new K(1))
                        .addColumn("COL2", BOOLEAN, new K(false), NOT_NULL)
                        .addColumn("COL3", DOUBLE, new K(2.2d))
                        .addColumn("COL4", LONG, new K(3L))
                        .pkFields("COL1");

        engine.addEntity(entity.build());

        engine.persist("TEST", entry().build());
        Map<String, ResultColumn> row = engine.query(select(all()).from(table("TEST"))).get(0);

        assertEquals("", 1, row.get("COL1").toInt().intValue());
        assertFalse("", row.get("COL2").toBoolean());
        assertEquals("", 2.2d, row.get("COL3").toDouble(), 0D);
        assertEquals("", 3L, row.get("COL4").toLong().longValue());
    }

    @Test
    public void upperTest() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL5", "ola").build());
        assertEquals("text is uppercase", "OLA", engine.query(select(upper(column("COL5")).alias("RES")).from(table("TEST"))).get(0).get("RES").toString());
    }

    @Test
    public void lowerTest() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL5", "OLA").build());
        assertEquals("text is lowercase", "ola", engine.query(select(lower(column("COL5")).alias("RES")).from(table("TEST"))).get(0).get("RES").toString());
    }

    @Test
    public void internalFunctionTest() throws DatabaseEngineException {
        test5Columns();
        engine.persist("TEST", entry().set("COL5", "OLA").build());
        assertEquals("text is uppercase", "ola", engine.query(select(f("LOWER", column("COL5")).alias("RES")).from(table("TEST"))).get(0).get("RES")
                .toString());
    }

    @Test
    public void entityEntryHashcodeTest() {
        Map<String, Object> map = new HashMap<>();
        map.put("id1", "val1");
        map.put("id2", "val2");
        map.put("id3", "val3");
        map.put("id4", "val4");

        EntityEntry entry = entry()
                .set(map)
                .build();

        assertEquals("entry's hashCode() matches map's hashCode()", map.hashCode(), entry.hashCode());
    }

    /**
     * Tests that creating a {@link DatabaseEngine} using try-with-resources will close the engine once the block is
     * exited from.
     *
     * @since 2.1.12
     */
    @Test
    public void tryWithResourcesClosesEngine() {
        final AtomicInteger closeCallCount = new AtomicInteger(0);

        try(final DatabaseEngine ignored = new MockUp<DatabaseEngine>() {
            @Mock
            void close() {
                closeCallCount.incrementAndGet();
            }
        }.getMockInstance()) {
            assertEquals(
                    "DatabaseEngine#close method should not be called within the try-with-resources block",
                    0,
                    closeCallCount.get()
            );
        }

        assertEquals(
                "DatabaseEngine#close method should be called after exiting try-with-resources block",
                1,
                closeCallCount.get()
        );
    }

    /**
     * Test that closing a database engine a 'create-drop' policywith multiple entities closes all insert statements associated with each
     * entity, regardless of the schema policy used.
     *
     * Each entity is associated with 3 prepared statements. This test ensures that 3 PSs per entity are closed.
     *
     * @throws DatabaseEngineException  If something goes wrong while adding an entity to the engine.
     * @throws DatabaseFactoryException If the database engine class specified in the properties does not exist.
     * @since 2.1.13
     */
    @Test
    public void closingAnEngineUsingTheCreateDropPolicyShouldDropAllEntities()
            throws DatabaseEngineException, DatabaseFactoryException {

        // Force the schema policy to be 'create-drop'
        properties.setProperty(SCHEMA_POLICY, "create-drop");
        engine = DatabaseFactory.getConnection(properties);

        engine.addEntity(buildEntity("ENTITY-1"));
        engine.addEntity(buildEntity("ENTITY-2"));

        // Force invocation counting to start here
        new Expectations(engine) {};

        engine.close();

        new Verifications() {{
            engine.dropEntity((DbEntity) any); times = 2;
        }};

    }

    /**
     * Assesses whether the current row count is incremented if the .next()/.nextResult()
     * methods are called in the iterator.
     *
     * @throws DatabaseEngineException If a database access error happens.
     */
    @Test
    public void doesRowCountIncrementTest()
            throws DatabaseEngineException {

        final ResultIterator resultIterator = engine.iterator(select(all()).from(table("TEST")));


        assertEquals("The current row count should be 0 if the iteration hasn't started",
                0,
                resultIterator.getCurrentRowCount());

        // If the .next() method is called once then the current row count
        // should be updated to 1
        resultIterator.next();

        assertEquals("The current row count is equal to 1",
                1,
                resultIterator.getCurrentRowCount());

        // If for the same iterator the .nextResult() method is called 3 additional
        // times then the current row count should be updated to 4
        for(int i = 0; i < 3; i++) {
            resultIterator.nextResult();
        }

        assertEquals("The current row count is equal to 4",
                4,
                resultIterator.getCurrentRowCount());
    }
}
