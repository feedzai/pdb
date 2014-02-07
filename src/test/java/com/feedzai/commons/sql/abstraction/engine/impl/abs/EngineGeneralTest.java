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
import com.feedzai.commons.sql.abstraction.ddl.*;
import com.feedzai.commons.sql.abstraction.dml.K;
import com.feedzai.commons.sql.abstraction.dml.Truncate;
import com.feedzai.commons.sql.abstraction.dml.Update;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.dml.result.ResultIterator;
import com.feedzai.commons.sql.abstraction.engine.*;
import com.feedzai.commons.sql.abstraction.engine.testconfig.BlobTest;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.feedzai.commons.sql.abstraction.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.*;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnConstraint.NOT_NULL;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.*;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;
import static org.junit.Assert.*;

/**
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class EngineGeneralTest {

    protected DatabaseEngine engine;
    protected Properties properties;

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations();
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

    @BeforeClass
    public static void initStatic() {
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.TRACE);

    }

    @Before
    public void init() throws DatabaseEngineException, DatabaseFactoryException {
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
    public void createEntityTest() throws DatabaseEngineException, InterruptedException {

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
    public void createEntityWithTwoColumnsBeingPKTest() throws DatabaseEngineException, InterruptedException {

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
        } catch (DatabaseEngineException e) {
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
    public void createEntityWithSequencesTest() throws DatabaseEngineException, InterruptedException {

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
    public void createEntityWithIndexesTest() throws DatabaseEngineException, InterruptedException {

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
        assertEquals("COL1 ok?", (int) 2, (int) query.get(0).get("COL1").toInt());

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
        assertEquals("COL1 ok?", (int) 2, (int) query.get(0).get("COL1").toInt());

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
        assertEquals("COL1 ok?", (int) 1, (int) query.get(0).get("COL1").toInt());

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
        assertEquals("COL1 ok?", (int) 1, (int) res.get("COL1").toInt());

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
        assertEquals("COL1 ok?", (int) 2, (int) query.get(0).get("COL1").toInt());

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
        assertEquals("COL1 ok?", (int) 3, (int) query.get(1).get("COL1").toInt());

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
        assertEquals("COL1 ok?", (int) 2, (int) query.get(0).get("COL1").toInt());

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
        assertEquals("COL1 ok?", (int) 3, (int) query.get(1).get("COL1").toInt());

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
        } catch (DatabaseEngineException de) {
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
        } catch (DatabaseEngineException de) {
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
        } catch (DatabaseEngineException de) {
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
        } catch (DatabaseEngineException de) {
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
        } catch (DatabaseEngineException de) {
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

    @Test
    public void getGeneratedKeysWithNoAutoIncTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", INT)
                .build();


        engine.addEntity(entity);

        EntityEntry ee = entry()
                .set("COL1", 1)
                .set("COL2", 2)
                .build();

        Long persist = engine.persist("TEST", ee);

        assertNull("ret null?", persist);
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
        } catch (Exception e) {

        } finally {
            assertTrue("tx active?", engine.isTransactionActive());
            if (engine.isTransactionActive()) {
                engine.rollback();
            }
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
    public void dropEntityThatDoesNotExistTest() throws DatabaseEngineException {
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
    public void createViewTest() throws DatabaseEngineException {
        test5Columns();

        try {
            engine.executeUpdate("DROP VIEW " + StringUtils.quotize("VN", engine.escapeCharacter()));
        } catch (Throwable a) {
        }

        engine.executeUpdate(
                createView("VN").as(select(all()).from(table("TEST")))
        );
    }

    @Test
    public void createOrReplaceViewTest() throws DatabaseEngineException {
        test5Columns();

        engine.executeUpdate(
                createView("VN").as(select(all()).from(table("TEST"))).replace()
        );
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

        engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(
                                eq(column("COL2"), k(true))
                        )
        );
    }

    @Test
    public void booleanFalseComparisonTest() throws DatabaseEngineException {
        test5Columns();

        engine.query(
                select(all())
                        .from(table("TEST"))
                        .where(
                                eq(column("COL2"), k(false))
                        )
        );
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
                                        k(3.0).alias("three")).alias("sq_1"))
        );

        assertEquals("result ok?", 1000, (long) query.get(0).get("timestamp").toLong());
        assertEquals("result ok?", 1, (int) query.get(0).get("first").toInt());
        assertEquals("result ok?", 2L, (long) query.get(0).get("second").toLong());
        assertEquals("result ok?", 3.0, (double) query.get(0).get("third").toDouble(), 0.0);
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

        engine.createPreparedStatement("test", "SELECT * FROM " + StringUtils.quotize("TEST", engine.escapeCharacter()) + " WHERE " + StringUtils.quotize("COL1", engine.escapeCharacter()) + " = ?");
        engine.setParameters("test", new Object[]{1});
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

        List<Map<String, ResultColumn>> res = engine.query("SELECT * FROM " + StringUtils.quotize("TEST", engine.escapeCharacter()));

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

        final Map<String, DbColumnType> metaMap = new LinkedHashMap<String, DbColumnType>();
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
        assertArrayEquals(bb, result.get(0).get("COL2").<byte[]>toBlob());


        Update upd = update(table("TEST")).set(eq(column("COL2"), lit("?"))).where(eq(column("COL1"), k("CENINHAS")));

        engine.createPreparedStatement("upd", upd);

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(bb2);

        engine.setParameters("upd", bos.toByteArray());

        engine.executePSUpdate("upd");

        result = engine.query(select(all()).from(table("TEST")));
        assertEquals("CENINHAS", result.get(0).get("COL1").toString());
        assertArrayEquals(bb2, result.get(0).get("COL2").<byte[]>toBlob());

    }

    @Test
    public void testBlobString() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", STRING)
                .addColumn("COL2", BLOB)
                .build();

        engine.addEntity(entity);

        StringBuffer sb = new StringBuffer();
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
    public void testBlobJSON() throws DatabaseEngineException, DatabaseFactoryException {
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

        EntityEntry entry = entry().set("COL1", 1).set("COL2", true).set("USER", 2d).set("COL4", 1l).set("COL5", "c")
                .build();
        engine.persist("TEST", entry);

        entity.removeColumn("USER");
        entity.removeColumn("COL2");
        engine.updateEntity(entity
                .build());

        // as the fields were removed the entity mapping ignores the fields.
        entry = entry().set("COL1", 2).set("COL2", true).set("COL3", 2d).set("COL4", 1l).set("COL5", "c")
                .build();
        engine.persist("TEST", entry);


        test = engine.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));

        entity.addColumn("COL6", BLOB).addColumn("COL7", DOUBLE);
        engine.updateEntity(entity
                .build());

        entry = entry().set("COL1", 3).set("COL2", true).set("USER", 2d).set("COL4", 1l).set("COL5", "c").set("COL6", new BlobTest(1, "")).set("COL7", 2d)
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

        EntityEntry entry = entry().set("COL1", 1).set("COL2", true).set("USER", 2d).set("COL4", 1l).set("COL5", "c")
                .build();
        engine2.persist("TEST", entry);

        entity.removeColumn("USER");
        entity.removeColumn("COL2");
        engine2.updateEntity(entity.build());

        // as the fields were removed the entity mapping ignores the fields.
        System.out.println("> " + engine2.getMetadata("TEST"));
        entry = entry().set("COL1", 2).set("COL2", true).set("COL3", 2d).set("COL4", 1l).set("COL5", "c")
                .build();
        engine2.persist("TEST", entry);


        test = engine2.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));

        entity.addColumn("COL6", BLOB).addColumn("COL7", DOUBLE);
        engine2.updateEntity(entity.build());

        entry = entry().set("COL1", 3).set("COL2", true).set("USER", 2d).set("COL4", 1l).set("COL5", "c").set("COL6", new BlobTest(1, "")).set("COL7", 2d)
                .build();
        engine2.persist("TEST", entry);

        test = engine2.getMetadata("TEST");
        assertEquals(INT, test.get("COL1"));
        assertEquals(LONG, test.get("COL4"));
        assertEquals(STRING, test.get("COL5"));
        assertEquals(BLOB, test.get("COL6"));
        assertEquals(DOUBLE, test.get("COL7"));

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
    public void testPersistOverideAutoIncrement() throws Exception {
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

        final List<Map<String, ResultColumn>> query = engine.query("SELECT * FROM " + StringUtils.quotize("MYTEST", engine.escapeCharacter()));
        for (Map<String, ResultColumn> stringResultColumnMap : query) {
//            System.out.println(stringResultColumnMap);
            assertTrue(stringResultColumnMap.get("COL2").toString().endsWith(stringResultColumnMap.get("COL1").toString()));
        }
        engine.close();
    }

    @Test
    public void testPersistOverideAutoIncrement2() throws Exception {
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
    public void testPersistOverideAutoIncrement3() throws Exception {
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

        final List<Map<String, ResultColumn>> query = engine.query("SELECT * FROM " + StringUtils.quotize("MYTEST", engine.escapeCharacter()));
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
        final Map<String, DbColumnType> metaMap = new LinkedHashMap<String, DbColumnType>();
        metaMap.put("timestamp", INT);
        assertEquals("Metamap ok?", metaMap, engine.getMetadata(newName));

        // Check the data
        List<Map<String, ResultColumn>> resultSet = engine.query(select(all()).from(table(newName)));
        assertEquals("Count ok?", 1, resultSet.size());

        assertTrue("Content ok?", 20 == resultSet.get(0).get("timestamp").toInt());

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
                ((AbstractDatabaseEngine) engine).dropEntity(dbEntity().name(table).build());
                //engine.executeUpdate(String.format("DROP TABLE %s", table));
            } catch (Throwable e) {
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

        List<Map<String, ResultColumn>> query = engine.query(select(all()).from(table("TEST")).where(like(udf("lower", column("COL5")), k("%teste%"))));
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
    public void fkTestRemoveRowPreviouslyReferencedByForeignKey() throws DatabaseEngineException, DatabaseFactoryException, SQLException, RecoveryException, RetryLimitExceededException, InterruptedException {
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
    public void alterColumnWithConstraintTest() throws DatabaseEngineException, NameAlreadyExistsException, ConnectionResetException {
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

        engine.executeUpdate("INSERT INTO " + StringUtils.quotize("TEST", engine.escapeCharacter()) + " (" + StringUtils.quotize("COL1", engine.escapeCharacter()) + ") VALUES(10)");

        List<Map<String, ResultColumn>> test = engine.query(select(all()).from(table("TEST")));
        assertEquals("Check size of records", 1, test.size());
        Map<String, ResultColumn> record = test.get(0);
        assertEquals("Check COL1", 10, record.get("COL1").toInt().intValue());
        assertEquals("Check COL2", false, record.get("COL2").toBoolean().booleanValue());
        assertEquals("Check COL3", 2.2d, record.get("COL3").toDouble().doubleValue(), 0);
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
        assertEquals("Check COL2", false, record.get("COL2").toBoolean().booleanValue());
        assertEquals("Check COL3", 2.2d, record.get("COL3").toDouble().doubleValue(), 1e-9);
        assertEquals("Check COL4", 3L, record.get("COL4").toLong().longValue());
        assertEquals("Check COL5", "mantorras", record.get("COL5").toString());
        assertEquals("Check COL6", true, record.get("COL6").toBoolean().booleanValue());
        assertEquals("Check COL7", 7, record.get("COL7").toInt().intValue());
        connection2.close();
    }
}
