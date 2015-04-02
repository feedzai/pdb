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
import com.feedzai.commons.sql.abstraction.engine.handler.OperationFault;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.hamcrest.Matcher;
import org.hamcrest.core.AnyOf;
import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

/**
 * Tests engine creation.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class EngineCreateTest {
    @Rule
    public ExpectedException expected = ExpectedException.none();

    protected Properties properties;

    @Parameters
    public static Collection<Object[]> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations();
    }

    @Parameter
    public DatabaseConfiguration config;


    @Before
    public void init() throws DatabaseEngineException {
        this.properties = new Properties() {
            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "create");
                setProperty(RETRY_INTERVAL, "1000");
            }
        };
    }

    @Test
    public void addEntityWithSchemaAlreadyCreated2Test() throws DatabaseEngineException, InterruptedException, DatabaseFactoryException {
        DatabaseEngine engine = DatabaseFactory.getConnection(properties);

        try {
            ((AbstractDatabaseEngine) engine).dropEntity(dbEntity().name("TEST2")
                    .build());
            ((AbstractDatabaseEngine) engine).dropEntity(dbEntity().name("TEST1")
                    .build());
        } catch (DatabaseEngineException e) {
        }

        DbEntity entity1 = dbEntity()
                .name("TEST1")
                .addColumn("COL1", INT)
                .pkFields("COL1")
                .build();

        engine.addEntity(entity1);

        DbEntity entity2 = dbEntity()
                .name("TEST2")
                .addColumn("COL1", INT)
                .addColumn("COL2", INT)
                .pkFields("COL1")
                .addFk(
                        dbFk()
                                .addColumn("COL2")
                                .foreignTable("TEST1")
                                .addForeignColumn("COL1")
                                .build()
                )
                .build();

        engine.addEntity(entity2);

        engine.close();

        engine = DatabaseFactory.getConnection(properties);

        engine.addEntity(entity1);
        engine.addEntity(entity2);

        engine.close();
    }

    @Test
    public void duplicationFailsTest() throws DatabaseFactoryException, DuplicateEngineException {
        expected.expect(DuplicateEngineException.class);
        expected.expectMessage("Duplicate can only be called if pdb.policy is set to 'create' or 'none'");
        DatabaseEngine engine = DatabaseFactory.getConnection(properties);

        Properties prop = new Properties() {
            {
                setProperty("pdb.schema_policy", "drop-create");
            }
        };

        engine.duplicate(prop, false);
    }

    @Test
    public void duplicationGoesOkTest() throws DatabaseFactoryException, DuplicateEngineException {
        DatabaseEngine engine = DatabaseFactory.getConnection(properties);


        engine.duplicate(properties, false);
    }

    @Test
    public void testWithFaultyTranslatorAndFailsTest() throws DatabaseFactoryException {
        expected.expect(DatabaseFactoryException.class);
        expected.expectMessage("Provided translator does extend from AbstractTranslator.");

        properties.setProperty(TRANSLATOR, "java.lang.Object");
        DatabaseFactory.getConnection(properties);
    }

    @Test
    public void testWithCustomTranslatorAndGoesOkTest() throws DatabaseFactoryException {
        properties.setProperty(TRANSLATOR, "com.feedzai.commons.sql.abstraction.engine.testconfig.CustomTranslator");
        DatabaseFactory.getConnection(properties);
    }

    @Test
    public void stopsWhenTableAlreadyExistsTest() throws Exception {
        expected.expect(DatabaseEngineException.class);
        expected.expectMessage("An error occurred adding the entity.");

        final DatabaseEngine conn = DatabaseFactory.getConnection(properties);
        DatabaseEngine conn2 = conn.duplicate(new Properties(), false);

        conn2.setExceptionHandler((op, e) -> {
            if (OperationFault.Type.TABLE_ALREADY_EXISTS.equals(op.getType())) {
                return false;
            }

            return true;
        });

        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT).build();

        conn.addEntity(entity);
        conn2.addEntity(entity);

    }

    /**
     * Tests the error thrown when an entity is loaded and it doesn't exist in the database.
     *
     * @throws Exception
     * @since 2.1.2
     */
    @Test
    public void testLoadEntityTableDoesNotExist() throws Exception {
        DatabaseEngine engine = DatabaseFactory.getConnection(properties);

        // make sure that entity doesn't exist
        silentTableDrop(engine, "TEST");

        try {

            DbEntity entity = dbEntity()
                    .name("TEST")
                    .addColumn("COL1", INT)
                    .pkFields("COL1")
                    .build();

            expected.expect(DatabaseEngineException.class);
            expected.expectMessage(AnyOf.anyOf(IsEqual.equalTo("Something went wrong persisting the entity"),
                    IsEqual.equalTo("Something went wrong handling statement")));
            engine.loadEntity(entity);

            // some of the databases will throw the error on loadEntity, the others only on persist
            engine.persist(entity.getName(), new EntityEntry.Builder().set("COL1", 1).build());
        } finally {
            engine.close();
        }
    }

    /**
     * Tests a normal usage of loadEntity on a database that already has the table defined.
     * <p>
     * Also validates that calling loadEntity multiple times is allowed.
     *
     * @throws Exception
     * @since 2.1.2
     */
    @Test
    public void testLoadEntity() throws Exception {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .pkFields("COL1")
                .build();

        DatabaseEngine engine = null;

        // make sure that entity doesn't exist and then create it from scratch
        try {
            engine = DatabaseFactory.getConnection(properties);

            silentTableDrop(engine, "TEST");
            engine.addEntity(entity);

        } finally {
            engine.close();
        }

        try {
            engine = DatabaseFactory.getConnection(properties);
            engine.loadEntity(entity);
            engine.persist(entity.getName(), new EntityEntry.Builder().set("COL1", 1).build());

            // make sure that calling loadEntity twice doesn't have any impact.
            engine.loadEntity(entity);
            engine.persist(entity.getName(), new EntityEntry.Builder().set("COL1", 2).build());

            List<Map<String, ResultColumn>> results = engine.query(select(all()).from(table(entity.getName())));

            assertEquals("Check that two lines are returned", 2, results.size());
            assertEquals("Check that first result is correct", 1, results.get(0).get("COL1").toInt().intValue());
            assertEquals("Check that second result is correct", 2, results.get(1).get("COL1").toInt().intValue());

        } finally {
            engine.close();
        }
    }

    /**
     * Tests that loadEntity method validates the entities.
     *
     * @throws Exception
     * @since 2.1.2
     */
    @Test
    public void testLoadEntityInvalidTable() throws Exception {
        DbEntity entity = dbEntity()
                .addColumn("COL1", INT)
                .pkFields("COL1")
                .build();

        DatabaseEngine engine = null;
        try {
            engine = DatabaseFactory.getConnection(properties);

            expected.expect(DatabaseEngineException.class);
            expected.expectMessage("You have to define the entity name");

            engine.loadEntity(entity);
        } finally {
            engine.close();
        }
    }

    /**
     * Tests that an entity that was loaded ny loadEntity is recovered with success on a database connection failure.
     *
     * @throws Exception
     * @since 2.1.2
     */
    @Test
    public void testLoadAndRecoverEntity() throws Exception {
        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT)
                .pkFields("COL1")
                .build();

        DatabaseEngine engine = null;

        // make sure that entity doesn't exist and then create it from scratch
        try {
            engine = DatabaseFactory.getConnection(properties);

            silentTableDrop(engine, "TEST");
            engine.addEntity(entity);

        } finally {
            engine.close();
        }

        try {
            engine = DatabaseFactory.getConnection(properties);

            engine.loadEntity(entity);

            // save the current connection to check if is not the same being used after the failure
            Connection oldConnection = engine.getConnection();

            // before persist force the connection to be closed in order to force a recover
            try {
                engine.getConnection().close();
            } catch (Exception e) {
            }

            engine.persist(entity.getName(), new EntityEntry.Builder().set("COL1", 1).build());

            assertFalse("Check that old and new connections are not the same", engine.getConnection().equals(oldConnection));

            // make sure that calling loadEntity twice doesn't have any impact.
            engine.loadEntity(entity);
            engine.loadEntity(entity);
            engine.persist(entity.getName(), new EntityEntry.Builder().set("COL1", 2).build());

            List<Map<String, ResultColumn>> results = engine.query(select(all()).from(table(entity.getName())));

            assertEquals("Check that two lines are returned", 2, results.size());
            assertEquals("Check that first result is correct", 1, results.get(0).get("COL1").toInt().intValue());
            assertEquals("Check that second result is correct", 2, results.get(1).get("COL1").toInt().intValue());

        } finally {
            engine.close();
        }
    }

    /**
     * Silently drops a table using a provided connection.
     *
     * @param engine    The database connection.
     * @param tableName The name of the table to drop.
     * @since 2.1.2
     */
    private void silentTableDrop(DatabaseEngine engine, String tableName) {
        try {
            ((AbstractDatabaseEngine) engine).dropEntity(new DbEntity.Builder().name(tableName).build());
        } catch (Exception ignored) {
        }
    }
}
