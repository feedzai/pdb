/*
 * Copyright 2021 Feedzai
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
package com.feedzai.commons.sql.abstraction.engine.impl.pool;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.dml.Query;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.pool.DatabaseEnginePool;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.STRING;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.all;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.column;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.eq;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.k;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.select;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.table;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.RETRY_INTERVAL;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

/**
 * Tests the {@link DatabaseEnginePool} class.
 *
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.8.3
 */
@RunWith(Parameterized.class)
public final class DatabaseEnginePoolTest {

    private static final String TEST_TABLE_NAME = "connection_pool_test";
    private static final DbEntity TEST_TABLE =
            dbEntity().name(TEST_TABLE_NAME)
                      .addColumn("COL1", INT)
                      .addColumn("COL2", STRING)
                      .pkFields("COL1")
                      .build();

    private DatabaseEnginePool pool;
    private Properties properties;

    @Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
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

        this.pool = DatabaseEnginePool.getConnectionPool(properties, db -> {
            // load the entity, so we can persist rows during the tests.
            try {
                db.loadEntity(TEST_TABLE);
            } catch (DatabaseEngineException e) {
                // suppress for testing purposes.
            }
        });

        // add a row to the test table.
        final EntityEntry row =
                new EntityEntry.Builder()
                        .set("COL1", 1)
                        .set("COL2", "lol")
                        .build();

        try (DatabaseEngine db = pool.borrow()) {
            // remove it just to prevent an error, since as post action we are loading it.
            db.removeEntity(TEST_TABLE_NAME);
            // actual table creation.
            db.addEntity(TEST_TABLE);
            db.persist(TEST_TABLE_NAME, row);
        }
    }

    @After
    public void close() throws DatabaseEngineException {
        if (pool != null) {
            // drop what was created.
            try (DatabaseEngine db = pool.borrow()) {
                db.dropEntity(TEST_TABLE_NAME);
            }

            pool.close();
        }
    }

    /**
     * Check that we query data.
     */
    @Test
    public void testQuery() throws DatabaseEngineException {
        try (DatabaseEngine db = pool.borrow()) {
            final List<Map<String, ResultColumn>> rows =
                    db.query(select(all()).from(table(TEST_TABLE_NAME)));

            final Map<String, ResultColumn> row = rows.get(0);
            assertEquals(1, row.get("COL1").toInt().intValue());
            assertEquals("lol", row.get("COL2").toString());
        }
    }

    /**
     * Check that we can persist rows, since the entity needs to be mapped in PDB.
     */
    @Test
    public void testPersist() throws DatabaseEngineException {
        try (DatabaseEngine db = pool.borrow()) {
            final EntityEntry build =
                    new EntityEntry.Builder()
                            .set("COL1", 2)
                            .set("COL2", "rofl")
                            .build();

            db.persist(TEST_TABLE_NAME, build);

            // check if what we persisted is there.
            final Query query =
                    select(all())
                            .from(table(TEST_TABLE_NAME))
                            .where(eq(column("COL1"), k(2)));

            final List<Map<String, ResultColumn>> rows = db.query(query);
            final Map<String, ResultColumn> row = rows.get(0);

            assertEquals(2, row.get("COL1").toInt().intValue());
            assertEquals("rofl", row.get("COL2").toString());
        }
    }

    /**
     * Check that we can't persist rows without loading the entity as post action.
     */
    @Test(expected = DatabaseEngineException.class)
    public void testPersistWithoutLoading() throws DatabaseEngineException {
        try (DatabaseEnginePool pool = DatabaseEnginePool.getConnectionPool(properties)) {
            try (DatabaseEngine db = pool.borrow()) {
                final EntityEntry build =
                        new EntityEntry.Builder()
                                .set("COL1", 2)
                                .set("COL2", "rofl")
                                .build();

                db.persist(TEST_TABLE_NAME, build);
            }
        }
    }

    /**
     * Check that pools contains different connections.
     */
    @Test
    public void testDifferentConnections() {
        try (DatabaseEngine one = pool.borrow(); DatabaseEngine two = pool.borrow()) {
            assertNotEquals("DatabaseEngine connections must be different.", one, two);
            assertTrue("DatabaseEngine connection must be alive.", one.checkConnection());
            assertTrue("DatabaseEngine connection must be alive.", two.checkConnection());
        }
    }

    /**
     * Check that we support {@link PdbProperties}.
     */
    @Test
    public void testPdbProperties() throws DatabaseEngineException {
        final PdbProperties pdbProperties = new PdbProperties(properties, true);

        final DatabaseEnginePool connectionPool = DatabaseEnginePool.getConnectionPool(pdbProperties, db -> {
            try {
                db.loadEntity(TEST_TABLE);
            } catch (DatabaseEngineException e) {
                // suppress for testing purposes.
            }
        });

        try (DatabaseEngine db = connectionPool.borrow()) {
            final List<Map<String, ResultColumn>> rows =
                    db.query(select(all()).from(table(TEST_TABLE_NAME)));

            final Map<String, ResultColumn> row = rows.get(0);
            assertEquals(1, row.get("COL1").toInt().intValue());
            assertEquals("lol", row.get("COL2").toString());
        } finally {
            if (!connectionPool.isClosed()) {
                connectionPool.close();
            }
        }
    }
}
