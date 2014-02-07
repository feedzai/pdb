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
import com.feedzai.commons.sql.abstraction.engine.*;
import com.feedzai.commons.sql.abstraction.engine.handler.ExceptionHandler;
import com.feedzai.commons.sql.abstraction.engine.handler.OperationFault;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbFk;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;
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

        conn2.setExceptionHandler(new ExceptionHandler() {
            @Override
            public boolean proceed(OperationFault op, Exception e) {
                if (OperationFault.Type.TABLE_ALREADY_EXISTS.equals(op.getType())) {
                    return false;
                }

                return true;
            }
        });

        DbEntity entity = dbEntity()
                .name("TEST")
                .addColumn("COL1", INT).build();

        conn.addEntity(entity);
        conn2.addEntity(entity);

    }
}
