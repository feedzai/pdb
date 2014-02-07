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
package com.feedzai.commons.sql.abstraction.engine.impl.sqlserver;

import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.*;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;

/**
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class SqlServerEngineGeneralTest {

    protected DatabaseEngine engine;
    protected Properties properties;

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("sqlserver");
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

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

    @Test
    public void selectFromWithNoLockTest() throws DatabaseEngineException {
        DbEntity entity =
                dbEntity()
                        .name("TEST")
                        .addColumn("COL1", INT)
                        .addColumn("COL2", BOOLEAN)
                        .addColumn("COL3", DOUBLE)
                        .addColumn("COL4", LONG)
                        .addColumn("COL5", STRING).build();

        engine.addEntity(entity);

        engine.query(
                select(all()).from(table("TEST").withNoLock()));
    }

    @Test
    public void selectFromWithNoLockQWithAliasTest() throws DatabaseEngineException {
        DbEntity entity =
                dbEntity()
                        .name("TEST")
                        .addColumn("COL1", INT)
                        .addColumn("COL2", BOOLEAN)
                        .addColumn("COL3", DOUBLE)
                        .addColumn("COL4", LONG)
                        .addColumn("COL5", STRING).build();

        engine.addEntity(entity);

        engine.query(
                select(all()).from(table("TEST").alias("ALIAS").withNoLock()));
    }

    @Test
    public void joinsWithNoLocksTest() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name("USER")
                .addColumn("COL1", INT, true)
                .pkFields("COL1").build();

        engine.addEntity(entity);

        entity = dbEntity()
                .name("ROLE")
                .addColumn("COL1", INT, true)
                .pkFields("COL1").build();

        engine.addEntity(entity);

        entity = dbEntity()
                .name("USER_ROLE")
                .addColumn("COL1", INT)
                .addColumn("COL2", INT)
                .addFk(
                        dbFk()
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
                .pkFields("COL1", "COL2").build();

        engine.addEntity(entity);

        engine.query(
                select(all()).from(
                        table("USER").alias("a").withNoLock().innerJoin(table("USER_ROLE").alias("b").withNoLock(), eq(column("a", "COL1"), column("b", "COL1")))));

        engine.query(
                select(all()).from(
                        table("USER").alias("a").withNoLock().innerJoin(table("USER_ROLE").alias("b").withNoLock(), eq(column("a", "COL1"), column("b", "COL1"))).innerJoin(table("ROLE").alias("c"), eq(column("b", "COL2"), column("c", "COL1")))));

        engine.query(
                select(all()).from(
                        table("USER").alias("a").withNoLock().rightOuterJoin(table("USER_ROLE").alias("b").withNoLock(), eq(column("a", "COL1"), column("b", "COL1")))));

        engine.query(
                select(all()).from(
                        table("USER").alias("a").withNoLock().leftOuterJoin(table("USER_ROLE").alias("b").withNoLock(), eq(column("a", "COL1"), column("b", "COL1")))));

    }
}
