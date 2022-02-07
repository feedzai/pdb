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
import com.feedzai.commons.sql.abstraction.dml.Query;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.engine.*;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.BOOLEAN;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.DOUBLE;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.LONG;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.STRING;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.all;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.column;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbFk;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.entry;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.eq;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.select;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.table;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static org.junit.Assert.assertEquals;

/**
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class SqlServerEngineGeneralTest {

    private static final String USER_TABLE = "USER";
    private static final String ID_COLUMN = "ID";
    private static final String NAME_COLUMN = "NAME";
    private static final String AGE_COLUMN = "AGE";

    protected DatabaseEngine engine;
    protected Properties properties;

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("sqlserver");
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

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
            .name(USER_TABLE)
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
                    .referencedTable(USER_TABLE)
                    .addReferencedColumn("COL1")
                    .build(),
                dbFk()
                    .addColumn("COL2")
                    .referencedTable("ROLE")
                    .addReferencedColumn("COL1")
                    .build()
            )
            .pkFields("COL1", "COL2").build();

        engine.addEntity(entity);

        engine.query(
            select(all()).from(
                table(USER_TABLE).alias("a").withNoLock()
                    .innerJoin(table("USER_ROLE").alias("b").withNoLock(), eq(column("a", "COL1"), column("b", "COL1")))
            )
        );

        engine.query(
            select(all()).from(
                table(USER_TABLE).alias("a").withNoLock()
                    .innerJoin(table("USER_ROLE").alias("b").withNoLock(), eq(column("a", "COL1"), column("b", "COL1")))
                    .innerJoin(table("ROLE").alias("c"), eq(column("b", "COL2"), column("c", "COL1")))
            )
        );

        engine.query(
            select(all()).from(
                table(USER_TABLE).alias("a").withNoLock()
                    .rightOuterJoin(table("USER_ROLE").alias("b").withNoLock(), eq(column("a", "COL1"), column("b", "COL1")))
            )
        );

        engine.query(
            select(all()).from(
                table(USER_TABLE).alias("a").withNoLock()
                    .leftOuterJoin(table("USER_ROLE").alias("b").withNoLock(), eq(column("a", "COL1"), column("b", "COL1")))
            )
        );
    }

    @Test
    public void selectWithOrderByWithMultipartIdentifier() throws DatabaseEngineException {
        DbEntity entity = dbEntity()
                .name(USER_TABLE)
                .addColumn(ID_COLUMN, INT, true)
                .addColumn(NAME_COLUMN, STRING)
                .addColumn(AGE_COLUMN, INT)
                .pkFields(ID_COLUMN).build();

        engine.addEntity(entity);

        final EntityEntry person1Entity = entry()
                .set(NAME_COLUMN,"Person 1")
                .set(AGE_COLUMN, 10)
                .build();
        engine.persist(USER_TABLE, person1Entity);

        final EntityEntry person2Entity = entry()
                .set(NAME_COLUMN,"Person 2")
                .set(AGE_COLUMN, 46)
                .build();
        engine.persist(USER_TABLE, person2Entity);

        final EntityEntry person3Entity = entry()
                .set(NAME_COLUMN,"Person 3")
                .set(AGE_COLUMN, 23)
                .build();
        engine.persist(USER_TABLE, person3Entity);

        // query with order by clause
        final Query query = select(all())
                .from(table(USER_TABLE))
                .orderby(ImmutableList.of(column(USER_TABLE, AGE_COLUMN).asc()))
                .limit(10)
                .offset(0);

        final List<Map<String, Object>> listProcessed = extractPersonList(engine.query(query));
        assertEquals(3, listProcessed.size());

        assertPerson(1, "Person 1", 10, listProcessed.get(0));
        assertPerson(3, "Person 3", 23, listProcessed.get(1));
        assertPerson(2, "Person 2", 46, listProcessed.get(2));

        // try with a different page
        final Query query2 = select(all())
                .from(table(USER_TABLE))
                .orderby(ImmutableList.of(column(USER_TABLE, AGE_COLUMN).asc()))
                .limit(1)
                .offset(1);

        final List<Map<String, Object>> listProcessed2 = extractPersonList(engine.query(query2));
        assertEquals(1, listProcessed2.size());
        assertPerson(3, "Person 3", 23, listProcessed2.get(0));
    }

    private List<Map<String, Object>> extractPersonList(final List<Map<String, ResultColumn>> results) {
        final List<Map<String, Object>> listProcessed = new ArrayList<>();
        for (final Map<String, ResultColumn> entry : results) {
            listProcessed.add(ImmutableMap.of(
                    ID_COLUMN, entry.get(ID_COLUMN).toInt(),
                    NAME_COLUMN, entry.get(NAME_COLUMN).toString(),
                    AGE_COLUMN, entry.get(AGE_COLUMN).toInt()
            ));
        }
        return listProcessed;
    }

    private void assertPerson(final int id, final String name, final int age, final Map<String, Object> person) {
        assertEquals(id, person.get(ID_COLUMN));
        assertEquals(name, person.get(NAME_COLUMN));
        assertEquals(age, person.get(AGE_COLUMN));
    }
}
