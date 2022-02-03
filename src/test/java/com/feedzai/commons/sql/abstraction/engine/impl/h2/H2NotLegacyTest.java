/*
 * Copyright 2022 Feedzai
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
package com.feedzai.commons.sql.abstraction.engine.impl.h2;

import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.EngineGeneralTest;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.STRING;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.entry;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static org.junit.Assert.assertEquals;

/**
 * Tests H2 behavior in not legacy mode.
 *
 * @author Carlos Tosin (carlos.tosin@feedzai.com)
 * @since ```feedzai.next.release```
 */
@RunWith(Parameterized.class)
public class H2NotLegacyTest extends EngineGeneralTest {

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("h2", "h2remote");
    }

    @Test
    @Override
    public void testPersistOverrideAutoIncrement() {
        // The implementation from superclass does not work on not legacy H2 mode.
    }

    @Test
    @Override
    public void testPersistOverrideAutoIncrement2() {
        // The implementation from the superclass does not work on not legacy H2 mode.
    }

    @Test
    @Override
    public void insertWithNoAutoIncAndThatResumeTheAutoIncTest() {
        // The implementation from the superclass does not work on non-legacy H2 mode.
    }

    @Test
    @Override
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


        ent = entry().set("COL2", "CENAS3").set("COL1", 3)
                .build();
        engine.persist("MYTEST", ent, false);

        ent = entry().set("COL2", "CENAS4")
                .build();
        engine.persist("MYTEST", ent);

        final List<Map<String, ResultColumn>> query = engine.query("SELECT * FROM " + quotize("MYTEST", engine.escapeCharacter()));
        assertEquals(4, query.size());

        assertEquals(1L, query.get(0).get("COL1").toLong().longValue());
        assertEquals("CENAS1", query.get(0).get("COL2").toString());

        assertEquals(1L, query.get(1).get("COL1").toLong().longValue());
        assertEquals("CENAS2", query.get(1).get("COL2").toString());

        assertEquals(3L, query.get(2).get("COL1").toLong().longValue());
        assertEquals("CENAS3", query.get(2).get("COL2").toString());

        assertEquals(2L, query.get(3).get("COL1").toLong().longValue());
        assertEquals("CENAS4", query.get(3).get("COL2").toString());

        engine.close();
    }
}
