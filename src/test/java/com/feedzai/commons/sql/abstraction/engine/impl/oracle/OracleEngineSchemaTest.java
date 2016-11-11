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
package com.feedzai.commons.sql.abstraction.engine.impl.oracle;


import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineSchemaTest;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Rafael Marmelo (rafael.marmelo@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class OracleEngineSchemaTest extends AbstractEngineSchemaTest {


    @Parameterized.Parameters
    public static Collection<Object[]> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("oracle");
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

    @Override
    public void init() throws Exception {
        properties = new Properties() {
            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "drop-create");
                setProperty(SCHEMA, getDefaultSchema());
            }
        };
    }

    @Override
    protected String getDefaultSchema() {
        return "";
    }

    @Override
    protected String getSchema() {
        return "";
    }

    /**
     * Checks that system generated columns with name starting with SYS_ in the
     * Oracle engine do not appear in the table metadata.
     *
     * @throws Exception propagates any Exception thrown by the test
     */
    @Test
    public void testSystemGeneratedColumns() throws Exception {
        DatabaseEngine engine = DatabaseFactory.getConnection(properties);

        try {
            DbEntity entity = dbEntity()
                    .name("TEST_SYS_COL")
                    // Simulates a system generated column
                    .addColumn("SYS_COL1", INT)
                    .addColumn("COL1", INT)
                    .pkFields("COL1")
                    .build();
            engine.addEntity(entity);

            assertFalse("The simulated system generated column should not appear in the table metadata", engine.getMetadata("TEST_SYS_COL").containsKey("SYS_COL1"));
            assertTrue("The regular column should appear in the table metadata", engine.getMetadata("TEST_SYS_COL").containsKey("COL1"));
        } finally {
            engine.close();
        }
    }
}
