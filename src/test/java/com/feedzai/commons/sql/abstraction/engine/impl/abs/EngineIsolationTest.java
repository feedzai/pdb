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

import com.feedzai.commons.sql.abstraction.engine.*;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.Test;

import java.util.Collection;
import java.util.Properties;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;

/**
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class EngineIsolationTest {
    protected Properties properties;

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations();
    }

    @Parameterized.Parameter
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
    public void readCommittedTest() throws DatabaseEngineException, InterruptedException, DatabaseFactoryException {
        properties.setProperty(ISOLATION_LEVEL, "read_committed");

        DatabaseEngine engine = DatabaseFactory.getConnection(properties);
    }

    @Test
    public void readUncommittedTest() throws DatabaseEngineException, InterruptedException, DatabaseFactoryException {
        this.properties.setProperty(ISOLATION_LEVEL, "read_uncommitted");

        DatabaseEngine engine = DatabaseFactory.getConnection(properties);
    }

    @Test
    public void repeatableReadTest() throws DatabaseEngineException, InterruptedException, DatabaseFactoryException {
        this.properties.setProperty(ISOLATION_LEVEL, "read_uncommitted");

        DatabaseEngine engine = DatabaseFactory.getConnection(properties);
    }

    @Test
    public void serializableTest() throws DatabaseEngineException, InterruptedException, DatabaseFactoryException {
        this.properties.setProperty(ISOLATION_LEVEL, "read_uncommitted");

        DatabaseEngine engine = DatabaseFactory.getConnection(properties);
    }
}
