/*
 * Copyright 2019 Feedzai
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

import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Objects;
import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ISOLATION_LEVEL;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;

/**
 * Additional isolation tests for SQL Server.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 * @since 2.4.7
 */
@RunWith(Parameterized.class)
public class SqlServerEngineIsolationTest {

    private Properties properties;

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("sqlserver");
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

    @Before
    public void init() throws Exception {
        final DatabaseConfiguration config = DatabaseTestUtil.loadConfigurations("sqlserver").iterator().next();

        this.properties = new Properties() {
            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "create");
                setProperty(ISOLATION_LEVEL, Objects.toString(SQLServerConnection.TRANSACTION_SNAPSHOT));
            }
        };
    }

    /**
     * Tests support for the custom isolation level "transaction snapshot" available in SQL Server (which has the same
     * behavior as "serializable" in other DB engines like PostgreSQL and Oracle).
     *
     * @throws DatabaseFactoryException if something goes wrong in the test.
     */
    @Test
    public void snapshotTest() throws DatabaseFactoryException {
        this.properties.setProperty(ISOLATION_LEVEL, Objects.toString(SQLServerConnection.TRANSACTION_SNAPSHOT));
        DatabaseFactory.getConnection(properties);
    }

}
