/*
 * Copyright 2018 Feedzai
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
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.engine.AbstractDatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import com.feedzai.commons.sql.abstraction.engine.NameAlreadyExistsException;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import mockit.Capturing;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Verifications;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.engine.EngineTestUtils.buildEntity;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;


/**
 * Tests closing a {@link DatabaseEngine} to make sure all resources are cleaned up correctly.
 *
 * It tests with each schema policy.
 *
 * @author David Fialho (david.fialho@feedzai.com)
 * @since 2.1.13
 */
@RunWith(Parameterized.class)
public class EngineCloseTest {

    protected DatabaseEngine engine;
    protected Properties properties;

    // Parameters
    protected DatabaseConfiguration config;
    protected String schemaPolicy;

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws Exception {

        final Collection<DatabaseConfiguration> configurations = DatabaseTestUtil.loadConfigurations();
        final Collection<String> schemaPolicies = Arrays.asList(
                "drop-create",
                "create-drop",
                "create",
                "none"
        );

        final ArrayList<Object[]> data = new ArrayList<>();
        for (final DatabaseConfiguration configuration : configurations) {
            for (final String schemaPolicy : schemaPolicies) {
                data.add(new Object[]{configuration, schemaPolicy});
            }
        }

        return data;
    }

    public EngineCloseTest(final DatabaseConfiguration config, final String schemaPolicy) {
        this.config = config;
        this.schemaPolicy = schemaPolicy;
    }

    @BeforeClass
    public static void initStatic() {
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.TRACE);
    }

    @Before
    public void setUp() throws DatabaseFactoryException {
        properties = new Properties() {{
            setProperty(JDBC, config.jdbc);
            setProperty(USERNAME, config.username);
            setProperty(PASSWORD, config.password);
            setProperty(ENGINE, config.engine);
            setProperty(SCHEMA_POLICY, schemaPolicy);
        }};

        engine = DatabaseFactory.getConnection(properties);
    }

    /**
     * Test that closing a database engine with multiple entities closes all insert statements associated with each
     * entity, regardless of the schema policy used.
     *
     * Each entity is associated with 3 prepared statements. This test ensures that 3 PSs per entity are closed.
     *
     * @param preparedStatementMock       The mock to check number of closed prepared statements.
     * @throws DatabaseEngineException    If something goes wrong while adding an entity to the engine.
     * @throws SQLException               If an error occurs while closing the DB engine.
     * @throws NameAlreadyExistsException If trying to create two statements with the same name.
     * @since 2.1.13
     */
    @Test
    public void closingAnEngineShouldFlushAndCloseInsertPSsAndCloseCachedPSs(@Capturing final Statement preparedStatementMock)
            throws DatabaseEngineException, SQLException, NameAlreadyExistsException {

        engine.addEntity(buildEntity("ENTITY-1"));
        engine.addEntity(buildEntity("ENTITY-2"));
        engine.createPreparedStatement("PS-1", "SELECT * FROM ENTITY-1");
        engine.createPreparedStatement("PS-2", "SELECT * FROM ENTITY-2");

        // Prevent the temporary statements, created while dropping entities, from affecting the
        // number of close() invocations
        new MockUp<AbstractDatabaseEngine>() {
            @Mock
            void dropEntity(final DbEntity entity) { }
        };

        // Force invocation counting to start here
        new Expectations() {{ }};

        engine.close();

        new Verifications() {{
            preparedStatementMock.close(); times = 2 * 3 + 2;   // {2 entities} x {PSs per entity} + {cached PSs}
            preparedStatementMock.executeBatch(); times = 2 * 3;   // {2 entities} x {PSs per entity}
        }};

    }

}
