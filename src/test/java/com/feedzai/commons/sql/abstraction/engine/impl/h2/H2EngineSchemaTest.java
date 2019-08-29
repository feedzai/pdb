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
package com.feedzai.commons.sql.abstraction.engine.impl.h2;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import com.feedzai.commons.sql.abstraction.engine.RecoveryException;
import com.feedzai.commons.sql.abstraction.engine.RetryLimitExceededException;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineSchemaTest;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.LOGIN_TIMEOUT;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SOCKET_TIMEOUT;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineSchemaTest.Ieee754Support.SUPPORTED_STRINGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/**
 * @author Joao Silva (joao.silva@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class H2EngineSchemaTest extends AbstractEngineSchemaTest {

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("h2");
    }

    @Override
    protected Ieee754Support getIeee754Support() {
        return SUPPORTED_STRINGS;
    }

    /**
     * This test overrides the superclass in order to check if the H2 engine is local or remote; if it is remote
     * the test is skipped.
     *
     * This was done because the UDF is defined by static methods in this class, which needs to be available (compiled)
     * to the H2 engine. This is already assumed when H2 is embedded, but making the class available in remote H2
     * would require copying this to the location of the remote server.
     * Since this is already being tested with H2 embedded, we just skip the test when the server is remote.
     *
     * @throws Exception If something goes wrong with the test.
     * @see AbstractEngineSchemaTest#udfGetOneTest()
     */
    @Override
    public void udfGetOneTest() throws Exception {
        assumeTrue("Test not supported when using H2 remote - skipped", checkIsLocalH2());
        super.udfGetOneTest();
    }

    /**
     * This test overrides the superclass in order to check if the H2 engine is local or remote; if it is remote
     * the test is skipped.
     *
     * This was done because the UDF is defined by static methods in this class, which needs to be available (compiled)
     * to the H2 engine. This is already assumed when H2 is embedded, but making the class available in remote H2
     * would require copying this to the location of the remote server.
     * Since this is already being tested with H2 embedded, we just skip the test when the server is remote.
     *
     * @throws Exception If something goes wrong with the test.
     * @see AbstractEngineSchemaTest#udfTimesTwoTest()
     */
    @Override
    public void udfTimesTwoTest() throws Exception {
        assumeTrue("Test not supported when using H2 remote - skipped", checkIsLocalH2());
        super.udfTimesTwoTest();
    }

    @Override
    protected void defineUDFGetOne(final DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate(
            "CREATE ALIAS IF NOT EXISTS GetOne FOR \"com.feedzai.commons.sql.abstraction.engine.impl.h2.H2EngineSchemaTest.GetOne\";"
        );
    }

    @Override
    protected void defineUDFTimesTwo(final DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate(
            "CREATE ALIAS IF NOT EXISTS \"" + getTestSchema() + "\".TimesTwo FOR \"com.feedzai.commons.sql.abstraction.engine.impl.h2.H2EngineSchemaTest.TimesTwo\";"
        );
    }

    @Override
    protected void createSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException {
        engine.executeUpdate("CREATE SCHEMA IF NOT EXISTS \"" + schema + "\"");
    }

    @Override
    protected void dropSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException {
        engine.executeUpdate("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
    }

    /**
     * Checks whether the current connection to H2 is local or to a remote server.
     *
     * This method won't throw exceptions, if there is any problem the connection will be considered local.
     *
     * @return {@code true} if the connection is local, {@code false} otherwise.
     */
    private boolean checkIsLocalH2() {
        try (final DatabaseEngine engine = DatabaseFactory.getConnection(properties)) {
            return "0".equals(engine.getConnection().getClientInfo("numServers"));
        } catch (final Exception ex) {
            return true;
        }
    }

    public static int GetOne() {
        return 1;
    }

    public static int TimesTwo(int value) {
        return value * 2;
    }

    /**
     * Tests if the timeout setting is properly set in the connection socket to the database.
     * Note: the DB2 and H2 databases don't support setting the connection socket timeout
     *
     * @throws DatabaseFactoryException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws RecoveryException
     * @throws RetryLimitExceededException
     * @throws SQLException
     */
    @Test
    public void testTimeout() throws DatabaseFactoryException, ClassNotFoundException, InterruptedException, RecoveryException, RetryLimitExceededException, SQLException {
        final int socketTimeout = 60;// in seconds
        final int loginTimeout = 30;// in seconds
        final Properties properties = new Properties() {
            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "create");
                setProperty(SOCKET_TIMEOUT, Integer.toString(socketTimeout));
                setProperty(LOGIN_TIMEOUT, Integer.toString(loginTimeout));
            }
        };

        final DatabaseEngine de = DatabaseFactory.getConnection(properties);
        final int connectionTimeoutInMs = de.getConnection().getNetworkTimeout();
        // Not supported
        assertEquals("Is the timeout of the DB connection disabled as expected?", 0, connectionTimeoutInMs);
    }
}
