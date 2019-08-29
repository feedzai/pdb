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
package com.feedzai.commons.sql.abstraction.engine.impl.db2;


import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.LOGIN_TIMEOUT;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SOCKET_TIMEOUT;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static org.junit.Assert.assertEquals;

/**
 * @author Joao Silva (joao.silva@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class DB2EngineSchemaTest extends AbstractEngineSchemaTest {

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("db2");
    }

    @Override
    protected void defineUDFGetOne(final DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate(
            "CREATE OR REPLACE FUNCTION GETONE () " +
            "    RETURNS INTEGER " +
            "    NO EXTERNAL ACTION " +
            "F1: BEGIN ATOMIC " +
                "RETURN 1; " +
            "END"
        );
    }

    @Override
    protected void defineUDFTimesTwo(final DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate(
            "CREATE OR REPLACE FUNCTION \"" + getTestSchema() + "\".TimesTwo (VARNAME VARCHAR(128)) " +
            "    RETURNS INTEGER " +
            "    NO EXTERNAL ACTION " +
            "F1: BEGIN ATOMIC " +
            "    RETURN VARNAME * 2; " +
            "END"
        );
    }

    @Override
    protected void createSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException {
        engine.executeUpdate("CREATE SCHEMA \"" + schema + "\"");
    }

    @Override
    protected void dropSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException {
        final List<Map<String, ResultColumn>> tableResults = engine.query(
            "SELECT tabname FROM syscat.tables WHERE tabschema='" + schema + "'"
        );

        for (final Map<String, ResultColumn> table : tableResults) {
            engine.executeUpdate("DROP TABLE \"" + schema + "\".\"" + table.get("TABNAME") + "\"");
        }

        final List<Map<String, ResultColumn>> funcResults = engine.query(
            "SELECT funcname FROM syscat.functions WHERE funcschema='" + schema + "'"
        );

        for (final Map<String, ResultColumn> result : funcResults) {
            engine.executeUpdate("DROP FUNCTION \"" + schema + "\".\"" + result.get("FUNCNAME") + "\"");
        }

        engine.executeUpdate(
                "BEGIN\n" +
                "   DECLARE CONTINUE HANDLER FOR SQLSTATE '42704' BEGIN END;\n" +
                "   EXECUTE IMMEDIATE 'DROP SCHEMA \"" + schema + "\" RESTRICT';\n" +
                "END"
        );
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
    public void testTimeout() throws DatabaseFactoryException, InterruptedException, RecoveryException, RetryLimitExceededException, SQLException {
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
