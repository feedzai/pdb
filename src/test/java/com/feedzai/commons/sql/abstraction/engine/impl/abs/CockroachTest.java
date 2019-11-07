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

package com.feedzai.commons.sql.abstraction.engine.impl.abs;

import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.FETCH_SIZE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static org.junit.Assert.fail;

/**
 * FIXME
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 * @since @@@feedzai.next.release@@@
 */
@RunWith(Parameterized.class)
public class CockroachTest {

    private static final Logger logger = LoggerFactory.getLogger(CockroachTest.class);

    protected Properties properties;

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations();
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

    @Before
    public void init() {
        this.properties = new Properties() {
            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "drop-create");
                setProperty(FETCH_SIZE, "2");
            }
        };
    }

    @Test
    public void limitsTest() throws Exception {
        // get a plain JDBC Connection for this test; this is the same as
        // java.sql.DriverManager.getConnection(config.jdbc, config.username, config.password);
        final Connection conn = DatabaseFactory.getConnection(properties).getConnection();

        conn.setAutoCommit(true);

        try (final Statement stmt = conn.createStatement()) {
            try {
                stmt.executeUpdate("DROP TABLE test");
            } catch (Exception e) {

            }
            stmt.executeUpdate("CREATE TABLE test (col1 VARCHAR(10), col2 VARCHAR(10))");
        }

        try (final PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO test VALUES(?, ?)")) {
            for (int i = 1; i <= 10; i++) {
                preparedStatement.setString(1, "c1val" + i);
                preparedStatement.setString(2, "c2val" + i);
                preparedStatement.executeUpdate();
            }
        }

        // without transactions
        try (final Statement stmt = conn.createStatement()) {
            stmt.setFetchSize(0);
            ResultSet resultAll = stmt.executeQuery("SELECT * FROM test");

            // the next case passes because CockroachDB has the same limitation as PostgreSQL:
            // if this is done outside a transaction, the DB doesn't keep cursors, hence the query ignores the
            // fetchSize hint and the client simply gets all results at once
            stmt.setFetchSize(2);
            ResultSet result1 = stmt.executeQuery("SELECT * FROM test");
        }

        // with transactions
        try (final Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            stmt.setFetchSize(0);
            ResultSet resultAll = stmt.executeQuery("SELECT * FROM test");
            conn.commit();
        }

        try (final Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            stmt.setFetchSize(2);
            ResultSet result1 = null;
            try {
                result1 = stmt.executeQuery("SELECT * FROM test");
            } catch (Exception ex) {
                logger.error("CockroachDB <19.2 does not support fetch size < resultset size inside transaction", ex);
                fail("test failed running query inside transaction");
            }

            // if the resultset is closed, this test will pass for CockroachDB 19.2
//            result1.close();
            try {
                conn.commit();
            } catch (Exception ex) {
                logger.error("CockroachDB 19.2 does not support commit transaction when there is a resultset that has not been closed and resultset size > fetch size", ex);
                fail("test failed committing transaction");
            }
        }
    }
}
