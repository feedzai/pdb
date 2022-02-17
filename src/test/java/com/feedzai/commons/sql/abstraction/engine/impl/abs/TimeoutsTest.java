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

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineDriver;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.CHECK_CONNECTION_TIMEOUT;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.LOGIN_TIMEOUT;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SOCKET_TIMEOUT;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Test the correct behavior of the PDB properties for timeouts.
 *
 * @author José Fidalgo
 */
@RunWith(Parameterized.class)
public class TimeoutsTest {

    /**
     * An executor to run actions asynchronously.
     */
    private final ExecutorService executor = Executors.newCachedThreadPool();

    /**
     * The database properties to use in the tests.
     */
    private Properties dbProps;

    /**
     * The test value for the login timeout.
     */
    private static final int LOGIN_TIMEOUT_SECONDS = 2;

    /**
     * The test value for the socket timeout (this must be greater than the sum of {@link #LOGIN_TIMEOUT_SECONDS}
     * and {@link #TIMEOUT_TOLERANCE_SECONDS}).
     */
    private static final int SOCKET_TIMEOUT_SECONDS = 20;

    /**
     * The tolerance to use when checking timeouts.
     *
     * This is needed because the drivers may take a few milliseconds after the set timeout value to effectively return;
     * some other databases may take double the amount or more of time configured, because they consider login timeout
     * separately from connection timeout when first establishing a connection to the DB.
     */
    private static final int TIMEOUT_TOLERANCE_SECONDS = 8;

    /**
     * The test value for the socket timeout (this must be greater than the sum of {@link #LOGIN_TIMEOUT_SECONDS}
     * and {@link #TIMEOUT_TOLERANCE_SECONDS}).
     */
    private static final int CHECK_CONNECTION_TIMEOUT_SECONDS = 15;

    /**
     * A test "router" that simply forwards the data sent between a {@link DatabaseEngine} in this test and the DB.
     * The connections can be interrupted without being closed, to test the correct functioning of the network timeouts.
     */
    private TestRouter testRouter;

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations();
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

    @Before
    public void setupTest() throws IOException {
        assertThat(SOCKET_TIMEOUT_SECONDS)
                .as("This test should be configured with a socket timeout greater than login timeout (plus timeout tolerance)")
                .isGreaterThan(LOGIN_TIMEOUT_SECONDS + TIMEOUT_TOLERANCE_SECONDS);

        dbProps = new Properties() {
            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "create-drop");
                put(LOGIN_TIMEOUT, LOGIN_TIMEOUT_SECONDS);
                put(SOCKET_TIMEOUT, SOCKET_TIMEOUT_SECONDS);
                put(CHECK_CONNECTION_TIMEOUT, CHECK_CONNECTION_TIMEOUT_SECONDS);
            }
        };

        final DatabaseEngineDriver engine = DatabaseEngineDriver.fromEngine(dbProps.getProperty(ENGINE));

        // For H2 there is a socket timeout property but it must be defined as a system property, before the driver
        // is loaded: see org.h2.engine.SysProperties
        assumeTrue(
            "H2 engine doesn't support setting timeouts, tests will be skipped",
            engine != DatabaseEngineDriver.H2 && engine != DatabaseEngineDriver.H2V2
        );

        testRouter = new TestRouter(engine.defaultPort());
    }

    @After
    public void cleanResources() {
        if (testRouter != null) {
            testRouter.close();
        }
        executor.shutdownNow();
    }

    /**
     * Tests if the timeout settings are properly set for the connection to the database.
     *
     * @throws Exception if something goes wrong (test fails).
     */
    @Test
    public void testTimeoutsConfigured() throws Exception {
        final DatabaseEngine de = DatabaseFactory.getConnection(this.dbProps);
        int loginTimeoutInSeconds = DriverManager.getLoginTimeout();
        int checkConnectionTimeoutInSec = de.getProperties().getCheckConnectionTimeout();

        // Some engines (e.g. mysql) set the network timeout asynchronously.
        await("await network timeout set")
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertEquals("Is the socket timeout of the DB connection the expected?",
                        TimeUnit.SECONDS.toMillis(SOCKET_TIMEOUT_SECONDS), de.getConnection().getNetworkTimeout())
                );

        assertEquals("Is the login timeout of the DB connection the expected?", LOGIN_TIMEOUT_SECONDS, loginTimeoutInSeconds);
        assertEquals("Is the check connection timeout of the DB connection the expected?",
                CHECK_CONNECTION_TIMEOUT_SECONDS, checkConnectionTimeoutInSec);
    }

    /**
     * Tests if the timeout settings are invalid a exception is thrown.
     *
     * @throws Exception if the invalid config were detected.
     */
    @Test(expected = DatabaseFactoryException.class)
    public void testInvalidTimeoutsConfigured() throws Exception {
        final Properties invalidProperties = new Properties();
        invalidProperties.putAll(this.dbProps);
        invalidProperties.put(SOCKET_TIMEOUT, 1);

        DatabaseFactory.getConnection(invalidProperties);
    }

    /**
     * Tests that when the DB server is down, the login timeout forces a return from {@link DatabaseFactory#getConnection}
     * in a timely fashion (with an Exception).
     */
    @Test
    public void testLoginTimeout() {
        final Properties testProps = TestRouter.getPatchedDbProperties(dbProps, testRouter.getDbPort(), testRouter.getLocalPort());
        final Future<DatabaseEngine> dbEngineFuture = executor.submit(() -> DatabaseFactory.getConnection(testProps));

        assertThatCode(() -> dbEngineFuture.get(LOGIN_TIMEOUT_SECONDS + TIMEOUT_TOLERANCE_SECONDS, TimeUnit.SECONDS))
                .as("When the DB server is down, the DB engine creation should fail by timeout in the connection")
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(DatabaseFactoryException.class);
    }

    /**
     * Tests that after a connection with the DB server has been successfully established, if that connection is broken
     * a given action returns in a timely fashion (with an Exception), according to the configured socket timeout.
     *
     * In this test the action performed is {@link DatabaseEngine#checkConnection()}.
     * The connection between the test DB engine and the DB server is broken <strong>without being closed</strong>
     * (this may happen for example when a load balancer changes the connections to a DB server without closing the old
     * ones to the client).
     */
    @Test
    public void testNetworkTimeout() throws Exception {
        final Properties props = new Properties();
        props.putAll(dbProps);
        props.put(CHECK_CONNECTION_TIMEOUT_SECONDS, 0);
        testRouter.init();

        final Properties testProps = TestRouter.getPatchedDbProperties(props, testRouter.getDbPort(), testRouter.getLocalPort());

        final DatabaseEngine dbEngine = executor.submit(() -> DatabaseFactory.getConnection(testProps))
                .get(LOGIN_TIMEOUT_SECONDS + TIMEOUT_TOLERANCE_SECONDS, TimeUnit.SECONDS);

        Future<Boolean> connCheckFuture = executor.submit((Callable<Boolean>) dbEngine::checkConnection);
        assertTrue("PDB should be connected to the DB server", connCheckFuture.get(LOGIN_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        testRouter.breakConnections();

        connCheckFuture = executor.submit((Callable<Boolean>) dbEngine::checkConnection);

        /*
         we want to make sure that the login timeout (which is usually lower than socket timeout)
          is not being used for timing out already established connections
         */
        TimeUnit.SECONDS.sleep(LOGIN_TIMEOUT_SECONDS + TIMEOUT_TOLERANCE_SECONDS);
        assertThat(connCheckFuture)
                .as("Connection check should only timeout after at least %d seconds", SOCKET_TIMEOUT_SECONDS)
                .isNotDone();

        assertFalse("After breaking connection, PDB should detect that it is not connected to the DB server",
                connCheckFuture.get(SOCKET_TIMEOUT_SECONDS - LOGIN_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    /**
     * Tests that after a connection with the DB server has been successfully established, if that connection is broken
     * a given action returns in a timely fashion (with an Exception), according to the configured connection check
     * timeout.
     *
     * This test is similar to {@link #testNetworkTimeout()} but it introduces a new configuration for the
     * {@link DatabaseEngine#checkConnection()}.
     */
    @Test
    public void testConnectionVerificationTimeout() throws Exception {
        testRouter.init();
        final Properties testProps = TestRouter.getPatchedDbProperties(dbProps,
                testRouter.getDbPort(),
                testRouter.getLocalPort());

        final DatabaseEngine dbEngine = executor.submit(() -> DatabaseFactory.getConnection(testProps))
                .get(LOGIN_TIMEOUT_SECONDS + TIMEOUT_TOLERANCE_SECONDS, TimeUnit.SECONDS);

        Future<Boolean> connCheckFuture = executor.submit((Callable<Boolean>) dbEngine::checkConnection);
        assertTrue("PDB should be connected to the DB server", connCheckFuture.get(LOGIN_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        assertEquals("Socket timeout should remain the same.",
                TimeUnit.SECONDS.toMillis(SOCKET_TIMEOUT_SECONDS),
                dbEngine.getConnection().getNetworkTimeout());

        testRouter.breakConnections();

        connCheckFuture = executor.submit((Callable<Boolean>) dbEngine::checkConnection);

        /*
         we want to make sure that the login timeout (which is usually lower than socket timeout)
          is not being used for timing out already established connections
         */
        TimeUnit.SECONDS.sleep(LOGIN_TIMEOUT_SECONDS + TIMEOUT_TOLERANCE_SECONDS);
        assertThat(connCheckFuture)
                .as("Connection check should only timeout after at least %d seconds", CHECK_CONNECTION_TIMEOUT_SECONDS)
                .isNotDone();

        assertFalse("PDB should detect that it is not connected to the server before the socket timeout",
                connCheckFuture.get(CHECK_CONNECTION_TIMEOUT_SECONDS - LOGIN_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
}
