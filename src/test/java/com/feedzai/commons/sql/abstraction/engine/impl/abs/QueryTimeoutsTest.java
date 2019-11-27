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
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineTimeoutException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.google.common.base.Stopwatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SELECT_QUERY_TIMEOUT;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;


/**
 * Unit tests for the query timeout feature. Tests run a query that takes very long to run
 * with a short timeout and verify that the statement aborts with the right error code.
 */
@RunWith(Parameterized.class)
public class QueryTimeoutsTest {

    /**
     * Default timeout in all tests.
     */
    private static final int TEST_TIMEOUT_SECS = 2;

    /**
     * Timeout override in all tests with timeout overrides.
     */
    private static final int TEST_TIMEOUT_OVERRIDE_SECS = 6;

    /**
     * The minimum time the test query takes to run, in seconds.
     */
    private static final int QUERY_MINIMUM_TIME_SECS = 59;

    /**
     * A query that takes at least {@link #QUERY_MINIMUM_TIME_SECS} seconds to run and that
     * is used in all tests.
     */
    private String heavyQuery;

    /**
     * The {@link DatabaseEngine} used in each tests.
     */
    private DatabaseEngine engine;

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations();
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

    @Before
    public void setupTest() throws DatabaseFactoryException {
        final Properties dbProps = new Properties() {
            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "create-drop");
                put(SELECT_QUERY_TIMEOUT, TEST_TIMEOUT_SECS);
            }
        };

        // Set heavyQuery with a query that takes a long time to run
        final DatabaseEngineDriver driver = DatabaseEngineDriver.fromEngine(dbProps.getProperty(ENGINE));
        switch (driver) {
            case POSTGRES:
                heavyQuery = "SELECT pg_sleep(" + QUERY_MINIMUM_TIME_SECS + ")";
                break;
            case SQLSERVER:
                heavyQuery = "WAITFOR DELAY '00:00:" + QUERY_MINIMUM_TIME_SECS + "'; SELECT 1";
                break;
            case MYSQL:
                heavyQuery = "SELECT SLEEP(" + QUERY_MINIMUM_TIME_SECS + ")";
                break;
            default:
                assumeTrue(driver + " engine doesn't support setting timeouts, tests will be skipped", false);
        }

        // Create connection
        engine = DatabaseFactory.getConnection(dbProps);
    }

    @After
    public void cleanResources() {
        if (engine != null) {
            engine.close();
        }
    }

    @Test(expected = DatabaseEngineTimeoutException.class)
    public void iteratorWithDefaultsTest() throws DatabaseEngineException {
        engine.iterator(heavyQuery).nextResult();
    }

    @Test(expected = DatabaseEngineTimeoutException.class)
    public void iteratorWithOverrideTest() throws DatabaseEngineException {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            engine.iterator(heavyQuery, 1000, TEST_TIMEOUT_OVERRIDE_SECS).nextResult();
        } finally {
            assertTrue(stopwatch.elapsed(TimeUnit.SECONDS) >= TEST_TIMEOUT_OVERRIDE_SECS - 1);
        }
    }

    @Test(expected = DatabaseEngineTimeoutException.class)
    public void queryWithDefaultsTest() throws DatabaseEngineException {
        engine.query(heavyQuery);
    }

    @Test(expected = DatabaseEngineTimeoutException.class)
    public void queryWithOverrideTest() throws DatabaseEngineException {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            engine.query(heavyQuery, 10);
        } finally {
            assertTrue(stopwatch.elapsed(TimeUnit.SECONDS) >= TEST_TIMEOUT_OVERRIDE_SECS - 1);
        }
    }

}
