/*
 * Copyright 2021 Feedzai
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
package com.feedzai.commons.sql.abstraction.engine.pool;

import com.google.common.collect.Maps;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.groupingBy;

/**
 * A pool of {@link DatabaseEngine} instances.
 *
 * @author Luiz Silva (luiz.silva@feedzai.com)
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.8.3
 */
public class DatabaseEnginePool implements AutoCloseable {

    /**
     * The logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseEnginePool.class);

    /**
     * The actual pool.
     */
    private final GenericObjectPool<PooledDatabaseEngine> pool;

    /**
     * Pool name for logging purposes.
     */
    private final String poolName;

    /**
     * Pool JDBC string for testing purposes.
     */
    private final String poolJdbc;

    /**
     * The maximum number of connections.
     */
    private final int maxTotal;

    /**
     * Creates a new {@link DatabaseEnginePool}.
     *
     * @param properties the configured database and pool properties.
     * @param engineModifier the database engine modifier to fit the application needs.
     */
    private DatabaseEnginePool(final Map<String, String> properties, final Consumer<DatabaseEngine> engineModifier) {

        // The properties object comes from the configuration file and are passed to the factory, which in turn
        // is passed to the PDB database engine factory.
        final Map<String, List<Entry<String, String>>> poolEntriesGroupedByPrefix = properties
                .entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("pool.generic.")
                        || entry.getKey().startsWith("pool.abandoned."))
                .collect(groupingBy(entry -> entry.getKey().split("\\.")[1]));

        final List<Entry<String, String>> genericProps = poolEntriesGroupedByPrefix.get("generic");
        final GenericObjectPoolConfig<PooledDatabaseEngine> genericConfig = genericProps == null
                ? new GenericObjectPoolConfig<>()
                : applyPrefixedConfig("pool.generic.", genericProps, new GenericObjectPoolConfig<>());


        final List<Entry<String, String>> abandonedProps = poolEntriesGroupedByPrefix.get("abandoned");
        final AbandonedConfig abandonedConfig = abandonedProps == null ? null
                : applyPrefixedConfig("pool.abandoned.", abandonedProps, new AbandonedConfig());

        final PooledDatabaseEngineFactory factory = new PooledDatabaseEngineFactory(properties, engineModifier);
        this.pool = new GenericObjectPool<>(factory, genericConfig, abandonedConfig);
        // format to have a pretty name.
        this.poolName = factory.getClass().getSimpleName().replace("Factory", "Pool");
        this.poolJdbc = properties.get("pdb.jdbc");
        this.maxTotal = genericConfig.getMaxTotal();
        LOGGER.info("'{}' has max size of {}", poolName, maxTotal);

        factory.setPool(this.pool);
    }

    /**
     * Apply a set of prefixed properties (represented as a list of entries) on a configuration object. Applying a
     * prefixed property stands for reading a property, remove its prefix, and call the corresponding setters on the
     * target configuration object.
     *
     * @param prefix the prefix.
     * @param properties the properties.
     * @param configObj the configuration object.
     * @param <T> the type of the configuration object.
     * @return the configured object.
     */
    private <T> T applyPrefixedConfig(final String prefix, final List<Entry<String, String>> properties,
            final T configObj) {
        properties.stream()
            .filter(entry -> entry.getKey().startsWith(prefix))
            .forEach(entry -> {
                    final String configurationName = entry.getKey();

                    try {
                        BeanUtils.setProperty(configObj, StringUtils.removeStart(configurationName, prefix),
                                entry.getValue());
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        LOGGER.error("Invalid configuration '{}'", configurationName, e);
                    }
                });

        return configObj;
    }

    /**
     * Borrows a database engine from the pool. Call {@link DatabaseEngine#close()} to return the borrowed database
     * engine to pool.
     *
     * @return a database engine.
     */
    public DatabaseEngine borrow() {
        // Log pool statistics every time we try to borrow a connection.
        logStats();
        try {
            final long startTime = System.nanoTime();
            final PooledDatabaseEngine pooledDb = pool.borrowObject();
            // if trace is enabled measure the time taken to get an object from the pool.
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("'{}' - Took {}ms to get a database engine.",
                             poolName,
                             NANOSECONDS.toMillis(System.nanoTime() - startTime));
            }
            return pooledDb;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Logs the number of active and idle pool connections.
     */
    private void logStats() {
        final int numActive = pool.getNumActive();
        // warn the user if the number of active connections is equal to max pool size.
        // this means that the application will be blocked waiting for DB.
        if (numActive == maxTotal) {
            LOGGER.warn("'{}' - WARNING: There isn't more connections on the pool. "
                                 + "The application will be blocked and it may perform poorly. "
                                 + "You may consider to increase the pool size, if this is a common occurrence.",
                         poolName);
            LOGGER.warn("'{}' - Check the documentation to learn how to do it.", poolName);
        }

        // Avoid processing the logging strings if trace is disabled.
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{} Active Connections: {}", poolName, numActive);
            LOGGER.trace("{} Idle Connections: {}", poolName, pool.getNumIdle());
            LOGGER.trace("{} Total Connections: {}", poolName, maxTotal);
        }
    }

    /**
     * Closes the pool.
     */
    public void close() {
        pool.close();
    }

    /**
     * Returns whether the pool is closed or not.
     *
     * @return true if the pool is closed, false otherwise.
     */
    public boolean isClosed() {
        return pool.isClosed();
    }

    /**
     * Gets the Pool JDBC string.
     *
     * @return the Pool JDBC string.
     */
    public String getPoolJdbc() {
        return poolJdbc;
    }

    /**
     * Gets the maximum number of connections.
     *
     * @return the maximum number of connections.
     */
    public int getMaxTotal() {
        return maxTotal;
    }

    /**
     * Gets the number of borrowed connections.
     *
     * @return the number of borrowed connections.
     */
    public int getNumActive() {
        return pool.getNumActive();
    }

    /**
     * Gets the pool name.
     *
     * @return the pool name.
     */
    public String getPoolName() {
        return poolName;
    }

    /**
     * Forces invalidation of a pool's {@link DatabaseEngine}.
     *
     * @apiNote method used for testing.
     * @param engine the pooled database engine to invalidate.
     * @throws Exception if an exception occurs destroying the object.
     * @throws IllegalStateException if obj does not belong to this pool.
     */
    void invalidate(PooledDatabaseEngine engine) throws Exception {
        pool.invalidateObject(engine);
    }

    /**
     * Creates a new {@link DatabaseEnginePool}.
     *
     * @param properties the configured database and pool properties.
     * @param engineModifier the database engine modifier to fit the application needs.
     * @return a new DatabaseEnginePool.
     */
    public static DatabaseEnginePool getConnectionPool(final Map<String, String> properties,
                                                       final Consumer<DatabaseEngine> engineModifier) {
        return new DatabaseEnginePool(properties, engineModifier);
    }

    /**
     * Creates a new {@link DatabaseEnginePool}.
     *
     * @param properties the configured database and pool properties.
     * @return a new DatabaseEnginePool.
     */
    public static DatabaseEnginePool getConnectionPool(final Map<String, String> properties) {
        return new DatabaseEnginePool(properties, db -> {});
    }

    /**
     * Creates a new {@link DatabaseEnginePool}.
     *
     * @param properties the configured database and pool properties.
     * @param engineModifier the database engine modifier to fit the application needs.
     * @return a new DatabaseEnginePool.
     */
    public static DatabaseEnginePool getConnectionPool(final Properties properties,
                                                       final Consumer<DatabaseEngine> engineModifier) {
        return new DatabaseEnginePool(Maps.fromProperties(properties), engineModifier);
    }

    /**
     * Creates a new {@link DatabaseEnginePool}.
     *
     * @param properties the configured database and pool properties.
     * @return a new DatabaseEnginePool.
     */
    public static DatabaseEnginePool getConnectionPool(final Properties properties) {
        return new DatabaseEnginePool(Maps.fromProperties(properties), db -> {});
    }

    /**
     * Creates a new {@link DatabaseEnginePool}.
     *
     * @param properties the configured database and pool properties.
     * @param engineModifier the database engine modifier to fit the application needs.
     * @return a new DatabaseEnginePool.
     */
    public static DatabaseEnginePool getConnectionPool(final PdbProperties properties,
                                                       final Consumer<DatabaseEngine> engineModifier) {
        return new DatabaseEnginePool(Maps.fromProperties(properties), engineModifier);
    }

    /**
     * Creates a new {@link DatabaseEnginePool}.
     *
     * @param properties the configured database and pool properties.
     * @return a new DatabaseEnginePool.
     */
    public static DatabaseEnginePool getConnectionPool(final PdbProperties properties) {
        return new DatabaseEnginePool(Maps.fromProperties(properties), db -> {});
    }
}
