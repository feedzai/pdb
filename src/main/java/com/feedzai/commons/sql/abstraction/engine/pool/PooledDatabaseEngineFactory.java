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

import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;

/**
 * The factory of {@link PooledDatabaseEngine} instances.
 *
 * @author Luiz Silva (luiz.silva@feedzai.com)
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.8.3
 */
class PooledDatabaseEngineFactory extends BasePooledObjectFactory<PooledDatabaseEngine> {

    /**
     * The pool in which to pool {@link PooledDatabaseEngine}.
     */
    private GenericObjectPool<PooledDatabaseEngine> pool;

    /**
     * The properties needed to fabricate {@link DatabaseEngine} instances.
     */
    private final Properties properties;

    /**
     * The database engine modifier to fit the application needs.
     */
    private final Consumer<DatabaseEngine> engineModifier;

    /**
     * Creates a new {@link PooledDatabaseEngineFactory}.
     *
     * @param propertiesMap the configured database properties.
     * {@link DatabaseEngine} instances and to configure the pool.
     * @param engineModifier the database engine modifier to fit the application needs.
     */
     PooledDatabaseEngineFactory(final Map<String, String> propertiesMap,
                                 final Consumer<DatabaseEngine> engineModifier) {
        this.engineModifier = engineModifier;
        // database connection properties.
        this.properties = new Properties();

        // load PDB connection properties from configuration file.
        for (final Map.Entry<String, String> entry : propertiesMap.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue() == null ? "" : entry.getValue());
        }
    }

    @Override
    public PooledDatabaseEngine create() throws Exception {
        final DatabaseEngine engine = DatabaseFactory.getConnection(properties);
        // if defined, modify the engine.
        if (engineModifier != null) {
            // modify the engine to fit the application needs.
            engineModifier.accept(engine);
        }
        return new PooledDatabaseEngine(pool, engine);
    }

    @Override
    public boolean validateObject(final PooledObject<PooledDatabaseEngine> pooled) {
        return pooled.getObject().checkConnection();
    }

    @Override
    public PooledObject<PooledDatabaseEngine> wrap(final PooledDatabaseEngine engine) {
        return new DefaultPooledObject<>(engine);
    }

    /**
     * Gets the pool.
     *
     * @return the pool.
     */
    GenericObjectPool<PooledDatabaseEngine> getPool() {
        return pool;
    }

    /**
     * Sets the pool.
     *
     * @param pool the pool.
     */
    void setPool(final GenericObjectPool<PooledDatabaseEngine> pool) {
        this.pool = pool;
    }
}
