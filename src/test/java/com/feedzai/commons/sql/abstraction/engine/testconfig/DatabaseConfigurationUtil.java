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
package com.feedzai.commons.sql.abstraction.engine.testconfig;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Utility to load and filter database configurations from an input.
 * <p/>
 * Resource format must be as follows (example for Oracle):
 * <pre>
 *   (...)
 *
 *   <VENDOR><VERSION>.engine=com.feedzai.commons.sql.abstraction.engine.impl.OracleEngine
 *   <VENDOR><VERSION>.jdbc=jdbc:oracle:thin:@HOSTNAME:1521:SID
 *   <VENDOR><VERSION>.username=USERNAME
 *   <VENDOR><VERSION>.password=PASSSWORD
 *
 *   <VENDOR><VERSION>.engine=com.feedzai.commons.sql.abstraction.engine.impl.OracleEngine
 *   <VENDOR><VERSION>.jdbc=jdbc:oracle:thin:@HOSTNAME:1523:SID
 *   <VENDOR><VERSION>.username=USERNAME
 *   <VENDOR><VERSION>.password=PASSSWORD
 *
 *   (...)
 * </pre>
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class DatabaseConfigurationUtil {
    /**
     * the logger
     */
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfigurationUtil.class);

    /**
     * the configurations loaded
     */
    private Map<String, DatabaseConfiguration> configs;

    /**
     * Creates a new instance of {@link DatabaseConfigurationUtil}.
     */
    private DatabaseConfigurationUtil() {
    }

    /**
     * Reads configuration from a file.
     *
     * @param path The file path.
     * @return A {@link DatabaseConfigurationUtil} object.
     * @throws Exception If something occurs while reading the data.
     */
    public static DatabaseConfigurationUtil from(String path) throws Exception {
        return from(new File(path));
    }

    /**
     * Reads configuration from a {@link File}.
     *
     * @param path The file.
     * @return A {@link DatabaseConfigurationUtil} object.
     * @throws Exception If something occurs while reading the data.
     */
    public static DatabaseConfigurationUtil from(File path) throws Exception {
        return from(new FileInputStream(path));
    }

    /**
     * Reads configuration from an {@link InputStream} closing it at the end.
     *
     * @param is The {@link InputStream}.
     * @return A {@link DatabaseConfigurationUtil} object.
     * @throws Exception If something occurs while reading the data.
     */
    public static DatabaseConfigurationUtil from(InputStream is) throws Exception {
        final DatabaseConfigurationUtil dbu = new DatabaseConfigurationUtil();
        try {
            dbu.loadDatabaseConfigurations(is);
        } finally {
            is.close();
        }

        return dbu;
    }

    /**
     * Gets the configurations loaded from the source.
     *
     * @return The configurations.
     */
    public Map<String, DatabaseConfiguration> getConfigurations() {
        return configs;
    }

    /**
     * Filters the configurations.
     *
     * @param instances The instances to filter.
     * @return The configurations filtered.
     */
    public Map<String, DatabaseConfiguration> filter(final Set<String> instances) {
        return Maps.filterKeys(configs, instances::contains);
    }

    /**
     * Loads the database configurations from the given source.
     *
     * @throws IOException if an error occurred when reading from the input stream.
     */
    private void loadDatabaseConfigurations(InputStream is) throws IOException {
        final Properties properties = new Properties();
        properties.load(is);

        final Map<String, String> config = Maps.fromProperties(properties);
        final Map<String, Collection<String>> propsByVendor = groupByVendor(config);

        this.configs = Maps.transformEntries(
            propsByVendor,
            (vendor, propertiesCollection) -> buildDatabaseConfiguration(vendor, propertiesCollection, config)
        );
    }

    /**
     * Builds a database configuration out of the properties.
     *
     * @param vendor     The vendor.
     * @param properties The properties.
     * @param config     The configuration.
     * @return The database configuration.
     */
    private DatabaseConfiguration buildDatabaseConfiguration(String vendor, Collection<String> properties, Map<String, String> config) {
        final DatabaseConfiguration.Builder builder = new DatabaseConfiguration.Builder().vendor(vendor);

        for (String v : properties) {
            final String prop = config.get(v);

            final String split;
            switch (split = v.split("\\.")[1]) {
                case "jdbc":
                    builder.jdbc(prop);
                    break;
                case "engine":
                    builder.engine(prop);
                    break;
                case "username":
                    builder.username(prop);
                    break;
                case "password":
                    builder.password(prop);
                    break;
                case "schema":
                    builder.schema(prop);
                    break;
                default:
                    logger.warn("Unknown property '{}' in '{}'", split, v);

            }
        }

        return builder.build();
    }

    /**
     * Groups the configuration by vendor.
     *
     * @param config The configuration.
     * @return Configuration grouped by vendor.
     */
    private Map<String, Collection<String>> groupByVendor(Map<String, String> config) {
        return Multimaps.index(config.keySet(), input -> input.split("\\.")[0]).asMap();
    }
}
