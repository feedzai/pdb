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

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility to read and filter database instances to test.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class DatabaseTestUtil {
    /**
     * the file containing the definitions
     */
    public static final String CONFIGURATION_FILE = "connections.properties";
    /**
     * the system property containing the instances to test
     */
    public static final String INSTANCES = "instances";
    /**
     * the system property to specify the connections.properties file if applicable
     */
    public static final String CONFIG_FILE_LOCATION = "connections";

    /**
     * Loads and transforms the configurations from {@code CONFIGURATION_FILE}.
     *
     * @return The instances to test defined in the configuration file.
     * @throws Exception If something occurs reading the resource.
     */
    public static Collection<DatabaseConfiguration> loadConfigurations() throws Exception {
        final String connectionLocation = System.getProperty(CONFIG_FILE_LOCATION);
        final InputStream is; // closed later by DatabaseConfigurationUtil.
        if (connectionLocation != null) {
            is = new FileInputStream(connectionLocation);
        } else {
            is = DatabaseTestUtil.class.getClassLoader().getResourceAsStream(CONFIGURATION_FILE);
        }

        return DatabaseConfigurationUtil.from(is)
            .filter(instancesToTest())
            .values();
    }

    /**
     * Filters the instances given by {@code vendor}.
     * <p/>
     * The match is performed by contains, i.e. oracle matches oracle11 and oracle12.
     *
     * @param vendors The list of instances to test.
     * @return The filtered instances to test.
     * @throws Exception If something wrong occurs reading the configurations.
     */
    public static Collection<DatabaseConfiguration> loadConfigurations(final String... vendors) throws Exception {
        return loadConfigurations().stream()
            .filter(dbConfig -> {
                for (String vendor : vendors) {
                    if (dbConfig.vendor.equalsIgnoreCase(vendor)) {
                        return true;
                    }
                }

                return false;
            })
            .collect(Collectors.toList());
    }

    /**
     * Gets the database instances to test from the {@code INSTANCES} system property.
     * <p/>
     * Must match the ones defined in {@code CONFIGURATION_FILE}.
     *
     * @return A {@link Set} containing the instances to test.
     */
    private static Set<String> instancesToTest() {
        final String databases = System.getProperty(INSTANCES);

        if (StringUtils.isBlank(databases)) {
            return ImmutableSet.of();
        }

        return ImmutableSet.copyOf(databases.split("\\s*,\\s*"));
    }
}
