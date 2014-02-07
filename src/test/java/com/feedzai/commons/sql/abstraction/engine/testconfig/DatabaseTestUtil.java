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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

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
    public static Collection<Object[]> loadConfigurations() throws Exception {
        final String connectionLocation = System.getProperty(CONFIG_FILE_LOCATION);
        final InputStream is; // closed later by DatabaseConfigurationUtil.
        if (connectionLocation != null) {
            is = new FileInputStream(connectionLocation);
        } else {
            is = DatabaseTestUtil.class.getClassLoader().getResourceAsStream(CONFIGURATION_FILE);
        }


        final Map<String, DatabaseConfiguration> configs =
                DatabaseConfigurationUtil
                        .from(is)
                        .filter(instancesToTest());

        return FluentIterable
                .from(configs.values())
                .transform(new Function<DatabaseConfiguration, Object[]>() {
                    @Override
                    public Object[] apply(DatabaseConfiguration input) {
                        return new Object[]{input};
                    }
                }).toList();
    }

    /**
     * Filters the instances given by {@code vendor}.
     * <p/>
     * The match is performed by contains, i.e. oracle matches oracle11 and oracle12.
     *
     * @param vendor The list of instances to test.
     * @return The filtered instances to test.
     * @throws Exception
     */
    public static Collection<Object[]> loadConfigurations(final String... vendor) throws Exception {
        return FluentIterable
                .from(loadConfigurations())
                .filter(new Predicate<Object[]>() {
                    @Override
                    public boolean apply(Object[] input) {
                        final DatabaseConfiguration db = (DatabaseConfiguration) input[0];
                        for (String v : vendor) {
                            if (db.vendor.contains(v)) {
                                return true;
                            }
                        }

                        return false;
                    }
                }).toList();
    }

    /**
     * Gets the database instances to test from the {@code INSTANCES} system property.
     * <p/>
     * Must match the ones defined in {@code CONFIGURATION_FILE}.
     *
     * @return A {@link java.util.Set} containing the instances to test.
     */
    private static Set<String> instancesToTest() {
        final String databases = System.getProperty(INSTANCES);

        if (StringUtils.isBlank(databases)) {
            return ImmutableSet.of();
        }

        return ImmutableSet.copyOf(databases.split("\\s*,\\s*"));
    }
}
