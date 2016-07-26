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

import com.google.common.base.Objects;

/**
 * A database configuration to be used in PDB with: engine, jdbc, username and password.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public final class DatabaseConfiguration {
    /**
     * the engine
     */
    public final String engine;
    /**
     * the JDBC connection string
     */
    public final String jdbc;
    /**
     * the username if applicable
     */
    public final String username;
    /**
     * the password if applicable
     */
    public final String password;
    /**
     * the vendor of the database
     */
    public final String vendor;
    /**
     * the schema
     */
    public final String schema;

    /**
     * Creates a new instance of {@link DatabaseConfiguration}.
     *
     * @param engine   The engine.
     * @param jdbc     The JDBC connection string.
     * @param username The username.
     * @param password The password.
     * @param vendor   The vendor.
     */
    public DatabaseConfiguration(String engine, String jdbc, String username, String password, String vendor, String schema) {
        this.engine = engine;
        this.jdbc = jdbc;
        this.username = username;
        this.password = password;
        this.vendor = vendor;
        this.schema = schema;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("engine", engine)
                .add("jdbc", jdbc)
                .add("username", username)
                .add("password", password)
                .add("vendor", vendor)
                .add("schema", schema)
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(engine, jdbc, username, password, vendor);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final DatabaseConfiguration other = (DatabaseConfiguration) obj;
        return Objects.equal(this.engine, other.engine) && Objects.equal(this.jdbc, other.jdbc) && Objects.equal(this.username, other.username) && Objects.equal(this.password, other.password) && Objects.equal(this.vendor, other.vendor);
    }

    /**
     * Builder to create {@link DatabaseConfiguration} objects.
     */
    public static class Builder implements com.feedzai.commons.sql.abstraction.util.Builder<DatabaseConfiguration> {
        /**
         * the engine
         */
        private String engine;
        /**
         * the jdbc connection string
         */
        private String jdbc;
        /**
         * the username
         */
        private String username;
        /**
         * the password
         */
        private String password;
        /**
         * the vendor
         */
        private String vendor;
        /**
         * the schema
         * @since 2.1.6
         */
        private String schema;

        /**
         * Creates a new instance of {@link Builder},
         */
        public Builder() {

        }

        /**
         * Sets the engine.
         * @param engine The engine.
         * @return This builder.
         */
        public Builder engine(String engine) {
            this.engine = engine;

            return this;
        }

        /**
         * Sets the JDBC.
         * @param jdbc The JDBC.
         * @return This builder.
         */
        public Builder jdbc(String jdbc) {
            this.jdbc = jdbc;

            return this;
        }

        /**
         * Sets the username.
         * @param username The username.
         * @return This builder.
         */
        public Builder username(String username) {
            this.username = username;

            return this;
        }

        /**
         * Sets the password.
         * @param password The password.
         * @return This builder.
         */
        public Builder password(String password) {
            this.password = password;

            return this;
        }

        /**
         * Sets the vendor.
         * @param vendor The vendor.
         * @return This builder.
         */
        public Builder vendor(String vendor) {
            this.vendor = vendor;

            return this;
        }

        /**
         * Sets the schema.
         * @param schema The schema.
         * @return This builder.
         * @since 2.1.6
         */
        public Builder schema(String schema) {
            this.schema = schema;

            return this;
        }

        @Override
        public DatabaseConfiguration build() {
            return new DatabaseConfiguration(engine, jdbc, username, password, vendor, schema);
        }
    }
}
