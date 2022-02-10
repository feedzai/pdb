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
package com.feedzai.commons.sql.abstraction.engine;

import java.util.EnumSet;

/**
 * Utility Enumeration to access default information for Database Vendors.
 *
 * @author Diogo Guerra (diogo.guerra@feedzai.com)
 * @since 2.0.0
 */
public enum DatabaseEngineDriver {
    /**
     * The H2 Database vendor.
     */
    H2("H2", "com.feedzai.commons.sql.abstraction.engine.impl.H2Engine", "org.h2.Driver", 0) {
        @Override
        public String connectionStringFor(String hostname, String database) {
            return String.format("jdbc:h2:%s/%s", hostname, database);
        }

        @Override
        public String connectionStringFor(String hostname, String database, int port) {
            return connectionStringFor(hostname, database);
        }
    },
    /**
     * The H2v2 Database vendor.
     */
    H2V2("H2V2", "com.feedzai.commons.sql.abstraction.engine.impl.H2EngineV2", "org.h2.Driver", 0) {
        @Override
        public String connectionStringFor(String hostname, String database) {
            return String.format("jdbc:h2:%s/%s", hostname, database);
        }

        @Override
        public String connectionStringFor(String hostname, String database, int port) {
            return connectionStringFor(hostname, database);
        }
    },
    /**
     * The PostgreSQL vendor.
     */
    POSTGRES("Postgres", "com.feedzai.commons.sql.abstraction.engine.impl.PostgreSqlEngine", "org.postgresql.Driver", 5432) {
        @Override
        public String connectionStringFor(String hostname, String database, int port) {
            return String.format("jdbc:postgresql://%s:%d/%s", hostname, port, database);
        }
    },
    /**
     * The CockroachDB vendor.
     * @since 2.5.0
     */
    COCKROACHDB("CockroachDB", "com.feedzai.commons.sql.abstraction.engine.impl.CockroachDBEngine", "org.postgresql.Driver", 26257) {
        @Override
        public String connectionStringFor(String hostname, String database, int port) {
            return String.format("jdbc:postgresql://%s:%d/%s", hostname, port, database);
        }
    },
    /**
     * The SQLServer vendor.
     */
    SQLSERVER("SQLServer", "com.feedzai.commons.sql.abstraction.engine.impl.SqlServerEngine", "com.microsoft.sqlserver.jdbc.SQLServerDriver", 1433) {
        @Override
        public String connectionStringFor(String hostname, String database, int port) {
            return String.format("jdbc:sqlserver://%s:%d;database=%s", hostname, port, database);
        }
    },
    /**
     * The Oracle vendor.
     */
    ORACLE("Oracle", "com.feedzai.commons.sql.abstraction.engine.impl.OracleEngine", "oracle.jdbc.OracleDriver", 1521) {
        @Override
        public String connectionStringFor(String hostname, String database, int port) {
            return String.format("jdbc:oracle:thin:@%s:%d:%s", hostname, port, database);
        }
    },
    /**
     * The MySQL vendor.
     */
    MYSQL("MySQL", "com.feedzai.commons.sql.abstraction.engine.impl.MySqlEngine", "com.mysql.jdbc.Driver", 3306) {
        @Override
        public String connectionStringFor(String hostname, String database, int port) {
            return String.format("jdbc:mysql://%s:%d/%s", hostname, port, database);
        }
    },
    /**
     * The DB2 vendor.
     */
    DB2("DB2", "com.feedzai.commons.sql.abstraction.engine.impl.DB2Engine", "com.ibm.db2.jcc.DB2Driver", 50000) {
        @Override
        public String connectionStringFor(String hostname, String database, int port) {
            return String.format("jdbc:db2://%s:%d/%s", hostname, port, database);
        }
    };

    /**
     * The name of the vendor.
     */
    private String name;
    /**
     * The engine class.
     */
    private String engineClass;
    /**
     * The driver class.
     */
    private String driverClass;
    /**
     * The port.
     */
    private int port;

    /**
     * Creates a new instance of {@link DatabaseEngineDriver}.
     *
     * @param name   The name of the vendor.
     * @param engine The engine implementation.
     * @param driver The driver class.
     * @param port   The port.
     */
    DatabaseEngineDriver(String name, String engine, String driver, int port) {
        this.name = name;
        this.engineClass = engine;
        this.driverClass = driver;
        this.port = port;
    }

    /**
     * Gets the {@link DatabaseEngineDriver} given the engine.
     *
     * @param engine The engine name.
     * @return The database engine.
     */
    public static DatabaseEngineDriver fromEngine(String engine) {
        for (final DatabaseEngineDriver element : EnumSet.allOf(DatabaseEngineDriver.class)) {
            if (element.engineClass.equals(engine)) {
                return element;
            }
        }
        return null;
    }

    /**
     * Gets the {@link DatabaseEngineDriver} given the driver class.
     *
     * @param driverClass The driver class.
     * @return The database engine.
     */
    public static DatabaseEngineDriver fromDriver(String driverClass) {
        for (final DatabaseEngineDriver element : EnumSet.allOf(DatabaseEngineDriver.class)) {
            if (element.driverClass.equals(driverClass)) {
                return element;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Gets the engine.
     *
     * @return The engine of this {@link DatabaseEngineDriver}.
     */
    public String engine() {
        return engineClass;
    }

    /**
     * Gets the driver.
     *
     * @return The driver of this {@link DatabaseEngineDriver}.
     */
    public String driver() {
        return driverClass;
    }

    /**
     * Gets the default port for this vendor.
     *
     * @return The default port of the vendor.
     * @throws UnsupportedOperationException If the vendor is H2.
     */
    public int defaultPort() throws UnsupportedOperationException {
        if (H2.equals(this)) {
            throw new UnsupportedOperationException("Cannot get a default port for H2 since there's no default port assigned by IANA.");
        }

        return port;
    }

    /**
     * Gets the JDBC connection string given the hostname and the database.
     *
     * @param hostname The hostname The hostname.
     * @param database The database.
     * @return The formatted string to create a JDBC connection.
     */
    public String connectionStringFor(final String hostname, final String database) {
        return connectionStringFor(hostname, database, defaultPort());
    }

    /**
     * Gets the JDBC connection string given the hostname and the database.
     *
     * @param hostname The hostname The hostname.
     * @param database The database.
     * @param port     The port.
     * @return The formatted string to create a JDBC connection.
     */
    public abstract String connectionStringFor(String hostname, String database, int port);
}
