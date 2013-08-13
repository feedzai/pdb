/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine;

import java.util.EnumSet;

/**
 * Utility Enumaration to access default information for Database Vendors.
 * @author deag
 */
public enum DatabaseEngineDriver {
    /** The H2 Database vendor. */
    H2("H2", "com.feedzai.commons.sql.abstraction.engine.impl.H2Engine", "org.h2.Driver") {
        @Override
        public String connectionStringFor(String hostname, String database) {
            return String.format("jdbc:h2:%s/%s", hostname, database);
        }
    },
    /** The PostgreSQL vendor. */
    POSTGRES("Postgres", "com.feedzai.commons.sql.abstraction.engine.impl.PostgreSqlEngine", "org.postgresql.Driver") {
        @Override
        public String connectionStringFor(String hostname, String database) {
            return String.format("jdbc:postgresql://%s/%s", hostname, database);
        }
    },
    /** The SQLServer vendor. */
    SQLSERVER("SQLServer", "com.feedzai.commons.sql.abstraction.engine.impl.SqlServerEngine", "com.microsoft.sqlserver.jdbc.SQLServerDriver") {
        @Override
        public String connectionStringFor(String hostname, String database) {
            return String.format("jdbc:sqlserver://%s;database=%s", hostname, database);
        }
    },
    /** The Oracle vendor. */
    ORACLE("Oracle", "com.feedzai.commons.sql.abstraction.engine.impl.OracleEngine", "oracle.jdbc.OracleDriver") {
        @Override
        public String connectionStringFor(String hostname, String database) {
            return String.format("jdbc:oracle:thin:@%s:1521:%s", hostname, database);
        }
    },
    /** The MySQL vendor. */
    MYSQL("MySQL", "com.feedzai.commons.sql.abstraction.engine.impl.MySqlEngine", "com.mysql.jdbc.Driver") {
        @Override
        public String connectionStringFor(String hostname, String database) {
            return String.format("jdbc:mysql://%s/%s", hostname, database);
        }
    },
    /** The DB2 vendor. */
    DB2("DB2", "com.feedzai.commons.sql.abstraction.engine.impl.DB2Engine", "com.ibm.db2.jcc.DB2Driver") {
        @Override
        public String connectionStringFor(String hostname, String database) {
            return String.format("jdbc:db2://%s:50000/%s", hostname, database);
        }
    }
    ;

    /** The name of the vendor */
    private String name;
    /** The engine class. */
    private String engineClass;
    /** The driver class. */
    private String driverClass;

    /**
     * Creates a new instance of {@link DatabaseEngineDriver}.
     * @param name The name of the vendor.
     * @param engine The engine implementation.
     * @param driver The driver class.
     */
    DatabaseEngineDriver(String name, String engine, String driver) {
        this.name = name;
        this.engineClass = engine;
        this.driverClass = driver;
    }

    /**
     * Gets the {@link DatabaseEngineDriver} given the engine.
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
     * @param driverClass
     * @return the database driver of the given driver class
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
     * @return The engine of this {@link DatabaseEngineDriver}.
     */
    public String engine() {
        return engineClass;
    }

    /**
     * @return The driver of this {@link DatabaseEngineDriver}.
     */
    public String driver() {
        return driverClass;
    }

    /**
     * Gets the JDBC connection string given the hostname and the database.
     * @param hostname The hostname The hostname.
     * @param database The database.
     * @return The formatted string to create a JDBC connection.
     */
    public abstract String connectionStringFor(String hostname, String database);
}
