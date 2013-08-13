/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine.configuration;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.feedzai.commons.sql.abstraction.util.StringUtil;

import java.sql.Connection;
import java.util.Map;
import java.util.Properties;

/**
 * Properties object to use.
 */
public class PdbProperties extends Properties implements Cloneable {

    /**
     * The JDBC property name.
     */
    public static final String JDBC = "pdb.jdbc";
    /**
     * The USERNAME property name.
     */
    public static final String USERNAME = "pdb.username";
    /**
     * The PASSWORD property name.
     */
    public static final String PASSWORD = "pdb.password";
    /**
     * VARCHAR size property name.
     */
    public static final String VARCHAR_SIZE = "pdb.varchar_size";
    /**
     * Schema policy property name.
     */
    public static final String SCHEMA_POLICY = "pdb.schema_policy";
    /**
     * The engine property
     */
    public static final String ENGINE = "pdb.engine";
    /**
     * Database schema property name.
     */
    public static final String SCHEMA = "pdb.schema";
    /**
     * The maximum identifier size property
     */
    public static final String MAX_IDENTIFIER_SIZE = "pdb.max_identifier_size";
    /**
     * The maximum blob size property
     */
    public static final String MAX_BLOB_SIZE = "pdb.max_blob_size";
    /**
     * The blob buffer size property
     */
    public static final String BLOB_BUFFER_SIZE = "pdb.blob_buffer_size";
    /**
     * The maximum number of retries.
     */
    public static final String MAX_NUMBER_OF_RETRIES = "pdb.max_number_retries";
    /**
     * The retry interval.
     */
    public static final String RETRY_INTERVAL = "pdb.retry_interval";
    /**
     * The default isolation level.
     */
    public static final String ISOLATION_LEVEL = "pdb.isolation_level";
    /**
     * The database driver.
     */
    public static final String DRIVER = "pdb.driver";
    /**
     * The property of reconnection.
     */
    public static final String RECONNECT_ON_LOST = "pdb.reconnect_on_lost";
    /**
     * The property for using encrypted passwords.
     */
    public static final String ENCRYPTED_PASSWORD = "pdb.encrypted_password";
    /**
     * The location of the private key for passwords.
     */
    public static final String SECRET_LOCATION = "pdb.secret_location";
    /**
     * The property for using encrypted usernames.
     */
    public static final String ENCRYPTED_USERNAME = "pdb.encrypted_username";
    /**
     * Property to allow column drops on entity updates.
     */
    public static final String ALLOW_COLUMN_DROP = "pdb.allow_column_drop";

    /**
     * Creates a new instance of {@link PdbProperties} with the default configuration.
     *
     * @param useDefaults True if default properties are to be set.
     */
    public PdbProperties(final boolean useDefaults) {
        super();
        if (useDefaults) {
            // The default properties
            setProperty(VARCHAR_SIZE, "256");
            setProperty(SCHEMA_POLICY, "create");
            setProperty(MAX_IDENTIFIER_SIZE, "30");
            setProperty(MAX_BLOB_SIZE, "-1");
            setProperty(BLOB_BUFFER_SIZE, "1048576"); // 1 mB.
            setProperty(MAX_NUMBER_OF_RETRIES, "-1");
            setProperty(RETRY_INTERVAL, "10000");
            setProperty(ISOLATION_LEVEL, "read_committed");
            setProperty(RECONNECT_ON_LOST, "true");
            setProperty(SECRET_LOCATION, "conf/engine/secret/secret.key");
            setProperty(ALLOW_COLUMN_DROP, "true");
        }
    }

    /**
     * Merges the given properties with the default configuration.
     *
     * @param properties The properties to merge.
     * @param useDefaults True if default properties are to be set.
     */
    public PdbProperties(final Properties properties, final boolean useDefaults) {
        this(useDefaults);
        merge(properties);
    }

    /**
     * Merge properties with the existing.
     *
     * @param properties The properties to merge.
     */
    public final void merge(final Properties properties) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            setProperty(entry.getKey().toString(), entry.getValue().toString());
        }
    }

    public boolean isEncryptedUsername() {
        return !StringUtil.isBlank(getProperty(ENCRYPTED_USERNAME));
    }

    public boolean isEncryptedPassword() {
        return !StringUtil.isBlank(getProperty(ENCRYPTED_PASSWORD));
    }

    /**
     * @return True if the schema is create-drop.
     */
    public boolean isSchemaCreateDrop() {
        return getProperty(SCHEMA_POLICY).equals("create-drop");
    }

    /**
     * @return True if the schema is drop-create.
     */
    public boolean isSchemaDropCreate() {
        return getProperty(SCHEMA_POLICY).equals("drop-create");
    }

    /**
     * @return True if the schema is create.
     */
    public boolean isSchemaCreate() {
        return getProperty(SCHEMA_POLICY).equals("create");
    }

    /**
     * @return True if the schema is none.
     */
    public boolean isSchemaNone() {
        return getProperty(SCHEMA_POLICY).equals("none");
    }

    /**
     * @return Gets the maximum identifier size.
     */
    public int getMaxIdentifierSize() {
        return Integer.parseInt(getProperty(MAX_IDENTIFIER_SIZE));
    }

    /**
     * @return Gets the maximum blob size.
     */
    public int getMaxBlobSize() {
        return Integer.parseInt(getProperty(MAX_BLOB_SIZE));
    }

    /**
     * @return True if the maximum blob size is set, false otherwise.
     */
    public boolean isMaxBlobSizeSet() {
        return getMaxBlobSize() != -1;
    }

    /**
     * @return The blob buffer size for Object -> ByteArray conversions.
     */
    public int getBlobBufferSize() {
        return Integer.parseInt(getProperty(BLOB_BUFFER_SIZE));
    }

    /**
     * @return Maximum number of retries.
     */
    public int getMaxRetries() {
        return Integer.parseInt(getProperty(MAX_NUMBER_OF_RETRIES));
    }

    /**
     * @return The retry interval.
     */
    public long getRetryInterval() {
        return Long.parseLong(getProperty(RETRY_INTERVAL));
    }

    /**
     * @return True if the connection is uses reconnect on lost, false otherwise.
     */
    public boolean isReconnectOnLost() {
        return Boolean.parseBoolean(getProperty(RECONNECT_ON_LOST));
    }

    /**
     * @return The isolation level.
     */
    public int getIsolationLevel() {
        final String isolation = getProperty(ISOLATION_LEVEL);

        if (isolation.equals("read_uncommitted")) {
            return Connection.TRANSACTION_READ_UNCOMMITTED;
        }

        if (isolation.equals("read_committed")) {
            return Connection.TRANSACTION_READ_COMMITTED;
        }

        if (isolation.equals("repeatable_read")) {
            return Connection.TRANSACTION_REPEATABLE_READ;
        }

        if (isolation.equals("serializable")) {
            return Connection.TRANSACTION_SERIALIZABLE;
        }

        throw new DatabaseEngineRuntimeException(String.format("Unrecognizable isolation '%s'", isolation));
    }

    /**
     * @return The JDBC URL.
     */
    public String getJdbc() {
        return getProperty(JDBC);
    }

    /**
     * @return The username.
     */
    public String getUsername() {
        return getProperty(USERNAME);
    }

    /**
     * @return The password.
     */
    public String getPassword() {
        return getProperty(PASSWORD);
    }

    /**
     * @return The engine.
     */
    public String getEngine() {
        return getProperty(ENGINE);
    }

    /**
     * @return The database schema.
     */
    public String getSchema() {
        return getProperty(SCHEMA);
    }

    /**
     * @return Whether or not a database schema is defined.
     */
    public boolean existsSchema() {
        return !StringUtil.isBlank(getSchema());
    }

    /**
     * @return If the connection has permissions to drop columns on entity updates.
     */
    public boolean allowColumnDrop(){
        return Boolean.parseBoolean(getProperty(ALLOW_COLUMN_DROP));
    }

    /**
     * Method to check the mandatory fields.
     *
     * @throws PdbConfigurationException If mandatory fields are not set.
     */
    public void checkMandatoryProperties() throws PdbConfigurationException {
        StringBuilder exceptionMessage = new StringBuilder();
        if (StringUtil.isBlank(getJdbc())) {
            exceptionMessage.append("- A connection string should be declared under the 'database.jdbc' property.\n");
        }

        if (StringUtil.isBlank(getEngine())) {
            exceptionMessage.append("- An engine string should be declared under the 'database.engine' property.\n");
        }

        if (exceptionMessage.length() > 0) {
            throw new PdbConfigurationException("The following configuration errors were detected in the configuration file: \n"
                    + exceptionMessage.toString());
        }
    }

    @Override
    public PdbProperties clone() {
        return new PdbProperties(this, false);
    }
}
