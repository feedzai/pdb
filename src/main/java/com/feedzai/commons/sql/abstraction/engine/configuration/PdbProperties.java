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
package com.feedzai.commons.sql.abstraction.engine.configuration;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.google.common.base.Enums;
import com.google.common.base.Optional;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.util.Constants.DEFAULT_ALLOW_COLUMN_DROP;
import static com.feedzai.commons.sql.abstraction.util.Constants.DEFAULT_BLOB_BUFFER_SIZE;
import static com.feedzai.commons.sql.abstraction.util.Constants.DEFAULT_FETCH_SIZE;
import static com.feedzai.commons.sql.abstraction.util.Constants.DEFAULT_ISOLATION_LEVEL;
import static com.feedzai.commons.sql.abstraction.util.Constants.DEFAULT_MAXIMUM_TIME_BATCH_SHUTDOWN;
import static com.feedzai.commons.sql.abstraction.util.Constants.DEFAULT_MAX_IDENTIFIER_SIZE;
import static com.feedzai.commons.sql.abstraction.util.Constants.DEFAULT_RECONNECT_ON_LOST;
import static com.feedzai.commons.sql.abstraction.util.Constants.DEFAULT_RETRY_INTERVAL;
import static com.feedzai.commons.sql.abstraction.util.Constants.DEFAULT_SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.util.Constants.DEFAULT_SECRET_LOCATION;
import static com.feedzai.commons.sql.abstraction.util.Constants.DEFAULT_SHOULD_COMPRESS_LOBS;
import static com.feedzai.commons.sql.abstraction.util.Constants.DEFAULT_VARCHAR_SIZE;

/**
 * Represents the possible properties to include when configuring the
 * engine.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class PdbProperties extends Properties implements com.feedzai.commons.sql.abstraction.util.Cloneable<PdbProperties> {
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
     * The engine property.
     */
    public static final String ENGINE = "pdb.engine";
    /**
     * The translator.
     */
    public static final String TRANSLATOR = "pdb.translator";
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
     * Property that indicates the fetch size for queries.
     */
    public static final String FETCH_SIZE = "pdb.fetch_size";
    /**
     * Property that indicates how much time to wait for a batch to shutdown.
     */
    public static final String MAXIMUM_TIME_BATCH_SHUTDOWN = "pdb.maximum_await_time_batch";
    /**
     * Property that indicates the lobs should be compressed. This depends on the database implementation.
     */
    public static final String SHOULD_COMPRESS_LOBS = "pdb.compress_lobs";

    /**
     * Creates a new instance of an empty {@link PdbProperties}.
     */
    public PdbProperties() {
        this(false);
    }

    /**
     * Creates a new instance of {@link PdbProperties} with the default configuration.
     *
     * @param useDefaults {@code true} if default properties are to be set, {@code false} otherwise.
     */
    public PdbProperties(final boolean useDefaults) {
        super();
        if (useDefaults) {
            // The default properties
            setProperty(VARCHAR_SIZE, DEFAULT_VARCHAR_SIZE);
            setProperty(SCHEMA_POLICY, DEFAULT_SCHEMA_POLICY);
            setProperty(MAX_IDENTIFIER_SIZE, DEFAULT_MAX_IDENTIFIER_SIZE);
            setProperty(MAX_BLOB_SIZE, -1);
            setProperty(BLOB_BUFFER_SIZE, DEFAULT_BLOB_BUFFER_SIZE); // 1 mB.
            setProperty(MAX_NUMBER_OF_RETRIES, -1);
            setProperty(RETRY_INTERVAL, DEFAULT_RETRY_INTERVAL);
            setProperty(ISOLATION_LEVEL, DEFAULT_ISOLATION_LEVEL);
            setProperty(RECONNECT_ON_LOST, DEFAULT_RECONNECT_ON_LOST);
            setProperty(SECRET_LOCATION, DEFAULT_SECRET_LOCATION);
            setProperty(ALLOW_COLUMN_DROP, DEFAULT_ALLOW_COLUMN_DROP);
            setProperty(FETCH_SIZE, DEFAULT_FETCH_SIZE);
            setProperty(MAXIMUM_TIME_BATCH_SHUTDOWN, DEFAULT_MAXIMUM_TIME_BATCH_SHUTDOWN);
            setProperty(SHOULD_COMPRESS_LOBS, DEFAULT_SHOULD_COMPRESS_LOBS);
        }
    }

    /**
     * Adds a property object by converting it to string.
     *
     * @param key The key.
     * @param o   The object to add.
     */
    public void setProperty(String key, Object o) {
        setProperty(key, o.toString());
    }

    /**
     * Merges the given properties with the default configuration.
     *
     * @param properties  The properties to merge.
     * @param useDefaults {@code true} if default properties are to be set, {@code false} otherwise.
     */
    public PdbProperties(final Properties properties, final boolean useDefaults) {
        this(useDefaults);
        merge(properties);
    }

    /**
     * Merges properties with the existing ones.
     *
     * @param properties The properties to merge.
     */
    public final void merge(final Properties properties) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            setProperty(entry.getKey().toString(), entry.getValue().toString());
        }
    }

    /**
     * Checks if a provided translator class is set.
     *
     * @return {@code true} if a provided translator class is set, {@code false} otherwise.
     */
    public boolean isTranslatorSet() {
        return !StringUtils.isBlank(getTranslator());
    }

    /**
     * Checks if username encryption is set.
     *
     * @return {@code true} if username encryption is set, {@code false} otherwise.
     */
    public boolean isEncryptedUsername() {
        return !StringUtils.isBlank(getProperty(ENCRYPTED_USERNAME));
    }

    /**
     * Checks if password encryption is set.
     *
     * @return {@code true} if password encryption is set, {@code false} otherwise.
     */
    public boolean isEncryptedPassword() {
        return !StringUtils.isBlank(getProperty(ENCRYPTED_PASSWORD));
    }

    /**
     * Gets the fetch size.
     *
     * @return the fetch size.
     */
    public int getFetchSize() {
        return Integer.parseInt(getProperty(FETCH_SIZE));
    }

    /**
     * Gets the maximum await time for batches to shutdown.
     *
     * @return The maximum await time for batches to shutdown.
     */
    public long getMaximumAwaitTimeBatchShutdown() {
        return Long.parseLong(getProperty(MAXIMUM_TIME_BATCH_SHUTDOWN));
    }

    /**
     * Checks if schema policy is CREATE DROP.
     *
     * @return {@code true} if the schema is create-drop, {@code false} otherwise.
     */
    public boolean isSchemaPolicyCreateDrop() {
        return "create-drop".equals(getProperty(SCHEMA_POLICY));
    }

    /**
     * Checks if schema policy is DROP CREATE.
     *
     * @return {@code true} if the schema is drop-create, {@code false} otherwise.
     */
    public boolean isSchemaPolicyDropCreate() {
        return "drop-create".equals(getProperty(SCHEMA_POLICY));
    }

    /**
     * Checks if the schema policy is CREATE.
     *
     * @return {@code true} if the schema is create, {@code false} otherwise.
     */
    public boolean isSchemaPolicyCreate() {
        return "create".equals(getProperty(SCHEMA_POLICY));
    }

    /**
     * Checks if schema policy is NONE.
     *
     * @return {@code true} if the schema is none, {@code false} otherwise.
     */
    public boolean isSchemaPolicyNone() {
        return "none".equals(getProperty(SCHEMA_POLICY));
    }

    /**
     * Gets the maximum identifier size.
     *
     * @return Gets the maximum identifier size.
     */
    public int getMaxIdentifierSize() {
        return Integer.parseInt(getProperty(MAX_IDENTIFIER_SIZE));
    }

    /**
     * Gets the maximum blob size.
     *
     * @return Gets the maximum blob size.
     */
    public int getMaxBlobSize() {
        return Integer.parseInt(getProperty(MAX_BLOB_SIZE));
    }

    /**
     * Checks if the {@link #getMaxBlobSize()} is set.
     *
     * @return {@code true} if the maximum blob size is set, {@code false} otherwise.
     */
    public boolean isMaxBlobSizeSet() {
        return getMaxBlobSize() != -1;
    }

    /**
     * Gets the blob buffer size.
     *
     * @return The blob buffer size for Object -> ByteArray conversions.
     */
    public int getBlobBufferSize() {
        return Integer.parseInt(getProperty(BLOB_BUFFER_SIZE));
    }

    /**
     * Gets the maximum number of retries.
     *
     * @return Maximum number of retries.
     */
    public int getMaxRetries() {
        return Integer.parseInt(getProperty(MAX_NUMBER_OF_RETRIES));
    }

    /**
     * Gets the retry interval.
     *
     * @return The retry interval.
     */
    public long getRetryInterval() {
        return Long.parseLong(getProperty(RETRY_INTERVAL));
    }

    /**
     * Checks if reconnect on connection lost is set.
     *
     * @return {@code true} if the connection is uses reconnect on lost, {@code false} otherwise.
     */
    public boolean isReconnectOnLost() {
        return Boolean.parseBoolean(getProperty(RECONNECT_ON_LOST));
    }

    /**
     * Checks if LOBS columns should be compressed.
     *
     * @return {@code true} if LOBS should be compressed, {@code false} otherwise.
     */
    public boolean shouldCompressLobs() {
        return Boolean.parseBoolean(getProperty(SHOULD_COMPRESS_LOBS));
    }

    /**
     * Gets the isolation level.
     *
     * @return The isolation level.
     */
    public int getIsolationLevel() {
        final Optional<IsolationLevel> e = Enums.getIfPresent(IsolationLevel.class, getProperty(ISOLATION_LEVEL).toUpperCase());

        if (!e.isPresent()) {
            throw new DatabaseEngineRuntimeException(ISOLATION_LEVEL + " must be set and be one of the following: " + EnumSet.allOf(IsolationLevel.class));
        }

        switch (e.get()) {
            case READ_UNCOMMITTED:
                return Connection.TRANSACTION_READ_UNCOMMITTED;
            case READ_COMMITTED:
                return Connection.TRANSACTION_READ_COMMITTED;
            case REPEATABLE_READ:
                return Connection.TRANSACTION_REPEATABLE_READ;
            case SERIALIZABLE:
                return Connection.TRANSACTION_SERIALIZABLE;
            default:
                // Never happens.
                throw new DatabaseEngineRuntimeException("New isolation level?!" + e.get());
        }
    }

    /**
     * Gets the JDBC URL.
     *
     * @return The JDBC URL.
     */
    public String getJdbc() {
        return getProperty(JDBC);
    }

    /**
     * Gets the username.
     *
     * @return The username.
     */
    public String getUsername() {
        return getProperty(USERNAME);
    }

    /**
     * Gets the password.
     *
     * @return The password.
     */
    public String getPassword() {
        return getProperty(PASSWORD);
    }

    /**
     * Gets the engine.
     *
     * @return The engine.
     */
    public String getEngine() {
        return getProperty(ENGINE);
    }

    /**
     * Gets the translator class.
     *
     * @return The translator class.
     */
    public String getTranslator() {
        return getProperty(TRANSLATOR);
    }

    /**
     * Gets the database schema.
     *
     * @return The database schema.
     */
    public String getSchema() {
        return getProperty(SCHEMA);
    }

    /**
     * Checks if the driver property is set.
     *
     * @return {@code true} if the driver property is set, {@code false} otherwise.
     */
    public boolean isDriverSet() {
        return getProperty(DRIVER) != null;
    }

    /**
     * Checks if the the schema is set.
     *
     * @return {@code true} if the schema is set, {@code false} otherwise.
     */
    public boolean isSchemaSet() {
        return StringUtils.isNotBlank(getSchema());
    }

    /**
     * Checks if column drop is allowed when the schema changes.
     *
     * @return {@code true} if the column drop is allowed, {@code false} otherwise.
     */
    public boolean allowColumnDrop() {
        return Boolean.parseBoolean(getProperty(ALLOW_COLUMN_DROP));
    }

    /**
     * Checks if the configuration validates for mandatory properties.
     *
     * @throws PdbConfigurationException If mandatory fields are not set.
     */
    public void checkMandatoryProperties() throws PdbConfigurationException {
        StringBuilder exceptionMessage = new StringBuilder();
        if (StringUtils.isBlank(getJdbc())) {
            exceptionMessage.append("- A connection string should be declared under the 'database.jdbc' property.\n");
        }

        if (StringUtils.isBlank(getEngine())) {
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

    /**
     * Gets the driver property.
     *
     * @return The driver property.
     */
    public String getDriver() {
        return getProperty(DRIVER);
    }
}
