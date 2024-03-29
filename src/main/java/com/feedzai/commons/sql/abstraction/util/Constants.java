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
package com.feedzai.commons.sql.abstraction.util;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineTimeoutException;
import com.feedzai.commons.sql.abstraction.engine.configuration.IsolationLevel;
import com.feedzai.commons.sql.abstraction.exceptions.DatabaseEngineRetryableException;
import com.feedzai.commons.sql.abstraction.exceptions.DatabaseEngineRetryableRuntimeException;
import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Common constants to be used in PDB.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public final class Constants {
    /**
     * The unit separator character.
     */
    public static final char UNIT_SEPARATOR_CHARACTER = '\u001F';
    /**
     * The value that represents absence of a timeout.
     */
    public static final int NO_TIMEOUT = 0;
    /**
     * The default var char size.
     */
    public static final int DEFAULT_VARCHAR_SIZE = 256;
    /**
     * The default schema policy.
     */
    public static final String DEFAULT_SCHEMA_POLICY = "create";
    /**
     * The default maximum identifier size.
     */
    public static final int DEFAULT_MAX_IDENTIFIER_SIZE = 30;
    /**
     * The default buffer size for blobs.
     */
    public static final int DEFAULT_BLOB_BUFFER_SIZE = 1048576;
    /**
     * The default retry interval. 10sec.
     */
    public static final long DEFAULT_RETRY_INTERVAL = TimeUnit.SECONDS.toMillis(10);
    /**
     * The default isolation level.
     */
    public static final IsolationLevel DEFAULT_ISOLATION_LEVEL = IsolationLevel.READ_COMMITTED;
    /**
     * By default reconnects on connection lost.
     */
    public static final boolean DEFAULT_RECONNECT_ON_LOST = true;
    /**
     * The default secret location.
     */
    public static final String DEFAULT_SECRET_LOCATION = "secret.key";
    /**
     * By default don't allow column drops.
     */
    public static final boolean DEFAULT_ALLOW_COLUMN_DROP = false;
    /**
     * The default fetch size.
     */
    public static final int DEFAULT_FETCH_SIZE = 1000;
    /**
     * The default maximum amount of time to wait when batches are shutting down. 5 minutes.
     */
    public static final long DEFAULT_MAXIMUM_TIME_BATCH_SHUTDOWN = TimeUnit.MINUTES.toMillis(5);
    /**
     * Indicates if the LOB columns should be compressed. This is only possible on implementations that allow
     * this behavior. The default value is {@code false}.
     */
    public static final boolean DEFAULT_COMPRESS_LOBS = false;
    /**
     * Indicates if LOB data caching should be disabled, to avoid consuming too much memory and/or disk space in the DB
     * server. This is only possible on implementations that support this behavior. The default value is {@code false}.
     */
    public static final boolean DEFAULT_DISABLE_LOB_CACHING = false;

    /**
     * Default duration (in seconds) to wait for the database connection to be established.
     * By default, there is no timeout at establishing the connection, so pdb will wait indefinitely for the database.
     */
    public static final int DEFAULT_LOGIN_TIMEOUT = NO_TIMEOUT;

    /**
     * The default socket connection timeout (in seconds).
     * By default, there is no timeout at the socket level, so pdb will wait indefinitely for the database to respond the queries.
     */
    public static final int DEFAULT_SOCKET_TIMEOUT = NO_TIMEOUT;

    /**
     * The default select query timeout (in seconds).
     * By default, there is no query timeout, so pdb will wait indefinitely for the database to respond to queries.
     */
    public static final int DEFAULT_SELECT_QUERY_TIMEOUT = NO_TIMEOUT;

    /**
     * The SQL standard State code for "transaction failure".
     * This may be caused by serialization failures in concurrent transactions in the DB server and possibly deadlocks;
     * this code indicates that the client app may retry the transaction.
     */
    public static final String SQL_STATE_TRANSACTION_FAILURE = "40001";

    /**
     * The SQL standard State code for "unique constraint violation".
     * A violation of the constraint imposed by a unique index or a unique constraint occurred.
     */
    public static final String SQL_STATE_UNIQUE_CONSTRAINT_VIOLATION = "23505";

    /**
     * A set of PDB Exceptions that can be considered retryable (when these are thrown, it is possible that a client
     * application will be successfull if it performs again the same actions that resulted in the exception).
     * @since 2.5.1
     */
    public static final Set<Class<?extends Throwable>> RETRYABLE_EXCEPTIONS = ImmutableSet.of(
            DatabaseEngineRetryableException.class,
            DatabaseEngineRetryableRuntimeException.class,
            DatabaseEngineTimeoutException.class
    );

    /**
     * The default timeout when checking if a connection is down.
     * @since 2.7.2
     */
    public static final int DEFAULT_CHECK_CONNECTION_TIMEOUT = 60;
}
