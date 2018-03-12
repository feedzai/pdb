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

import com.feedzai.commons.sql.abstraction.engine.configuration.IsolationLevel;

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
     * By default allow column drops.
     */
    public static final boolean DEFAULT_ALLOW_COLUMN_DROP = true;
    /**
     * The default fetch size.
     */
    public static final int DEFAULT_FETCH_SIZE = 1000;
    /**
     * The default maximum amount of time to wait when batches are shutting down. 5 minutes.
     */
    public static final long DEFAULT_MAXIMUM_TIME_BATCH_SHUTDOWN = TimeUnit.MINUTES.toMillis(5);
    /**
     * Indicates that the LOBS columns should be compressed. This is only possible on implementations that allow
     * this behavior.
     */
    public static final boolean DEFAULT_COMPRESS_LOBS = true;
}
