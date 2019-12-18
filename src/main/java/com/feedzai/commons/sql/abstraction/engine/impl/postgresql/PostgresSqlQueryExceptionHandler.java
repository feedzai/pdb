/*
 * Copyright 2019 Feedzai
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

package com.feedzai.commons.sql.abstraction.engine.impl.postgresql;

import com.feedzai.commons.sql.abstraction.engine.handler.QueryExceptionHandler;

import java.sql.SQLException;

/**
 * A specific implementation of {@link QueryExceptionHandler} for PostgreSQL engine.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 * @since 2.5.1
 */
public class PostgresSqlQueryExceptionHandler extends QueryExceptionHandler {
    /**
     * The SQL State code PostgreSQL uses for "transaction failure due to deadlocks".
     * This may be caused by serialization failure due to a deadlock detected in the DB server in concurrent; this code
     * indicates that the client app may retry the transaction.
     */
    private static final String DEADLOCK_SQL_STATE = "40P01";

    /**
     * The SQL State that indicates a timeout occurred.
     */
    private static final String TIMEOUT_SQL_STATE = "57014";

    @Override
    public boolean isTimeoutException(final SQLException exception) {
        return TIMEOUT_SQL_STATE.equals(exception.getSQLState()) || super.isTimeoutException(exception);
    }

    @Override
    public boolean isRetryableException(final SQLException exception) {
        return DEADLOCK_SQL_STATE.equals(exception.getSQLState()) || super.isRetryableException(exception);
    }
}
