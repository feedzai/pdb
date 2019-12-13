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

package com.feedzai.commons.sql.abstraction.engine.impl.h2;

import com.feedzai.commons.sql.abstraction.engine.handler.QueryExceptionHandler;

import java.sql.SQLException;

/**
 * A specific implementation of {@link QueryExceptionHandler} for H2 engine.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 * @since 2.5.1
 */
public class H2QueryExceptionHandler extends QueryExceptionHandler {

    /**
     * H2 uses this error code when another connection locked an object longer than the lock timeout set for this
     * connection, or when a deadlock occurred.
     * This may be caused by serialization failure due to a deadlock detected in the DB server in concurrent; this code
     * indicates that the client app may retry the transaction.
     */
    private static final int LOCK_TIMEOUT_ERROR_CODE = 50200;

    @Override
    public boolean isRetryableException(final SQLException exception) {
        return exception.getErrorCode() == LOCK_TIMEOUT_ERROR_CODE || super.isRetryableException(exception);
    }
}
