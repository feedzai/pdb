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

package com.feedzai.commons.sql.abstraction.engine.impl.oracle;

import com.feedzai.commons.sql.abstraction.engine.handler.QueryExceptionHandler;
import com.google.common.collect.ImmutableSet;

import java.sql.SQLException;
import java.util.Set;

/**
 * A specific implementation of {@link QueryExceptionHandler} for Oracle engine.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 * @since 2.5.1
 */
public class OracleQueryExceptionHandler extends QueryExceptionHandler {

    /**
     * Oracle uses these error codes when deadlocks occur.
     *
     * Both may be caused by serialization failure due to a deadlock detected in the DB server in concurrent; these
     * codes indicate that the client app may retry the transaction.
     *
     * - ORA-00060: deadlock detected while waiting
     * - ORA-08177: can't serialize access for this transaction
     */
    private static final Set<Integer> DEADLOCK_ERROR_CODES = ImmutableSet.of(60, 8177);

    /**
     * Oracle uses this error code to indicate a unique constraint violation.
     *
     * - ORA-00001: unique constraint (string.string) violated
     */
    private static final int UNIQUE_CONSTRAINT_VIOLATION_ERROR_CODE = 1;

    @Override
    public boolean isRetryableException(final SQLException exception) {
        return DEADLOCK_ERROR_CODES.contains(exception.getErrorCode()) || super.isRetryableException(exception);
    }

    @Override
    public boolean isUniqueConstraintViolationException(final SQLException exception) {
        return UNIQUE_CONSTRAINT_VIOLATION_ERROR_CODE == exception.getErrorCode()
                || super.isUniqueConstraintViolationException(exception);
    }
}
