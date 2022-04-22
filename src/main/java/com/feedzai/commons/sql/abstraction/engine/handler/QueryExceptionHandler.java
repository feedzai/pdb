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

package com.feedzai.commons.sql.abstraction.engine.handler;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineUniqueConstraintViolationException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineTimeoutException;
import com.feedzai.commons.sql.abstraction.exceptions.DatabaseEngineRetryableException;
import com.feedzai.commons.sql.abstraction.util.Constants;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;

/**
 * A handler that can be used to disambiguate the meaning of an {@link SQLException} thrown when executing a JDBC
 * method.
 *
 * The methods in this class can be used for example to tell whether an exception is retryable, or more in particular
 * if it is a timeout (which can also be considered retryable).
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 * @since 2.5.1
 */
public class QueryExceptionHandler {

    /**
     * Indicates if a given exception is a timeout. Logic for this may be driver-specific, so
     * drivers that support query timeouts may have to override this method.
     *
     * A timeout exception can also be considered retryable.
     *
     * @param exception  The exception to check.
     * @return {@code true} if the exception is a timeout, {@code false} otherwise.
     */
    public boolean isTimeoutException(final SQLException exception) {
        return exception instanceof SQLTimeoutException;
    }

    /**
     * Checks if an Exception occurred due to serialization failures in concurrent transactions and may be retried on
     * the client-side.
     *
     * @param exception  The exception to check.
     * @return {@code true} if the exception is retryable, {@code false} otherwise.
     */
    public boolean isRetryableException(final SQLException exception) {
        return Constants.SQL_STATE_TRANSACTION_FAILURE.equals(exception.getSQLState());
    }

    /**
     * Checks if an Exception occurred due to a unique constraint violation.
     *
     * @param exception  The exception to check.
     * @return {@code true} if the exception is a unique constraint violation, {@code false} otherwise.
     */
    public boolean isUniqueConstraintViolationException(final SQLException exception) {
        if (exception instanceof BatchUpdateException) {
            return isUniqueConstraintViolationException(exception.getNextException());
        } else {
            return Constants.SQL_STATE_UNIQUE_CONSTRAINT_VIOLATION.equals(exception.getSQLState());
        }
    }

    /**
     * Handles the Exception, disambiguating it into a specific PDB Exception and throwing it.
     * <p>
     * If a specific type does not match the info in the provided Exception, throws a {@link DatabaseEngineException}.
     *
     * @param exception The exception to handle.
     * @param message   The message to associate with the thrown exception.
     * @return a {@link DatabaseEngineException} (declared, but only to keep Java type system happy; this method will
     * always throw an exception).
     * @since 2.5.1
     */
    public DatabaseEngineException handleException(final Exception exception,
                                                   final String message) throws DatabaseEngineException {
        if (exception instanceof SQLException) {
            final SQLException sqlException = (SQLException) exception;
            if (isTimeoutException(sqlException)) {
                throw new DatabaseEngineTimeoutException(message + " [timeout]", sqlException);
            }

            if (isRetryableException(sqlException)) {
                throw new DatabaseEngineRetryableException(message + " [retryable]", sqlException);
            }

            if (isUniqueConstraintViolationException(sqlException)) {
                throw new DatabaseEngineUniqueConstraintViolationException(message + " [unique_constraint_violation]", sqlException);
            }
        }

        throw new DatabaseEngineException(message, exception);
    }
}
