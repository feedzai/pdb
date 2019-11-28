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

/**
 * To be thrown when faults happen during regular database operations.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class DatabaseEngineRuntimeException extends RuntimeException {

    /**
     * Property that indicates if Exception is retryable or not.
     * This property must be defined 'true' if the Transaction Retry must be done on client side.
     *
     * @see <a href=https://www.cockroachlabs.com/docs/stable/transactions.html#client-side-intervention>CockroachDB - transactions</a>
     */
    private boolean isRetryable = false;

    /**
     * Constructs a new runtime exception with {@code null} as its
     * detail message.  The cause is not initialized, and may subsequently be
     * initialized by a call to {@link #initCause}.
     */
    public DatabaseEngineRuntimeException() {
        super();
    }

    /**
     * Constructs a new runtime exception with the specified detail message.
     * The cause is not initialized, and may subsequently be initialized by a
     * call to {@link #initCause}.
     *
     * @param message the detail message. The detail message is saved for
     *                later retrieval by the {@link #getMessage()} method.
     */
    public DatabaseEngineRuntimeException(final String message) {
        super(message);
    }

    /**
     * Constructs a new runtime exception with the specified detail message and
     * cause.  <p>Note that the detail message associated with
     * {@code cause} is <i>not</i> automatically incorporated in
     * this runtime exception's detail message.
     *
     * @param message the detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method).
     * @param cause   the cause (which is saved for later retrieval by the
     *                {@link #getCause()} method).  (A <tt>null</tt> value is
     *                permitted, and indicates that the cause is nonexistent or
     *                unknown.)
     */
    public DatabaseEngineRuntimeException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new runtime exception with the specified cause and a
     * detail message of <tt>(cause==null ? null : cause.toString())</tt>
     * (which typically contains the class and detail message of
     * <tt>cause</tt>).  This constructor is useful for runtime exceptions
     * that are little more than wrappers for other throwables.
     *
     * @param cause the cause (which is saved for later retrieval by the
     *              {@link #getCause()} method).  (A <tt>null</tt> value is
     *              permitted, and indicates that the cause is nonexistent or
     *              unknown.)
     */
    public DatabaseEngineRuntimeException(final Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new runtime exception with the specified detail
     * message, cause, suppression enabled or disabled, and writable
     * stack trace enabled or disabled.
     *
     * @param message            the detail message.
     * @param cause              the cause.  (A {@code null} value is permitted,
     *                           and indicates that the cause is nonexistent or unknown.)
     * @param enableSuppression  whether or not suppression is enabled
     *                           or disabled
     * @param writableStackTrace whether or not the stack trace should
     *                           be writable
     */
    protected DatabaseEngineRuntimeException(final String message,
                                             final Throwable cause,
                                             final boolean enableSuppression,
                                             final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    /**
     * Constructs a new runtime exception with the specified detail message,
     * cause and a specified isRetryable state.  <p>Note that the detail message associated with
     * {@code cause} is <i>not</i> automatically incorporated in
     * this runtime exception's detail message.
     *
     * @param message     The detail message (which is saved for later retrieval
     *                    by the {@link #getMessage()} method).
     * @param cause       The cause (which is saved for later retrieval by the
     *                    {@link #getCause()} method).  (A <tt>null</tt> value is
     *                    permitted, and indicates that the cause is nonexistent or
     *                    unknown.)
     * @param isRetryable The property must be true if generated Exception must be retried.
     */
    public DatabaseEngineRuntimeException(final String message, final Throwable cause, final boolean isRetryable) {
        super(message, cause);
        this.isRetryable = isRetryable;
    }

    /**
     * Gets whether the error that caused this exception is retryable or not.
     *
     * In particular, if a transaction commit resulted in this exception, the client can attempt to retry it.
     *
     * @return whether this exception is retryable or not.
     */
    public boolean isRetryable() {
        return isRetryable;
    }
}
