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

package com.feedzai.commons.sql.abstraction.engine.impl.mysql;

import com.feedzai.commons.sql.abstraction.engine.handler.QueryExceptionHandler;

import com.mysql.jdbc.exceptions.MySQLTimeoutException;

import java.sql.SQLException;

/**
 * A specific implementation of {@link QueryExceptionHandler} for MySQL engine.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 * @since 2.5.1
 */
public class MySqlQueryExceptionHandler extends QueryExceptionHandler {

    /**
     * The MySQL error code that indicates a unique constraint violation.
     */
    private static final int UNIQUE_CONSTRAINT_VIOLATION_ERROR_CODE = 1062;

    @Override
    public boolean isTimeoutException(final SQLException exception) {
        return exception instanceof MySQLTimeoutException || super.isTimeoutException(exception);
    }

    @Override
    public boolean isUniqueConstraintViolationException(final SQLException exception) {
        return UNIQUE_CONSTRAINT_VIOLATION_ERROR_CODE == exception.getErrorCode()
                || super.isUniqueConstraintViolationException(exception);
    }
}
