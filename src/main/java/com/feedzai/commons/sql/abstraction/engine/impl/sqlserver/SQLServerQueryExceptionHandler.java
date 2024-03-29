/*
 * Copyright 2022 Feedzai
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

package com.feedzai.commons.sql.abstraction.engine.impl.sqlserver;

import java.sql.SQLException;

import com.feedzai.commons.sql.abstraction.engine.handler.QueryExceptionHandler;

/**
 * A specific implementation of {@link QueryExceptionHandler} for SQLServer engine.
 *
 * @author Ines Fernandes (ines.fernandes@feedzai.com)
 */
public class SQLServerQueryExceptionHandler extends QueryExceptionHandler {

    /**
     * The SQLServer State code that indicates a unique constraint violation.
     */
    private static final int UNIQUE_CONSTRAINT_VIOLATION_ERROR_CODE = 2627;

    @Override
    public boolean isUniqueConstraintViolationException(final SQLException exception) {
        return UNIQUE_CONSTRAINT_VIOLATION_ERROR_CODE == exception.getErrorCode()
                || super.isUniqueConstraintViolationException(exception);
    }
}
