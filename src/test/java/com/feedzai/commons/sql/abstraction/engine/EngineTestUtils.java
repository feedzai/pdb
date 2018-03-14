/*
 * Copyright 2018 Feedzai
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

import com.feedzai.commons.sql.abstraction.ddl.DbEntity;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.BOOLEAN;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.DOUBLE;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.LONG;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.STRING;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;

/**
 * Utils for using in {@link DatabaseEngine} tests.
 *
 * @author David Fialho (david.fialho@feedzai.com)
 * @since 2.1.13
 */
public final class EngineTestUtils {

    /**
     * Disable the default constructor.
     */
    private EngineTestUtils() {}

    /**
     * Builds a new {@link DbEntity} with the specified name.
     *
     * @param name The name for the built entity.
     * @return The built entity.
     * @since 2.1.13
     */
    public static DbEntity buildEntity(final String name) {
        return dbEntity()
                .name(name)
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .pkFields("COL1")
                .build();
    }

}
