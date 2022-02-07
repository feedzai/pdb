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
package com.feedzai.commons.sql.abstraction.engine.impl;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;

/**
 * H2 specific database implementation.
 *
 * @author Carlos Tosin (carlos.tosin@feedzai.com)
 * @since 2.9.0
 */
public class H2EngineV2 extends H2Engine {

    /**
     * Creates a new H2 connection.
     *
     * @param properties The properties for the database connection.
     * @throws DatabaseEngineException When the connection fails.
     */
    public H2EngineV2(final PdbProperties properties) throws DatabaseEngineException {
        super(properties);
    }

    @Override
    protected String getFinalJdbcConnection(final String jdbc) {
        String modifiedUrl = super.getFinalJdbcConnection(jdbc);

        // Disables the legacy mode, if already defined
        return modifiedUrl.replace(";MODE=LEGACY", "");
    }
}
