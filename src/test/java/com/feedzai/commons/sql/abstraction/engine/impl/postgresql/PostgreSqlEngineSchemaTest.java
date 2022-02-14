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
package com.feedzai.commons.sql.abstraction.engine.impl.postgresql;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineSchemaTest;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineSchemaTest.Ieee754Support.SUPPORTED;

/**
 * @author Rafael Marmelo (rafael.marmelo@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class PostgreSqlEngineSchemaTest extends AbstractEngineSchemaTest {

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("postgresql");
    }

    @Override
    protected Ieee754Support getIeee754Support() {
        return SUPPORTED;
    }

    @Override
    protected void defineUDFGetOne(final DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate(
                "CREATE OR REPLACE FUNCTION GetOne()" +
                        " RETURNS INTEGER" +
                        " AS 'SELECT 1;'" +
                        " LANGUAGE SQL;"
        );
    }

    @Override
    protected void defineUDFTimesTwo(final DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate(
                "CREATE OR REPLACE FUNCTION " + getTestSchema() + ".TimesTwo(INTEGER) " +
                        " RETURNS INTEGER" +
                        " AS 'SELECT $1 * 2;'" +
                        " LANGUAGE SQL;"
        );
    }

    @Override
    protected void createSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException {
        engine.executeUpdate("CREATE SCHEMA \"" + schema + "\"");

    }

    @Override
    protected void dropSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException {
        engine.executeUpdate("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
    }
}
