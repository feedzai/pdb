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
package com.feedzai.commons.sql.abstraction.engine.impl.mysql;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineSchemaTest;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * @author Rafael Marmelo (rafael.marmelo@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class MySqlEngineSchemaTest extends AbstractEngineSchemaTest {

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("mysql");
    }

    @Override
    protected String getSchema() {
        return "myschema";
    }

    @Override
    protected void defineUDFGetOne(DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate("DROP FUNCTION IF EXISTS GetOne");
        engine.executeUpdate(
            "CREATE FUNCTION GetOne()\n" +
                "   RETURNS INTEGER\n" +
                "   RETURN 1;"
        );
    }

    @Override
    protected void defineUDFTimesTwo(DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate("DROP SCHEMA IF EXISTS myschema");
        engine.executeUpdate("CREATE SCHEMA myschema");

        engine.executeUpdate("DROP FUNCTION IF EXISTS myschema.TimesTwo");
        engine.executeUpdate(
            "CREATE FUNCTION myschema.TimesTwo(number INT)\n" +
                "   RETURNS INTEGER\n" +
                "   RETURN number * 2;"
        );
    }
}
