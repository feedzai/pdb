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
package com.feedzai.commons.sql.abstraction.engine.impl.db2;


import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineSchemaTest;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author Joao Silva (joao.silva@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class DB2EngineSchemaTest extends AbstractEngineSchemaTest {

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("db2");
    }

    @Override
    protected void defineUDFGetOne(final DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate(
                "CREATE OR REPLACE FUNCTION GETONE()" +
                        " RETURNS INTEGER" +
                        " NO EXTERNAL ACTION" +
                        " F1: BEGIN ATOMIC" +
                        "    RETURN 1;" +
                        " END"
        );
    }

    @Override
    protected void defineUDFTimesTwo(final DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate(
            "CREATE OR REPLACE FUNCTION \"" + getTestSchema() + "\".TimesTwo(N VARCHAR(128))" +
                    " RETURNS INTEGER" +
                    " NO EXTERNAL ACTION" +
                    " F1: BEGIN ATOMIC" +
                    "    RETURN N * 2;" +
                    " END"
        );
    }

    @Override
    protected void createSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException {
        engine.executeUpdate("CREATE SCHEMA \"" + schema + "\"");
    }

    @Override
    protected void dropSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException {
        final List<Map<String, ResultColumn>> tableResults = engine.query(
            "SELECT tabname FROM syscat.tables WHERE tabschema='" + schema + "'"
        );

        for (final Map<String, ResultColumn> table : tableResults) {
            engine.executeUpdate("DROP TABLE \"" + schema + "\".\"" + table.get("TABNAME") + "\"");
        }

        final List<Map<String, ResultColumn>> funcResults = engine.query(
            "SELECT funcname FROM syscat.functions WHERE funcschema='" + schema + "'"
        );

        for (final Map<String, ResultColumn> result : funcResults) {
            engine.executeUpdate("DROP FUNCTION \"" + schema + "\".\"" + result.get("FUNCNAME") + "\"");
        }

        engine.executeUpdate(
                "BEGIN\n" +
                "   DECLARE CONTINUE HANDLER FOR SQLSTATE '42704' BEGIN END;\n" +
                "   EXECUTE IMMEDIATE 'DROP SCHEMA \"" + schema + "\" RESTRICT';\n" +
                "END"
        );
    }
}
