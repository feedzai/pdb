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
package com.feedzai.commons.sql.abstraction.engine.impl.sqlserver;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineSchemaTest;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class SqlServerEngineSchemaTest extends AbstractEngineSchemaTest {


    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("sqlserver");
    }

    @Override
    @Test
    @Ignore("Microsoft Sql Server doesn't support setting schema per session")
    public void udfTimesTwoTest() {
    }

    @Override
    @Test
    @Ignore("Microsoft Sql Server doesn't respect fetch size unless we use a server cursor, but that seems unnecessary" +
            " because by default it uses adaptative buffering and doesn't fetch all results into memory." +
            "See https://docs.microsoft.com/en-us/sql/connect/jdbc/using-adaptive-buffering")
    public void testFetchSize() {
    }

    @Override
    @Test
    @Ignore("Microsoft Sql Server doesn't support setting schema per session")
    public void testCreateSameEntityDifferentSchemas() {
    }

    @Override
    protected void defineUDFGetOne(final DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate(
                "CREATE OR ALTER FUNCTION GetOne() " +
                        " RETURNS INTEGER AS" +
                        " BEGIN" +
                        "    RETURN(1)" +
                        " END"
        );
    }

    @Override
    protected void defineUDFTimesTwo(final DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate(
                "CREATE OR ALTER FUNCTION " + getTestSchema() + ".TimesTwo(@number INTEGER) " +
                        " RETURNS INTEGER AS" +
                        " BEGIN" +
                        "    RETURN(@number * 2) " +
                        " END"
        );
    }

    @Override
    protected void createSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException {
        engine.executeUpdate("CREATE SCHEMA " + schema);
    }

    @Override
    protected void dropSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException {
        engine.executeUpdate("DROP SCHEMA IF EXISTS " + schema);
    }
}
