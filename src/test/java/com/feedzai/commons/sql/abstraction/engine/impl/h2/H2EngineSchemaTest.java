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
package com.feedzai.commons.sql.abstraction.engine.impl.h2;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineSchemaTest;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineSchemaTest.Ieee754Support.SUPPORTED_STRINGS;

/**
 * @author Joao Silva (joao.silva@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class H2EngineSchemaTest extends AbstractEngineSchemaTest {

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("h2");
    }

    @Override
    protected Ieee754Support getIeee754Support() {
        return SUPPORTED_STRINGS;
    }

    @Override
    @Test
    @Ignore("H2 respects the fetch size, but takes a *very* long time to timeout on closing the DatabaseEngine/ResultSet")
    public void testFetchSize() {
    }

    @Override
    protected void defineUDFGetOne(final DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate("DROP ALIAS IF EXISTS GetOne");
        engine.executeUpdate("CREATE ALIAS IF NOT EXISTS GetOne FOR \"" + this.getClass().getName() + ".GetOne\"");
    }

    @Override
    protected void defineUDFTimesTwo(final DatabaseEngine engine) throws DatabaseEngineException {
        engine.executeUpdate("DROP ALIAS IF EXISTS \"" + getTestSchema() + "\".TimesTwo");
        engine.executeUpdate(
                "CREATE ALIAS IF NOT EXISTS \"" + getTestSchema() + "\".TimesTwo FOR \"" + this.getClass().getName() + ".TimesTwo\""
        );
    }

    @Override
    protected void createSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException {
        engine.executeUpdate("CREATE SCHEMA IF NOT EXISTS \"" + schema + "\"");
    }

    @Override
    protected void dropSchema(final DatabaseEngine engine, final String schema) throws DatabaseEngineException {
        engine.executeUpdate("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
    }

    /**
     * Checks whether the current connection to H2 is local or to a remote server.
     *
     * This method won't throw exceptions, if there is any problem the connection will be considered local.
     *
     * @return {@code true} if the connection is local, {@code false} otherwise.
     */
    private boolean checkIsLocalH2() {
        try (final DatabaseEngine engine = DatabaseFactory.getConnection(properties)) {
            return "0".equals(engine.getConnection().getClientInfo("numServers"));
        } catch (final Exception ex) {
            return true;
        }
    }

    public static int GetOne() {
        return 1;
    }

    public static int TimesTwo(int value) {
        return value * 2;
    }
}
