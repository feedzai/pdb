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
package com.feedzai.commons.sql.abstraction.engine.impl.cockroach;

import com.feedzai.commons.sql.abstraction.engine.impl.postgresql.PostgreSqlEngineSchemaTest;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * Schema related tests for CockroachDB.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
@RunWith(Parameterized.class)
public class CockroachDBEngineSchemaTest extends PostgreSqlEngineSchemaTest {

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations("cockroach");
    }

    @Override
    @Test
    @Ignore("CockroachDB doesn't support stored procedures; see https://github.com/cockroachdb/cockroach/issues/17511")
    public void udfGetOneTest() {
    }

    @Override
    @Test
    @Ignore("CockroachDB doesn't support stored procedures; see https://github.com/cockroachdb/cockroach/issues/17511")
    public void udfTimesTwoTest() {
    }
}
