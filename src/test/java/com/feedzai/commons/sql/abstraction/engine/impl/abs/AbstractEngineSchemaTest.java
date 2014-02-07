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
package com.feedzai.commons.sql.abstraction.engine.impl.abs;

import com.feedzai.commons.sql.abstraction.dml.result.ResultColumn;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA;
import static org.junit.Assert.assertEquals;


/**
 * @author Rafael Marmelo (rafael.marmelo@feedzai.com)
 * @since 2.0.0
 */
public abstract class AbstractEngineSchemaTest {

    protected DatabaseEngine engine;
    protected Properties properties;

    protected abstract String getSchema();

    protected abstract String getDefaultSchema();

    @Before
    public abstract void init() throws Exception;


    //
    // these tests use the default schema
    //

    @Test
    public void udfGetOneTest() throws DatabaseEngineException, DatabaseFactoryException {
        // engine using the default schema
        engine = DatabaseFactory.getConnection(properties);
        defineUDFGetOne(engine);

        List<Map<String, ResultColumn>> query = engine.query(select(udf("GetOne").alias("ONE")));
        assertEquals("result ok?", 1, (int) query.get(0).get("ONE").toInt());
    }


    //
    // these tests use a given schema
    //

    @Test
    public void udfTimesTwoTest() throws DatabaseEngineException, DatabaseFactoryException {
        // engine using the default schema
        this.properties.setProperty(SCHEMA, getSchema());
        engine = DatabaseFactory.getConnection(properties);

        defineUDFTimesTwo(engine);

        List<Map<String, ResultColumn>> query = engine.query(select(udf("TimesTwo", k(10)).alias("TIMESTWO")));
        assertEquals("result ok?", 20, (int) query.get(0).get("TIMESTWO").toInt());
    }

    protected void defineUDFGetOne(DatabaseEngine engine) throws DatabaseEngineException {
    }

    protected void defineUDFTimesTwo(DatabaseEngine engine) throws DatabaseEngineException {
    }
}
