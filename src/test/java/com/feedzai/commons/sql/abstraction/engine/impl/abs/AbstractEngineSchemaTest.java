/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
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
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;
import static org.junit.Assert.*;

public abstract class AbstractEngineSchemaTest extends AbstractEngineTest {

    protected DatabaseEngine engine;
    protected Properties properties;

    protected String getPolicy() { return "drop-create"; }

    @Before
    public void init() throws Exception {
        properties = new Properties() {
            {
                setProperty(JDBC, getConnection());
                setProperty(USERNAME, getUsername());
                setProperty(PASSWORD, getPassword());
                setProperty(ENGINE, getEngine());
                setProperty(SCHEMA_POLICY, getPolicy());
                setProperty(SCHEMA, getDefaultSchema());
            }
        };
    }

    //
    // these tests use the default schema
    //
    
    @Test
    public void udfGetOneTest() throws DatabaseEngineException, DatabaseFactoryException {
        // engine using the default schema
        engine = DatabaseFactory.getConnection(properties);

        defineUDFGetOne(engine);
        
        List<Map<String, ResultColumn>> query = engine.query(select(udf("GetOne").alias("ONE")));
        //List<Map<String, ResultColumn>> query = engine.query(select(column("ONE")).from(udf("GETONE")));

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
        //List<Map<String, ResultColumn>> query = engine.query(select(column("TIMESTWO")).from(udf("TIMESTWO", k(10))));

        assertEquals("result ok?", 20, (int) query.get(0).get("TIMESTWO").toInt());
    }
    
    protected void defineUDFGetOne(DatabaseEngine engine) throws DatabaseEngineException {};
    protected void defineUDFTimesTwo(DatabaseEngine engine) throws DatabaseEngineException {};
}
