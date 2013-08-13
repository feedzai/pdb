/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine;

import com.feedzai.commons.sql.abstraction.engine.impl.*;
import org.junit.Assert;
import org.junit.Test;
import java.util.Properties;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;

public class DatabaseFactoryTest {
    
    @Test
    public void getOracleEngineTest() throws DatabaseFactoryException {
        Properties properties = new Properties() {
            {
                setProperty(JDBC, "jdbc:oracle:thin:@test-database.zai:1521:orcl");
                setProperty(ENGINE, "com.feedzai.commons.sql.abstraction.engine.impl.OracleEngine");
                setProperty(USERNAME, "pulse");
                setProperty(PASSWORD, "pulse");
                setProperty(SCHEMA_POLICY, "create");
            }
        };

        DatabaseEngine de = DatabaseFactory.getConnection(properties);

        Assert.assertEquals("class ok?", OracleEngine.class, de.getClass());
    }

    @Test
    public void getMySqlEngineTest() throws DatabaseFactoryException {
        Properties properties = new Properties() {
            {
                setProperty(JDBC, "jdbc:mysql://test-database/pulse-tests");
                setProperty(ENGINE, "com.feedzai.commons.sql.abstraction.engine.impl.MySqlEngine");
                setProperty(USERNAME, "pulse");
                setProperty(PASSWORD, "pulse");
                setProperty(SCHEMA_POLICY, "create");
            }
        };

        DatabaseEngine de = DatabaseFactory.getConnection(properties);

        Assert.assertEquals("class ok?", MySqlEngine.class, de.getClass());
    }

    @Test
    public void getPostgreSqlEngineTest() throws DatabaseFactoryException {
        Properties properties = new Properties() {
            {
                setProperty(JDBC, "jdbc:postgresql://test-database/pulse-tests");
                setProperty(ENGINE, "com.feedzai.commons.sql.abstraction.engine.impl.PostgreSqlEngine");
                setProperty(USERNAME, "pulse");
                setProperty(PASSWORD, "pulse");
                setProperty(SCHEMA_POLICY, "create");
            }
        };

        DatabaseEngine de = DatabaseFactory.getConnection(properties);

        Assert.assertEquals("class ok?", PostgreSqlEngine.class, de.getClass());
    }

    @Test
    public void getSqlServerEngineTest() throws DatabaseFactoryException {
        Properties properties = new Properties() {
            {
                setProperty(JDBC, "jdbc:sqlserver://test-database;database=pulse-tests");
                setProperty(ENGINE, "com.feedzai.commons.sql.abstraction.engine.impl.SqlServerEngine");
                setProperty(USERNAME, "pulse");
                setProperty(PASSWORD, "pulse");
                setProperty(SCHEMA_POLICY, "create");
            }
        };

        DatabaseEngine de = DatabaseFactory.getConnection(properties);

        Assert.assertEquals("class ok?", SqlServerEngine.class, de.getClass());
    }
}
