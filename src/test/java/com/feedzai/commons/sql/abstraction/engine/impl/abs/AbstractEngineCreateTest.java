/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine.impl.abs;

import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.ddl.DbFk;
import com.feedzai.commons.sql.abstraction.engine.*;
import com.feedzai.commons.sql.abstraction.util.StringUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.*;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;
import static org.junit.Assert.*;

/**
 *
 * @author rui.vilao
 */
public abstract class AbstractEngineCreateTest extends AbstractEngineTest {
    protected Properties properties;

    protected String getPolicy() { return "create"; }

    @Before
    public void init() throws DatabaseEngineException {
        this.properties = new Properties() {
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
    
//    @Test
//    public void addTableTwiceInSameConnectionTest() throws DatabaseEngineException, InterruptedException, DatabaseFactoryException {
//        DatabaseEngine engine = DatabaseFactory.getConnection(properties);
//
//        try {
//            engine.executeUpdate("DROP TABLE \"TEST\"");
//        } catch (DatabaseEngineException e) { }
//
//        DbEntity entity = new DbEntity()
//                .setName("TEST")
//                .addColumn("COL1", INT)
//                .addColumn("COL2", BOOLEAN)
//                .addColumn("COL3", DOUBLE)
//                .addColumn("COL4", LONG)
//                .addColumn("COL5", STRING)
//                .setPkFields("COL1")
//                .addIndex("COL3");
//
//        engine.addEntity(entity);
//
//        engine.addEntity(entity);
//
//        engine.close();
//    }

    @Test
    public void addEntityWithSchemaAlreadyCreated1Test() throws DatabaseEngineException, InterruptedException, DatabaseFactoryException {
        DatabaseEngine engine = DatabaseFactory.getConnection(properties);

        try {
            engine.executeUpdate("DROP TABLE "+ StringUtil.quotize("TEST",engine.escapeCharacter()));
        } catch (DatabaseEngineException e) { }

        DbEntity entity = new DbEntity()
                .setName("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING)
                .setPkFields("COL1")
                .addIndex("COL3");

        engine.addEntity(entity);

        engine.close();

        engine = DatabaseFactory.getConnection(properties);

        engine.addEntity(entity);

        engine.close();
    }

    @Test
    public void addEntityWithSchemaAlreadyCreated2Test() throws DatabaseEngineException, InterruptedException, DatabaseFactoryException {
        DatabaseEngine engine = DatabaseFactory.getConnection(properties);

        try {
            ((DatabaseEngineImpl)engine).dropEntity(new DbEntity().setName("TEST2"));
            ((DatabaseEngineImpl)engine).dropEntity(new DbEntity().setName("TEST1"));
        } catch (DatabaseEngineException e) { }

        DbEntity entity1 = new DbEntity()
                .setName("TEST1")
                .addColumn("COL1", INT)
                .setPkFields("COL1");

        engine.addEntity(entity1);

        DbEntity entity2 = new DbEntity()
                .setName("TEST2")
                .addColumn("COL1", INT)
                .addColumn("COL2", INT)
                .setPkFields("COL1")
                .addFk(
                    new DbFk()
                    .addColumn("COL2")
                    .setForeignTable("TEST1")
                    .addForeignColumn("COL1")
                );

        engine.addEntity(entity2);

        engine.close();

        engine = DatabaseFactory.getConnection(properties);

        engine.addEntity(entity1);
        engine.addEntity(entity2);

        engine.close();
    }

    @Test(expected=DuplicateEngineException.class)
    public void duplicationFailsTest() throws DatabaseFactoryException, DuplicateEngineException {
        DatabaseEngine engine = DatabaseFactory.getConnection(properties);

        Properties prop = new Properties() {
            {
                setProperty("pdb.schema_policy", "drop-create");
            }
        };

        try {
            engine.duplicate(prop, false);
            System.out.println("?!");
        } catch (DuplicateEngineException e) {
            assertEquals("", "Duplicate can only be called if pdb.policy is set to 'create' or 'none'", e.getMessage());

            throw e;
        }
    }

    @Test
    public void duplicationGoesOkTest() throws DatabaseFactoryException, DuplicateEngineException {
        DatabaseEngine engine = DatabaseFactory.getConnection(properties);
        engine.duplicate(properties, false);
    }
}
