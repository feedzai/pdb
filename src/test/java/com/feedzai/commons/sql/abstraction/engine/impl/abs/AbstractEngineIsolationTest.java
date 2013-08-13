/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine.impl.abs;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import org.junit.Before;
import org.junit.Test;
import java.util.Properties;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;

/**
 *
 * @author rui.vilao
 */
public abstract class AbstractEngineIsolationTest extends AbstractEngineTest {
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

    @Test
    public void readCommittedTest() throws DatabaseEngineException, InterruptedException, DatabaseFactoryException {
        properties.setProperty(ISOLATION_LEVEL, "read_committed");

        DatabaseEngine engine = DatabaseFactory.getConnection(properties);
    }

    @Test
    public void readUncommittedTest() throws DatabaseEngineException, InterruptedException, DatabaseFactoryException {
        this.properties.setProperty(ISOLATION_LEVEL, "read_uncommitted");

        DatabaseEngine engine = DatabaseFactory.getConnection(properties);
    }

    @Test
    public void repeatableReadTest() throws DatabaseEngineException, InterruptedException, DatabaseFactoryException {
        this.properties.setProperty(ISOLATION_LEVEL, "repeatable_read");

        DatabaseEngine engine = DatabaseFactory.getConnection(properties);
    }

    @Test
    public void serializableTest() throws DatabaseEngineException, InterruptedException, DatabaseFactoryException {
        this.properties.setProperty(ISOLATION_LEVEL, "serializable");

        DatabaseEngine engine = DatabaseFactory.getConnection(properties);
    }
}
