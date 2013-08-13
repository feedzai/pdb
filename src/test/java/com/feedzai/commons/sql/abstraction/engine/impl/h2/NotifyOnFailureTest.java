/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine.impl.h2;

import com.feedzai.commons.sql.abstraction.batch.BatchEntry;
import com.feedzai.commons.sql.abstraction.batch.DefaultBatch;
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.engine.*;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.*;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.*;

import mockit.Mock;
import mockit.MockUp;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.Properties;

/**
 * @author Rui Vilao (rui.vilao@feedzai.com)
 */
@Ignore
public class NotifyOnFailureTest {
    private DatabaseEngine engine;
    protected Properties properties;

    private BatchEntry[] failureResults = null;

    @Before
    public void init() throws DatabaseEngineException, DatabaseFactoryException {
        properties = new Properties() {

            {
                setProperty(ENGINE, "com.feedzai.commons.sql.abstraction.engine.impl.H2Engine");
                setProperty(USERNAME, "pulse");
                setProperty(PASSWORD, "pulse");
                setProperty(JDBC, "jdbc:h2:target/pulse-tests");
                setProperty(SCHEMA_POLICY, "drop-create");
            }
        };

        engine = DatabaseFactory.getConnection(properties);
    }

    @After
    public void cleanup() {
        engine.close();
    }

    @Test
    public void testBatchFailureTest() throws DatabaseEngineException {
        mockClasses();
        DbEntity entity = new DbEntity()
                .setName("TEST")
                .addColumn("COL1", INT)
                .addColumn("COL2", BOOLEAN)
                .addColumn("COL3", DOUBLE)
                .addColumn("COL4", LONG)
                .addColumn("COL5", STRING);

        engine.addEntity(entity);

        DefaultBatch batch = DefaultBatch.create(engine, 5, 10000L);

        for (int i = 0; i < 5; i++) {
            batch.add("TEST", new EntityEntry().set("COL1", i));
        }

        assertEquals("", 5, failureResults.length);
        for (int i = 0; i < 5; i++) {
            assertEquals("table name ok?", "TEST", failureResults[i].getTableName());
            assertEquals("COL1 value ok?", new Integer(i), failureResults[i].getEntityEntry().get("COL1"));
        }

    }

    private void mockClasses() {
        new MockUp<DefaultBatch>() {

            @Mock
            public void onFlushFailure(BatchEntry[] entries) {
                failureResults = entries;
            }

            @Mock
            public void run() {
                // Ignore batch flushing on timeout.
            }
        };

        new MockUp<DatabaseEngineImpl>() {
            @Mock
            public void beginTransaction() throws DatabaseEngineRuntimeException {
                throw new DatabaseEngineRuntimeException("", new RetryLimitExceededException(""));
            }
        };
    }
}
