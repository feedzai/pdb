/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine.impl.oracle;

import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineGeneralTest;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.TestConfiguration;
import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.*;

public class OracleEngineGeneralTest extends AbstractEngineGeneralTest {
    @Override
    protected String getEngine() {
        return "com.feedzai.commons.sql.abstraction.engine.impl.OracleEngine";
    }

    public void insertWithTwoSequencesTest() throws DatabaseEngineException {
        try {
            DbEntity entity = new DbEntity().setName("TEST").addColumn("COL1", INT, true).addColumn("COL2", BOOLEAN).addColumn("COL3", DOUBLE).addColumn("COL4", LONG, true).addColumn("COL5", STRING);

            engine.addEntity(entity);
        } catch (DatabaseEngineException e) {

            throw e;
        }
    }

    protected TestConfiguration.Configuration.Database getDatabaseType() { return TestConfiguration.Configuration.Database.ORACLE; }
}
