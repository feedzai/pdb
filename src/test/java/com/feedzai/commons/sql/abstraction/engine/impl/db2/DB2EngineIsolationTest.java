/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine.impl.db2;

import com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineIsolationTest;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.TestConfiguration;

/**
 *
 * @author joao.silva
 */
public class DB2EngineIsolationTest extends AbstractEngineIsolationTest {
    @Override
    protected String getEngine() {
        return "com.feedzai.commons.sql.abstraction.engine.impl.DB2Engine";
    }

    protected TestConfiguration.Configuration.Database getDatabaseType() { return TestConfiguration.Configuration.Database.DB2; }
}
