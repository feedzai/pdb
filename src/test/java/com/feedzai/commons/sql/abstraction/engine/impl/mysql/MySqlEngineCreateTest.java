/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine.impl.mysql;

import com.feedzai.commons.sql.abstraction.engine.impl.abs.AbstractEngineCreateTest;
import com.feedzai.commons.sql.abstraction.engine.impl.abs.TestConfiguration;

/**
 *
 * @author rui.vilao
 */
public class MySqlEngineCreateTest extends AbstractEngineCreateTest {
    @Override
    protected String getEngine() {
        return "com.feedzai.commons.sql.abstraction.engine.impl.MySqlEngine";
    }

    protected TestConfiguration.Configuration.Database getDatabaseType() { return TestConfiguration.Configuration.Database.MYSQL; }
}
