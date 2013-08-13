/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine.impl.abs;

import org.codehaus.jackson.map.ObjectMapper;
import java.io.File;

/**
 *  @author Miguel Miranda (miguel.miranda@feedzai.com)
 */
abstract public class AbstractEngineTest {
    static ObjectMapper mapper = new ObjectMapper();

    protected TestConfiguration testConfig = null;
    protected TestConfiguration.Configuration configuration = null;

    public AbstractEngineTest() {
        try {
            TestConfiguration testConfig = mapper.readValue(new File("configuration.json"), TestConfiguration.class);

            for(TestConfiguration.Configuration conf : testConfig.getConfigurations()) {
                if(conf.getDatabase() == getDatabaseType()) {
                    configuration = conf;
                    break;
                }
            }
        } catch (Exception ex) {
            //failed to load configuration file
        }
    }

    abstract protected String getEngine();

    protected String getConnection() { return configuration.getConnection(); }

    protected String getUsername() { return configuration.getUsername(); }

    protected String getPassword() { return configuration.getPassword(); }

    abstract protected String getPolicy();

    protected String getDefaultSchema() { return ""; }

    protected String getSchema() { return ""; }

    abstract protected TestConfiguration.Configuration.Database getDatabaseType();
}
