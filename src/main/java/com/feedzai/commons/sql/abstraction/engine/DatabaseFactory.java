/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine;

import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.util.StringUtil;
import java.lang.reflect.Constructor;
import java.util.Properties;

/**
 * Factory used to obtain database connections.
 */
public final class DatabaseFactory {

    /**
     * Hides the public constructor.
     */
    private DatabaseFactory() { }

    /**
     * Gets a database connection from the specified properties.
     * @param properties The database properties.
     * @return A reference of the specified database engine.
     * @throws DatabaseFactoryException If the class specified does not exist or its not well implemented.
     */
    public static DatabaseEngine getConnection(Properties properties) throws DatabaseFactoryException {
        DatabaseEngine de = null;

        final String engine = properties.getProperty(PdbProperties.ENGINE);

        if (StringUtil.isBlank(engine)) {
            throw new DatabaseFactoryException("pdb.engine property is mandatory");
        }

        try {
            Class c = Class.forName(engine);
            
            Constructor cons = c.getConstructor(Properties.class);
            de = (DatabaseEngine) cons.newInstance(properties);

            return de;
        } catch (Exception e) {
            throw new DatabaseFactoryException(e);
        }
    }
}
