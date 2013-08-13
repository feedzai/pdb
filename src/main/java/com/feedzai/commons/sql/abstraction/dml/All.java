/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.dml;

/**
 * Translates into * or environment.*.
 */
public class All extends Name {

    /**
     * Creates a new instance of {@link All}.
     */
    public All() {
        super("*");
    }

    /**
     * Creates a new instance of {@link All}.
     * @param tableName The environment.
     */
    public All(final String tableName) {
        super(tableName, "*");
    }
}
