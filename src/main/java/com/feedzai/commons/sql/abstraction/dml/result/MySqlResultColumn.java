/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.dml.result;

/**
 * The MySql column result implementation.
 */
public class MySqlResultColumn extends ResultColumn {

    /**
     * Creates a new instance of {@link MySqlResultColumn}.
     * @param name The column name.
     * @param val The value.
     */
    public MySqlResultColumn(final String name, final Object val) {
        super(name, val);
    }
}
