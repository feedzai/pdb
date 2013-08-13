/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.ddl;

/**
 * The column constraints.
 */
public enum DbColumnConstraint {
    /** The unique constraint. */
    UNIQUE {
        @Override
        public String translate() {
            return "UNIQUE";
        }
    },
    /** The not null constraint. */
    NOT_NULL {
        @Override
        public String translate() {
            return "NOT NULL";
        }
    };

    /**
     * The default translation.
     * @return The default translation.
     */
    public abstract String translate();
}
