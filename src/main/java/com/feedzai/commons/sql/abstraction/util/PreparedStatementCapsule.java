/*
 * The copyright of this file belongs to FeedZai. The file cannot be
 * reproduced in whole or in part, stored in a retrieval system, transmitted
 * in any form, or by any means electronic, mechanical, or otherwise, without
 * the prior permission of the owner. Please refer to the terms of the license
 * agreement.
 *
 * (c) 2013 Feedzai, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.util;

import java.sql.PreparedStatement;

/**
 * Encapsulates a prepared statement, name as its properties.
 * @author rui.vilao
 */
public final class PreparedStatementCapsule {
    /** The query. */
    public final String query;
    /** The prepared statement. */
    public final PreparedStatement ps;
    /** The query timeout. */
    public final int timeout;

    /**
     * Creates a new instance of {@link PreparedStatementCapsule}.
     * @param query The query.
     * @param ps The prepared statement.
     * @param timeout The query timeout.
     */
    public PreparedStatementCapsule(String query, PreparedStatement ps, int timeout) {
        this.query = query;
        this.ps = ps;
        this.timeout = timeout;
    }
}
