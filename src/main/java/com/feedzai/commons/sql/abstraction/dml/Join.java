/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.dml;

import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.impl.MySqlEngine;
import com.feedzai.commons.sql.abstraction.util.StringUtil;

/**
 * Represents a SQL join.
 */
public class Join extends Expression {
    /** The join string. */
    private String join = null;
    /** The Table to join. */
    private Expression joinTable = null;
    /** The Expression to join. */
    private Expression joinExpr = null;

    /**
     * Creates a new instance of {@link Join}.
     * @param join The join string.
     * @param joinTable The join table.
     * @param joinExpr The join expression.
     */
    public Join(final String join, final Expression joinTable, final Expression joinExpr) {
        this.join = StringUtil.escapeSql(join);
        this.joinTable = joinTable;
        this.joinExpr = joinExpr;
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        if (joinTable.isAliased()) {
            return String.format("%s %s %s ON (%s)", join, joinTable.translateDB2(properties), StringUtil.quotize(joinTable.alias), joinExpr.translateDB2(properties));
        } else {
            return String.format("%s %s ON (%s)", join, joinTable.translateDB2(properties), joinExpr.translateDB2(properties));
        }
    }

    @Override
    public String translateOracle(final PdbProperties properties) {
        if (joinTable.isAliased()) {
            return String.format("%s %s %s ON (%s)", join, joinTable.translateOracle(properties), StringUtil.quotize(joinTable.alias), joinExpr.translateOracle(properties));
        } else {
            return String.format("%s %s ON (%s)", join, joinTable.translateOracle(properties), joinExpr.translateOracle(properties));
        }
    }

    @Override
    public String translateMySQL(final PdbProperties properties) {
        if (joinTable.isAliased()) {
            return String.format("%s %s %s ON (%s)", join, joinTable.translateMySQL(properties), StringUtil.quotize(joinTable.alias, MySqlEngine.ESCAPE_CHARACTER), joinExpr.translateMySQL(properties));
        } else {
            return String.format("%s %s ON (%s)", join, joinTable.translateMySQL(properties), joinExpr.translateMySQL(properties));
        }
    }

    @Override
    public String translateSQLServer(final PdbProperties properties) {
        if (joinTable.isAliased()) {
            return String.format("%s %s %s %s ON (%s)", join, joinTable.translateSQLServer(properties), StringUtil.quotize(joinTable.alias), joinTable.isWithNoLock() ? " WITH(NOLOCK)" : "", joinExpr.translateSQLServer(properties));
        } else {
            return String.format("%s %s %s ON (%s)", join, joinTable.translateSQLServer(properties), joinTable.isWithNoLock() ? " WITH(NOLOCK)" : "", joinExpr.translateSQLServer(properties));
        }
    }

    @Override
    public String translatePostgreSQL(final PdbProperties properties) {
        if (joinTable.isAliased()) {
            return String.format("%s %s %s ON (%s)", join, joinTable.translatePostgreSQL(properties), StringUtil.quotize(joinTable.alias), joinExpr.translatePostgreSQL(properties));
        } else {
            return String.format("%s %s ON (%s)", join, joinTable.translatePostgreSQL(properties), joinExpr.translatePostgreSQL(properties));
        }
    }

    @Override
    public String translateH2(PdbProperties properties) {
        if (joinTable.isAliased()) {
            return String.format("%s %s %s ON (%s)", join, joinTable.translateH2(properties), StringUtil.quotize(joinTable.alias), joinExpr.translateH2(properties));
        } else {
            return String.format("%s %s ON (%s)", join, joinTable.translateH2(properties), joinExpr.translateH2(properties));
        }
    }
}
