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

/**
 * The BETWEEN operator.
 */
public class Between extends Expression {

    /** The column. */
    private final Expression column;
    /** The AND expression. */
    private final Expression and;
    /** Negates the expression. */
    private boolean not = false;

    /**
     * Creates a new instance of {@link Between}.
     * @param column The column.
     * @param exp The AND expression.
     */
    public Between(final Expression column, final Expression exp) {
        this.column = column;
        this.and = exp;
    }

    /**
     * Negates the expression.
     * @return This expression.
     */
    public Between not() {
        not = true;

        return this;
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        String modifier = "BETWEEN";
        if (not) {
            modifier = "NOT " + modifier;
        }

        String result = String.format("%s %s %s", column.translateDB2(properties), modifier, and.translateDB2(properties));

        if (isEnclosed()) {
            result = "(" + result + ")";
        }

        return result;
    }

    @Override
    public String translateOracle(final PdbProperties properties) {
        String modifier = "BETWEEN";
        if (not) {
            modifier = "NOT " + modifier;
        }

        String result = String.format("%s %s %s", column.translateOracle(properties), modifier, and.translateOracle(properties));

        if (isEnclosed()) {
            result = "(" + result + ")";
        }

        return result;
    }

    @Override
    public String translateMySQL(final PdbProperties properties) {
        String modifier = "BETWEEN";
        if (not) {
            modifier = "NOT " + modifier;
        }
        
        String result = String.format("%s %s %s", column.translateMySQL(properties), modifier, and.translateMySQL(properties));

        if (isEnclosed()) {
            result = "(" + result + ")";
        }

        return result;
    }

    @Override
    public String translateSQLServer(final PdbProperties properties) {
        String modifier = "BETWEEN";
        if (not) {
            modifier = "NOT " + modifier;
        }
        
        String result = String.format("%s %s %s", column.translateSQLServer(properties), modifier, and.translateSQLServer(properties));

        if (isEnclosed()) {
            result = "(" + result + ")";
        }

        return result;
    }

    @Override
    public String translatePostgreSQL(final PdbProperties properties) {
        String modifier = "BETWEEN";
        if (not) {
            modifier = "NOT " + modifier;
        }
        
        String result = String.format("%s %s %s", column.translatePostgreSQL(properties), modifier, and.translatePostgreSQL(properties));

        if (isEnclosed()) {
            result = "(" + result + ")";
        }

        return result;
    }

    @Override
    public String translateH2(PdbProperties properties) {
        String modifier = "BETWEEN";
        if (not) {
            modifier = "NOT " + modifier;
        }
        
        String result = String.format("%s %s %s", column.translateH2(properties), modifier, and.translateH2(properties));

        if (isEnclosed()) {
            result = "(" + result + ")";
        }

        return result;
    }

}
