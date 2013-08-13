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
import com.feedzai.commons.sql.abstraction.util.StringUtil;
import java.util.Arrays;

/**
 * The Coalesce operator.
 */
public class Coalesce extends Expression {
    /** The expression to test. */
    private final Expression exp;
    /** The alternative expressions. */
    private final Expression []alternative;

    /**
     * Creates a new instance of {@link Coalesce}.
     * @param exp The expression to test.
     * @param alternative The alternative expressions.
     */
    public Coalesce(final Expression exp, final Expression... alternative) {
        this.exp = exp;
        this.alternative = alternative;
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        final String []alts = new String[alternative.length];
        int i = 0;
        for (Expression e : alternative) {
            alts[i] = e.translateDB2(properties);
            i++;
        }

        return String.format("COALESCE(%s, " + StringUtil.join(Arrays.asList(alts), ", ") + ")", exp.translateDB2(properties));
    }

    @Override
    public String translateOracle(final PdbProperties properties) {
        final String []alts = new String[alternative.length];
        int i = 0;
        for (Expression e : alternative) {
            alts[i] = e.translateOracle(properties);
            i++;
        }

        return String.format("COALESCE(%s, " + StringUtil.join(Arrays.asList(alts), ", ") + ")", exp.translateOracle(properties));
    }

    @Override
    public String translateMySQL(final PdbProperties properties) {
        final String []alts = new String[alternative.length];
        int i = 0;
        for (Expression e : alternative) {
            alts[i] = e.translateMySQL(properties);
            i++;
        }

        return String.format("COALESCE(%s, " + StringUtil.join(Arrays.asList(alts), ", ") + ")", exp.translateMySQL(properties));
    }

    @Override
    public String translateSQLServer(final PdbProperties properties) {
        final String []alts = new String[alternative.length];
        int i = 0;
        for (Expression e : alternative) {
            alts[i] = e.translateSQLServer(properties);
            i++;
        }

        return String.format("COALESCE(%s, " + StringUtil.join(Arrays.asList(alts), ", ") + ")", exp.translateSQLServer(properties));
    }

    @Override
    public String translatePostgreSQL(final PdbProperties properties) {
        final String []alts = new String[alternative.length];
        int i = 0;
        for (Expression e : alternative) {
            alts[i] = e.translatePostgreSQL(properties);
            i++;
        }

        return String.format("COALESCE(%s, " + StringUtil.join(Arrays.asList(alts), ", ") + ")", exp.translatePostgreSQL(properties));
    }

    @Override
    public String translateH2(PdbProperties properties) {
        final String []alts = new String[alternative.length];
        int i = 0;
        for (Expression e : alternative) {
            alts[i] = e.translateH2(properties);
            i++;
        }

        return String.format("COALESCE(%s, " + StringUtil.join(Arrays.asList(alts), ", ") + ")", exp.translateH2(properties));
    }
}
