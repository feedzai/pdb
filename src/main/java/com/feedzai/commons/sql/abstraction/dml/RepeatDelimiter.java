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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Utility to repeat an expression with a certain delimiter.
 */
public class RepeatDelimiter extends Expression {
    /**
     * The AND delimited.
     */
    public static final String AND = " AND ";
    /**
     * The OR delimited.
     */
    public static final String OR = " OR ";
    /**
     * The + delimited.
     */
    public static final String PLUS = " + ";
    /**
     * The - delimited.
     */
    public static final String MINUS = " - ";
    /**
     * The * delimited.
     */
    public static final String MULT = " * ";
    /**
     * The / delimited.
     */
    public static final String DIV = " / ";
    /**
     * The &gt; delimited.
     */
    public static final String GT = " > ";
    /**
     * The &lt; delimited.
     */
    public static final String LT = " < ";
    /**
     * The &#8805; delimited.
     */
    public static final String GTEQ = " >= ";
    /**
     * The &#8804; delimited.
     */
    public static final String LTEQ = " <= ";
    /**
     * The LIKE delimited.
     */
    public static final String LIKE = " LIKE ";
    /**
     * The EQ delimited.
     */
    public static final String EQ = " = ";
    /**
     * The NEQ delimiter.
     */
    public static final String NEQ = " <> ";
    /**
     * Semicolon delimiter.
     */
    public static final String SEMICOLON = ", ";
    /**
     * The IN delimited.
     */
    public static final String IN = " IN ";
    /**
     * The NOT IN delimited.
     */
    public static final String NOTIN = " NOT IN ";

    /**
     * The delimiter.
     */
    private String delimiter;
    /**
     * The list of expressions.
     */
    private final List<Expression> exps = new ArrayList<Expression>();

    /**
     * Creates a new instance of {@link RepeatDelimiter}.
     *
     * @param delimiter The delimiter to use.
     * @param dbe       The list of expressions.
     */
    public RepeatDelimiter(final String delimiter, final Expression... dbe) {
        this.delimiter = delimiter;
        this.exps.addAll(Arrays.asList(dbe));
    }

    /**
     * Creates a new instance of {@link RepeatDelimiter}.
     *
     * @param delimiter The delimiter to use.
     * @param dbe       The collection of expressions.
     */
    public RepeatDelimiter(final String delimiter, final Collection<? extends Expression> dbe) {
        this.delimiter = delimiter;
        this.exps.addAll(dbe);
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        List<String> all = new ArrayList<String>();
        for (Expression dbe : exps) {
            all.add(dbe.translateDB2(properties));
        }

        if (DIV.equals(delimiter)) {
            /* DB2 operations are type sensitive...must convert to  double first (why IBM??)*/
            if (isEnclosed()) {
                return "(1.0*" + StringUtil.join(all, delimiter) + ")";
            } else {
                return "1.0*"+StringUtil.join(all, delimiter);
            }
        } else {
            if (isEnclosed()) {
                return "(" + StringUtil.join(all, delimiter) + ")";
            } else {
                return StringUtil.join(all, delimiter);
            }
        }
    }

    @Override
    public String translateOracle(final PdbProperties properties) {
        List<String> all = new ArrayList<String>();
        for (Expression dbe : exps) {
            all.add(dbe.translateOracle(properties));
        }

        if (isEnclosed()) {
            return "(" + StringUtil.join(all, delimiter) + ")";
        } else {
            return StringUtil.join(all, delimiter);
        }
    }

    @Override
    public String translateMySQL(final PdbProperties properties) {
        List<String> all = new ArrayList<String>();
        for (Expression dbe : exps) {
            all.add(dbe.translateMySQL(properties));
        }

        if (isEnclosed()) {
            return "(" + StringUtil.join(all, delimiter) + ")";
        } else {
            return StringUtil.join(all, delimiter);
        }
    }

    @Override
    public String translateSQLServer(final PdbProperties properties) {
        List<String> all = new ArrayList<String>();

        if (delimiter.equals(DIV)) {
            all.add(String.format("CONVERT(DOUBLE PRECISION, %s)", exps.get(0).translateSQLServer(properties)));
        } else {
            all.add(exps.get(0).translateSQLServer(properties));

        }

        for (int i = 1; i < exps.size(); i++) {
            all.add(exps.get(i).translateSQLServer(properties));
        }

        if (isEnclosed()) {
            return "(" + StringUtil.join(all, delimiter) + ")";
        } else {
            return StringUtil.join(all, delimiter);
        }
    }

    @Override
    public String translatePostgreSQL(final PdbProperties properties) {
        List<String> all = new ArrayList<String>();

        if (delimiter.equals(DIV)) {
            all.add(String.format("CAST(%s AS DOUBLE PRECISION)", exps.get(0).translatePostgreSQL(properties)));
        } else {
            all.add(exps.get(0).translatePostgreSQL(properties));

        }

        for (int i = 1; i < exps.size(); i++) {
            all.add(exps.get(i).translatePostgreSQL(properties));
        }

        if (isEnclosed()) {
            return "(" + StringUtil.join(all, delimiter) + ")";
        } else {
            return StringUtil.join(all, delimiter);
        }
    }

    @Override
    public String translateH2(PdbProperties properties) {
        List<String> all = new ArrayList<String>();

        if (delimiter.equals(DIV)) {
            all.add(String.format("CAST(%s AS DOUBLE PRECISION)", exps.get(0).translateH2(properties)));
        } else {
            all.add(exps.get(0).translateH2(properties));

        }

        for (int i = 1; i < exps.size(); i++) {
            all.add(exps.get(i).translateH2(properties));
        }

        if (isEnclosed()) {
            return "(" + StringUtil.join(all, delimiter) + ")";
        } else {
            return StringUtil.join(all, delimiter);
        }
    }
}
