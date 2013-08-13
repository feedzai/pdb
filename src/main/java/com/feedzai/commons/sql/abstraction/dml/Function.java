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

/**
 * Represents SQL functions.
 */
public class Function extends Expression {
    /**
     * The MAX function.
     */
    public static final String MAX = "MAX";
    /**
     * The MIN function.
     */
    public static final String MIN = "MIN";
    /**
     * The AVG function.
     */
    public static final String AVG = "AVG";
    /**
     * The COUNT function.
     */
    public static final String COUNT = "COUNT";
    /**
     * The STDDEV function.
     */
    public static final String STDDEV = "STDDEV";
    /**
     * The SUM function.
     */
    public static final String SUM = "SUM";

    /**
     * The function.
     */
    private String function;
    /**
     * The expression enclosed in the function.
     */
    private Expression exp;

    /**
     * Creates a new instance of {@link Function}.
     *
     * @param function The function.
     */
    public Function(final String function) {
        this(function, null);
    }

    /**
     * Creates a new instance of {@link Function}.
     *
     * @param function The function.
     * @param exp      The expression.
     */
    public Function(final String function, final Expression exp) {
        this.function = StringUtil.escapeSql(function);
        this.exp = exp;
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        String exp = "";

        if (this.exp != null) {
            exp = this.exp.translateDB2(properties);
        }

        if (STDDEV.equalsIgnoreCase(this.function)) {
            /* DB2 STDDEV divides VARIANCE by N instead of N-1 (why IBM??? why?), this fixes it */
            return "SQRT(VARIANCE(" + exp + ")*COUNT(1)/(COUNT(1)-1))";

        }
        if (AVG.equalsIgnoreCase(this.function)) {
           /* DB2 AVG is type sensitive - avg of int returns int (why IBM???)*/
            return "AVG(" + exp + "+0.0)";
        } else {
            // if it is a user-defined function
            if (isUDF() && properties.existsSchema()) {
                return properties.getSchema() + "." + function + "(" + exp + ")";
            } else {
                return function + "(" + exp + ")";
            }
        }
    }

    @Override
    public String translateOracle(final PdbProperties properties) {
        String exp = "";

        if (this.exp != null) {
            exp = this.exp.translateOracle(properties);
        }

        // if it is a user-defined function
        if (isUDF() && properties.existsSchema()) {
            return properties.getSchema() + "." + function + "(" + exp + ")";
        } else {
            return function + "(" + exp + ")";
        }
    }

    @Override
    public String translateMySQL(final PdbProperties properties) {
        String function = this.function;
        String exp = "";

        if (this.exp != null) {
            exp = this.exp.translateMySQL(properties);
        }

        if (function.equals(STDDEV)) {
            function = "STDDEV_SAMP";
        }

        // if it is a user-defined function
        if (isUDF() && properties.existsSchema()) {
            return properties.getSchema() + "." + function + "(" + exp + ")";
        } else {
            return function + "(" + exp + ")";
        }
    }

    @Override
    public String translateSQLServer(final PdbProperties properties) {
        String function = this.function;
        String exp = "";

        if (this.exp != null) {
            exp = this.exp.translateSQLServer(properties);
        }

        if (function.equals(STDDEV)) {
            function = "STDEV";
        }

        if (function.equals(AVG)) {
            exp = String.format("CONVERT(DOUBLE PRECISION, %s)", exp);
        }

        // if it is a user-defined function
        if (isUDF() && properties.existsSchema()) {
            return properties.getSchema() + "." + function + "(" + exp + ")";
        } else {
            return function + "(" + exp + ")";
        }
    }

    @Override
    public String translatePostgreSQL(final PdbProperties properties) {
        String exp = "";

        if (this.exp != null) {
            exp = this.exp.translatePostgreSQL(properties);
        }

        // if it is a user-defined function
        if (isUDF() && properties.existsSchema()) {
            return properties.getSchema() + "." + function + "(" + exp + ")";
        } else {
            return function + "(" + exp + ")";
        }
    }

    @Override
    public String translateH2(PdbProperties properties) {
        String exp = "";

        if (this.exp != null) {
            exp = this.exp.translateH2(properties);
        }

        if (function.equals(AVG)) {
            exp = String.format("CONVERT(%s, DOUBLE PRECISION)", exp);
        }

        // if it is a user-defined function
        if (isUDF() && properties.existsSchema()) {
            return properties.getSchema() + "." + function + "(" + exp + ")";
        } else {
            return function + "(" + exp + ")";
        }
    }

    /**
     * @return Whether or not the current function is a user-defined function.
     */
    private boolean isUDF() {
        return !function.equals(MAX) && !function.equals(MIN) && !function.equals(AVG) &&
                !function.equals(COUNT) && !function.equals(STDDEV) && !function.equals(SUM);
    }
}
