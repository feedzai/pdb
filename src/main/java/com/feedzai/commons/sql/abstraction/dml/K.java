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
 * An SQL constant.
 */
public class K extends Expression {
    /** The constant object. */
    private final Object o;

    /**
     * Creates a new instance of {@link K}.
     * @param o The constant object.
     */
    public K(Object o) {
        this.o = o;
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        String result;

        if (o != null) {

            if (!isQuote()) {
                result = o.toString();
            } else if (o instanceof String) {
                result = StringUtil.singleQuotize(StringUtil.escapeSql((String) o));
            } else if (o instanceof Boolean) {
                result = (Boolean) o ? "'1'" : "'0'";
            } else {
                result = o.toString();
            }

        } else {
            result = "NULL";
        }

        return isEnclosed() ? ("(" + result + ")") : result;
    }

    @Override
    public String translateOracle(final PdbProperties properties) {
        String result;

        if (o != null) {

            if (!isQuote()) {
                result = o.toString();
            } else if (o instanceof String) {
                result = StringUtil.singleQuotize(StringUtil.escapeSql((String) o));
            } else if (o instanceof Boolean) {
                result = (Boolean) o ? "'1'" : "'0'";
            } else {
                result = o.toString();
            }

        } else {
            result = "NULL";
        }

        return isEnclosed() ? ("(" + result + ")") : result;
    }

    @Override
    public String translateMySQL(final PdbProperties properties) {
        String result;

        if (o != null) {

            if (!isQuote()) {
                result = o.toString();
            } else if (o instanceof String) {
                result = StringUtil.singleQuotize(StringUtil.escapeSql((String) o));
            } else if (o instanceof Boolean) {
                result = (Boolean) o ? "1" : "0";
            } else {
                result = o.toString();
            }

        } else {
            result = "NULL";
        }

        return isEnclosed() ? ("(" + result + ")") : result;
    }

    @Override
    public String translateSQLServer(final PdbProperties properties) {
        String result;

        if (o != null) {

            if (!isQuote()) {
                result = o.toString();
            } else if (o instanceof String) {
                result = StringUtil.singleQuotize(StringUtil.escapeSql((String) o));
            } else if (o instanceof Boolean) {
                result = (Boolean) o ? "1" : "0";
            } else {
                result = o.toString();
            }

        } else {
            result = "NULL";
        }

        return isEnclosed() ? ("(" + result + ")") : result;
    }

    @Override
    public String translatePostgreSQL(final PdbProperties properties) {
        String result;

        if (o != null) {

            if (!isQuote()) {
                result = o.toString();
            } else if (o instanceof String) {
                result = StringUtil.singleQuotize(StringUtil.escapeSql((String) o));
            } else if (o instanceof Boolean) {
                result = (Boolean) o ? "TRUE" : "FALSE";
            } else {
                result = o.toString();
            }

        } else {
            result = "NULL";
        }

        return isEnclosed() ? ("(" + result + ")") : result;
    }

    @Override
    public String translateH2(PdbProperties properties) {
        String result;

        if (o != null) {

            if (!isQuote()) {
                result = o.toString();
            } else if (o instanceof String) {
                result = StringUtil.singleQuotize(StringUtil.escapeSql((String) o));
            } else if (o instanceof Boolean) {
                result = (Boolean) o ? "TRUE" : "FALSE";
            } else {
                result = o.toString();
            }

        } else {
            result = "NULL";
        }

        return isEnclosed() ? ("(" + result + ")") : result;
    }

}
