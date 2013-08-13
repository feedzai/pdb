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

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a SQL view.
 */
public class View extends Expression {
    /** The name of the view. */
    private final String name;
    /** Signals if the view is to replace. */
    private boolean replace = false;
    /** The expression. */
    private Expression as;

    /**
     * Creates a new instance of {@link View}.
     * @param name The name of the view.
     */
    public View(final String name) {
        this.name = StringUtil.escapeSql(name);
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        final List<String> res = new ArrayList<String>();
        res.add("CREATE");

        if (replace) {
            res.add("OR REPLACE");
        }

        res.add("VIEW");
        res.add(StringUtil.quotize(name));
        res.add("AS " + as.translateDB2(properties));

        return StringUtil.join(res, " ");
    }

    @Override
    public String translateOracle(final PdbProperties properties) {
        final List<String> res = new ArrayList<String>();
        res.add("CREATE");

        if (replace) {
            res.add("OR REPLACE");
        }

        res.add("VIEW");
        res.add(StringUtil.quotize(name));
        res.add("AS " + as.translateOracle(properties));

        return StringUtil.join(res, " ");
    }

    @Override
    public String translateMySQL(final PdbProperties properties) {
        final List<String> res = new ArrayList<String>();
        res.add("CREATE");

        if (replace) {
            res.add("OR REPLACE");
        }

        res.add("VIEW");
        res.add(StringUtil.quotize(name, MySqlEngine.ESCAPE_CHARACTER));
        res.add("AS " + as.translateMySQL(properties));

        return StringUtil.join(res, " ");
    }

    @Override
    public String translateSQLServer(final PdbProperties properties) {
        final List<String> res = new ArrayList<String>();

        res.add("CREATE VIEW");
        res.add(StringUtil.quotize(name));
        res.add("AS " + as.translateSQLServer(properties));

        return StringUtil.join(res, " ");
    }

    @Override
    public String translatePostgreSQL(final PdbProperties properties) {
        final List<String> res = new ArrayList<String>();
        res.add("CREATE");

        if (replace) {
            res.add("OR REPLACE");
        }

        res.add("VIEW");
        res.add(StringUtil.quotize(name));
        res.add("AS " + as.translatePostgreSQL(properties));

        return StringUtil.join(res, " ");
    }
    
    @Override
    public String translateH2(PdbProperties properties) {
        final List<String> res = new ArrayList<String>();
        res.add("CREATE");

        if (replace) {
            res.add("OR REPLACE");
        }

        res.add("VIEW");
        res.add(StringUtil.quotize(name));
        res.add("AS " + as.translateH2(properties));

        return StringUtil.join(res, " ");
    }

    /**
     * Sets this view to be replaced.
     * @return This object.
     */
    public View replace() {
        this.replace = true;

        return this;
    }

    /**
     * Sets the as expression.
     * @param as The as expression.
     * @return This object.
     */
    public View as(final Expression as) {
        this.as = as;

        return this;
    }

    /**
     * @return Checks if this view is to be replaced.
     */
    public boolean isReplace() {
        return replace;
    }

    /**
     * @return The view name.
     */
    public String getName() {
        return name;
    }

}
