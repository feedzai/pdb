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
import java.util.ArrayList;
import java.util.List;
import com.feedzai.commons.sql.abstraction.util.StringUtil;

/**
 * The DELETE operator.
 */
public class Delete extends Expression {
    /** The where expression. */
    private Expression where;
    /** The Tabçe. */
    private final Expression table;

    /**
     * Creates a new instance of {@link Delete}.
     * @param table The table.
     */
    public Delete(final Expression table) {
        this.table = table;
    }

    /**
     * Sets the where expression.
     * @param where The where expression.
     * @return This object.
     */
    public Delete where(final Expression where) {
        this.where = where;

        return this;
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("DELETE FROM");
        temp.add(table.translateDB2(properties));

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translateDB2(properties));
        }

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translateOracle(final PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("DELETE FROM");
        temp.add(table.translateOracle(properties));

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translateOracle(properties));
        }

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translateMySQL(final PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("DELETE FROM");
        temp.add(table.translateMySQL(properties));

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translateMySQL(properties));
        }

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translateSQLServer(final PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("DELETE FROM");
        temp.add(table.translateSQLServer(properties));

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translateSQLServer(properties));
        }

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translatePostgreSQL(final PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("DELETE FROM");
        temp.add(table.translatePostgreSQL(properties));

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translatePostgreSQL(properties));
        }

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translateH2(PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("DELETE FROM");
        temp.add(table.translateH2(properties));

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translateH2(properties));
        }

        return StringUtil.join(temp, " ");
    }
}
