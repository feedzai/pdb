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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * The UPDATE SQL keyword.
 */
public class Update extends Expression {

    /** The table. */
    private final Expression table;
    /** The set expression. */
    private final List<Expression> columns = new ArrayList<Expression>();
    /** The where clause. */
    private Expression where = null;

    /**
     * Creates a new instance of {@link Update}.
     * @param table
     */
    public Update(final Expression table) {
        this.table = table;
        
    }

    /**
     * The set keyword.
     * @param exps The expressions.
     * @return This object.
     */
    public Update set(final Expression... exps) {
        this.columns.addAll(Arrays.asList(exps));

        return this;
    }

    /**
     * The set keyword.
     * @param exps The expressions.
     * @return This object.
     */
    public Update set(final Collection<? extends Expression> exps) {
        this.columns.addAll(exps);

        return this;
    }

    /**
     * The where clause.
     * @param where The object.
     * @return This object.
     */
    public Update where(final Expression where) {
        this.where = where;

        return this;
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("UPDATE");
        temp.add(table.translateDB2(properties));
        if (table.isAliased()) {
            temp.add(StringUtil.quotize(table.alias));
        }
        temp.add("SET");
        List<String> setTranslations = new ArrayList<String>();
        for (Expression e : columns) {
            setTranslations.add(e.translateDB2(properties));
        }
        temp.add(StringUtil.join(setTranslations, ", "));

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translateDB2(properties));
        }

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translateOracle(final PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("UPDATE");
        temp.add(table.translateOracle(properties));
        if (table.isAliased()) {
            temp.add(StringUtil.quotize(table.alias));
        }
        temp.add("SET");
        List<String> setTranslations = new ArrayList<String>();
        for (Expression e : columns) {
            setTranslations.add(e.translateOracle(properties));
        }
        temp.add(StringUtil.join(setTranslations, ", "));

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translateOracle(properties));
        }

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translateMySQL(final PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("UPDATE");
        temp.add(table.translateMySQL(properties));
        if (table.isAliased()) {
            temp.add(StringUtil.quotize(table.alias, MySqlEngine.ESCAPE_CHARACTER));
        }
        temp.add("SET");
        List<String> setTranslations = new ArrayList<String>();
        for (Expression e : columns) {
            setTranslations.add(e.translateMySQL(properties));
        }
        temp.add(StringUtil.join(setTranslations, ", "));

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translateMySQL(properties));
        }

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translateSQLServer(final PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("UPDATE");
        if (table.isAliased()) {
            temp.add(StringUtil.quotize(table.alias));
        } else {
            temp.add(table.translateSQLServer(properties));
        }

        temp.add("SET");
        List<String> setTranslations = new ArrayList<String>();
        for (Expression e : columns) {
            setTranslations.add(e.translateSQLServer(properties));
        }
        temp.add(StringUtil.join(setTranslations, ", "));

        temp.add("FROM");
        temp.add(table.translateSQLServer(properties));
        if (table.isAliased()) {
            temp.add(StringUtil.quotize(table.alias));
        }

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translateSQLServer(properties));
        }

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translatePostgreSQL(final PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("UPDATE");
        temp.add(table.translatePostgreSQL(properties));
        if (table.isAliased()) {
            temp.add(StringUtil.quotize(table.alias));
        }
        temp.add("SET");
        List<String> setTranslations = new ArrayList<String>();
        for (Expression e : columns) {
            setTranslations.add(e.translatePostgreSQL(properties));
        }
        temp.add(StringUtil.join(setTranslations, ", "));

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translatePostgreSQL(properties));
        }

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translateH2(PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("UPDATE");
        temp.add(table.translateH2(properties));
        if (table.isAliased()) {
            temp.add(StringUtil.quotize(table.alias));
        }
        temp.add("SET");
        List<String> setTranslations = new ArrayList<String>();
        for (Expression e : columns) {
            setTranslations.add(e.translateH2(properties));
        }
        temp.add(StringUtil.join(setTranslations, ", "));

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translateH2(properties));
        }

        return StringUtil.join(temp, " ");
    }

}
