/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.dml;

import com.feedzai.commons.sql.abstraction.dml.dialect.Dialect;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A generic SQL Expression capable of being translated.
 * <p/>
 * NOTE: There are DDL expressions too so this class should be moved from this package.
 */
public abstract class Expression implements Serializable {
    /**
     * The expression alias if applicable.
     */
    protected String alias = null;
    /**
     * True if the expression is to be enclosed in parenthesis.
     */
    protected boolean enclosed = false;
    /**
     * True if the expression is to be put around quotes.
     */
    protected boolean quotes = true;
    /**
     * The string that specified the ordering if applicable.
     */
    protected String ordering = null;
    /**
     * The list of joins, if applicable.
     */
    protected final List<Join> joins = new ArrayList<Join>();
    /**
     * The SQL Server's no lock keyword.
     */
    protected boolean noLock = false;

    /**
     * Translates this expression according to the dialect.
     *
     * @param diaclect   The dialect.
     * @param properties The configuration.
     * @return The result of the translation.
     */
    public String translate(final Dialect diaclect, final PdbProperties properties) {
        switch (diaclect) {
            case MYSQL:
                return translateMySQL(properties);

            case POSTGRESQL:
                return translatePostgreSQL(properties);

            case ORACLE:
                return translateOracle(properties);

            case SQLSERVER:
                return translateSQLServer(properties);

            case H2:
                return translateH2(properties);

            case DB2:
                return translateDB2(properties);

            default:
                throw new DatabaseEngineRuntimeException("Unknown diaclect " + diaclect);
        }
    }

    /**
     * DB2 SQL translation.
     *
     * @param properties The configuration.
     * @return The translation result.
     */
    public abstract String translateDB2(PdbProperties properties);

    /**
     * Oracle SQL translation.
     *
     * @param properties The configuration.
     * @return The translation result.
     */
    public abstract String translateOracle(final PdbProperties properties);

    /**
     * MySQL SQL translation.
     *
     * @param properties The configuration.
     * @return The translation result.
     */
    public abstract String translateMySQL(final PdbProperties properties);

    /**
     * SQLServer SQL translation.
     *
     * @param properties The configuration.
     * @return The translation result.
     */
    public abstract String translateSQLServer(final PdbProperties properties);

    /**
     * PostgreSQL SQL translation.
     *
     * @param properties The configuration.
     * @return The translation result.
     */
    public abstract String translatePostgreSQL(final PdbProperties properties);

    /**
     * PostgreSQL SQL translation.
     *
     * @param properties The configuration.
     * @return The translation result.
     */
    public abstract String translateH2(final PdbProperties properties);

    /**
     * Alias this expression.
     *
     * @param alias The alias.
     * @return This object.
     */
    public Expression alias(final String alias) {
        this.alias = alias;

        return this;
    }

    /**
     * Remove quotes for this expression.
     *
     * @return This object.
     */
    public Expression unquote() {
        this.quotes = false;

        return this;
    }

    /**
     * Enclose this expression with parenthesis.
     *
     * @return This object.
     */
    public Expression enclose() {
        this.enclosed = true;

        return this;
    }

    /**
     * Sets ordering to ascendent.
     *
     * @return This object.
     */
    public Expression asc() {
        this.ordering = "ASC";

        return this;
    }

    /**
     * Sets ordering to descendent.
     *
     * @return This object.
     */
    public Expression desc() {
        this.ordering = "DESC";

        return this;
    }

    /**
     * @return True if this expression is enclosed, false otherwise.
     */
    public boolean isEnclosed() {
        return enclosed;
    }

    /**
     * True if this expression is quoted, false otherwise.
     *
     * @return true if expression is in between quotes
     */
    public boolean isQuote() {
        return quotes;
    }

    public boolean isAliased() {
        return alias != null;
    }

    /**
     * Sets an inner join with the current table.
     *
     * @param table The table to join.
     * @param expr  The expressions to join.
     * @return This object.
     */
    public Expression innerJoin(final Expression table, final Expression expr) {
        if (table instanceof Query) {
            table.enclose();
        }
        joins.add(new Join("INNER JOIN", table, expr));

        return this;
    }

    /**
     * Sets a left outer join with the current table.
     *
     * @param table The table to join.
     * @param expr  The expressions to join.
     * @return This object.
     */
    public Expression leftOuterJoin(final Expression table, final Expression expr) {
        if (table instanceof Query) {
            table.enclose();
        }

        joins.add(new Join("LEFT OUTER JOIN", table, expr));

        return this;
    }

    /**
     * Sets a right outer join with the current table.
     *
     * @param table The table to join.
     * @param expr  The expressions to join.
     * @return This object.
     */
    public Expression rightOuterJoin(final Expression table, final Expression expr) {
        if (table instanceof Query) {
            table.enclose();
        }
        joins.add(new Join("RIGHT OUTER JOIN", table, expr));

        return this;
    }

    /**
     * Sets a full outer join with the current table.
     *
     * @param table The table to join.
     * @param expr  The expressions to join.
     * @return This object.
     */
    public Expression fullOuterJoin(final Expression table, final Expression expr) {
        if (table instanceof Query) {
            table.enclose();
        }
        joins.add(new Join("FULL OUTER JOIN", table, expr));

        return this;
    }

    /**
     * Sets no lock keyword on SQL Server tables.
     *
     * @return This object.
     */
    public Expression withNoLock() {
        this.noLock = true;

        return this;
    }

    /**
     * @return True of the no lock keyword is set, false otherwise.
     */
    public boolean isWithNoLock() {
        return this.noLock;
    }
}
