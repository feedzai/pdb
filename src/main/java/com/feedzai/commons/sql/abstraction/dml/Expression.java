/*
 * Copyright 2014 Feedzai
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.feedzai.commons.sql.abstraction.dml;

import com.feedzai.commons.sql.abstraction.engine.AbstractTranslator;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A generic SQL Expression capable of being translated.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public abstract class Expression implements Serializable {
    /**
     * The abstract translator.
     */
    @Inject
    protected AbstractTranslator translator;
    /**
     * The PDB properties.
     */
    @Inject
    protected PdbProperties properties;
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
    protected boolean withNoLock = false;

    /**
     * Translates the expression.
     *
     * @return A translation of the implementing expression.
     */
    public abstract String translate();

    /**
     * Aliases this expression.
     *
     * @param alias The alias.
     * @return This expression.
     */
    public Expression alias(final String alias) {
        this.alias = alias;

        return this;
    }

    /**
     * Removes the quotes for this expression.
     *
     * @return This expression.
     */
    public Expression unquote() {
        this.quotes = false;

        return this;
    }

    /**
     * Encloses this expression with parenthesis.
     *
     * @return This expression.
     */
    public Expression enclose() {
        this.enclosed = true;

        return this;
    }

    /**
     * Sets ordering to ascendant.
     *
     * @return This expression.
     */
    public Expression asc() {
        this.ordering = "ASC";

        return this;
    }

    /**
     * Sets ordering to descendant.
     *
     * @return This expression.
     */
    public Expression desc() {
        this.ordering = "DESC";

        return this;
    }

    /**
     * Checks if this expression is to be enclosed in parenthesis.
     *
     * @return {@code true} if this expression is enclosed, {@code false} otherwise.
     */
    public boolean isEnclosed() {
        return enclosed;
    }

    /**
     * Checks if this expression is to be quoted.
     *
     * @return {@code true} if this expression is quoted, {@code false} otherwise.
     */
    public boolean isQuote() {
        return quotes;
    }

    /**
     * Checks if this expression is to be aliased.
     *
     * @return {@code true} if this expression is aliased, {@code false} otherwise.
     */
    public boolean isAliased() {
        return alias != null;
    }

    /**
     * Sets an inner join with the current table.
     *
     * @param table The table to join.
     * @param expr  The expressions to join.
     * @return This expression.
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
     * @return This expression.
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
     * @return This expression.
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
     * @return This expression.
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
     * @return This expression.
     */
    public Expression withNoLock() {
        this.withNoLock = true;

        return this;
    }

    /**
     * Checks if this expression (TABLES) is with no lock.
     *
     * @return {@code true} if this expression (TABLE) is with no lock, {@code false} otherwise.
     */
    public boolean isWithNoLock() {
        return withNoLock;
    }

    /**
     * Gets the join list.
     *
     * @return The join list.
     */
    public List<Join> getJoins() {
        return ImmutableList.copyOf(joins);
    }

    /**
     * Gets the ordering.
     *
     * @return The ordering.
     */
    public String getOrdering() {
        return ordering;
    }

    /**
     * Checks if this expression is to be translated with quotes.
     *
     * @return {@code true} if this expression is to be translated with quotes, {@code false} otherwise.
     */
    public boolean isQuotes() {
        return quotes;
    }

    /**
     * Gets the alias.
     *
     * @return The alias, {@code null} if not applicable.
     */
    public String getAlias() {
        return alias;
    }
}
