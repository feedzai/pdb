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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.and;

/**
 * Represents a SQL Query.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class Query extends Expression {
    /**
     * The list of object in the select clause.
     */
    private final List<Expression> selectColumns = new ArrayList<>();
    /**
     * The list of object in the from clause.
     */
    private final List<Expression> fromColumns = new ArrayList<>();
    /**
     * The WHERE clause.
     */
    private Expression where = null;
    /**
     * The list of objects in the group by clause.
     */
    private final List<Expression> groupbyColumns = new ArrayList<>();
    /**
     * The having clause.
     */
    private Expression having = null;
    /**
     * The list of object in the order by clause.
     */
    private final List<Expression> orderbyColumns = new ArrayList<>();
    /**
     * Limit the number of rows.
     */
    private Integer limit = 0;
    /**
     * Offset before the limit of rows.
     */
    private Integer offset = 0;
    /**
     * Signals the DISTINCT keyword.
     */
    private boolean distinct = false;

    /**
     * Creates a new instance of {@link Query}.
     */
    public Query() {
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }

    /**
     * Gets the SELECT columns.
     *
     * @return The SELECT columns.
     */
    public List<Expression> getSelectColumns() {
        return ImmutableList.copyOf(selectColumns);
    }

    /**
     * Gets the FROM columns.
     *
     * @return The FROM columns.
     */
    public List<Expression> getFromColumns() {
        return ImmutableList.copyOf(fromColumns);
    }

    /**
     * Gets the WHERE expression.
     *
     * @return The WHERE expression.
     */
    public Expression getWhere() {
        return where;
    }

    /**
     * Gets the GROUP BY expressions.
     *
     * @return The GROUP BY expressions.
     */
    public List<Expression> getGroupbyColumns() {
        return ImmutableList.copyOf(groupbyColumns);
    }

    /**
     * Gets the HAVING expression.
     *
     * @return The HAVING expression.
     */
    public Expression getHaving() {
        return having;
    }

    /**
     * Gets the ORDER BY expressions.
     *
     * @return The ORDER BY expressions.
     */
    public List<Expression> getOrderbyColumns() {
        return ImmutableList.copyOf(orderbyColumns);
    }

    /**
     * Gets the limit. {@code null} is returned when not applicable.
     *
     * @return The limit.
     */
    public Integer getLimit() {
        return limit;
    }

    /**
     * Gets the offset. {@code null} is returned when not applicable.
     *
     * @return The offset.
     */
    public Integer getOffset() {
        return offset;
    }

    /**
     * Checks if the SELECT expression is distinct.
     *
     * @return {@code true} is the SELECT expression is distinct, {@code false} otherwise.
     */
    public boolean isDistinct() {
        return distinct;
    }

    /**
     * Sets the SELECT expression as DISTINCT.
     *
     * @return This expression.
     */
    public Query distinct() {
        this.distinct = true;

        return this;
    }

    /**
     * Adds the SELECT columns.
     *
     * @param selectColumns The columns.
     * @return This expression.
     */
    public Query select(final Expression... selectColumns) {
        if (selectColumns == null) {
            return this;
        }

        return select(Arrays.asList(selectColumns));
    }

    /**
     * Adds the SELECT columns.
     *
     * @param selectColumns The columns.
     * @return This expression.
     */
    public Query select(final Collection<? extends Expression> selectColumns) {
        this.selectColumns.addAll(selectColumns);

        return this;
    }

    /**
     * Adds the FROM columns.
     *
     * @param fromColumns The columns.
     * @return This expression.
     */
    public Query from(final Expression... fromColumns) {
        if (fromColumns == null) {
            return this;
        }

        return from(Arrays.asList(fromColumns));
    }

    /**
     * Adds the FROM columns.
     *
     * @param fromColumns The columns.
     * @return This expression.
     */
    public Query from(final Collection<? extends Expression> fromColumns) {
        if (fromColumns == null) {
            return this;
        }

        this.fromColumns.addAll(fromColumns);

        return this;
    }

    /**
     * The WHERE clause.
     * <p/>
     * If there is no where clause already defined, sets to the defined value.
     * <p/>
     * If there is already an expression for where defined, defines an <code>and</code> expression between old and new clauses.
     * <p/>
     * Example:
     * <pre>
     *     query.where(eq(column("col1"),k(1)).andWhere(eq(column("col2"),k(2))
     *
     *     will be translated to
     *
     *     ("col1" = 1) AND ("col2" = 2)
     * </pre>
     *
     * @param where The WHERE expression.
     * @return This expression.
     */
    public Query andWhere(final Expression where) {
        if (this.where == null) {
            this.where = where;
        } else {
            this.where = and(this.where.enclose(), where.enclose());
        }

        return this;
    }

    /**
     * The where clause.
     * <p/>
     * <b>Note:</b>This method replaces any <code>where</code> clauses previously defined.
     *
     * @param where The object.
     * @return This expression.
     */
    public Query where(final Expression where) {
        this.where = where;

        return this;
    }

    /**
     * Adds the GROUP BY columns.
     *
     * @param groupbyColumns The columns.
     * @return This expression.
     */
    public Query groupby(final Expression... groupbyColumns) {
        if (groupbyColumns == null) {
            return this;
        }

        return groupby(Arrays.asList(groupbyColumns));
    }

    /**
     * Adds the GROUP BY columns.
     *
     * @param groupbyColumns The columns.
     * @return This expression.
     */
    public Query groupby(final Collection<? extends Expression> groupbyColumns) {
        if (groupbyColumns == null) {
            return this;
        }

        this.groupbyColumns.addAll(groupbyColumns);

        return this;
    }

    /**
     * Adds the HAVING expression.
     *
     * @param having The having expression.
     * @return This expression.
     */
    public Query having(final Expression having) {
        this.having = having;

        return this;
    }

    /**
     * Adds the ORDER BY columns.
     *
     * @param orderbyColumns The columns.
     * @return This expression.
     */
    public Query orderby(final Expression... orderbyColumns) {
        if (orderbyColumns == null) {
            return this;
        }

        return orderby(Arrays.asList(orderbyColumns));
    }

    /**
     * Adds the ORDER BY columns.
     *
     * @param orderbyColumns The columns.
     * @return This expression.
     */
    public Query orderby(final Collection<? extends Expression> orderbyColumns) {
        if (orderbyColumns == null) {
            return this;
        }

        this.orderbyColumns.addAll(orderbyColumns);

        return this;
    }

    /**
     * Sets the limit.
     *
     * @param limit The number of rows that the query returns.
     * @return This expression.
     */
    public Query limit(final Integer limit) {
        this.limit = limit;

        return this;
    }

    /**
     * Sets the offset.
     *
     * @param offset The start position
     * @return This expression.
     */
    public Query offset(final Integer offset) {
        this.offset = offset;

        // If we defined offset without limit, force a limit to handle MySQL and others gracefully.
        if (limit <= 0) {
            limit = Integer.MAX_VALUE - offset;
        }

        return this;
    }
}
