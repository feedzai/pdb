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

/**
 * Represents the UPDATE operator.
 *
 * @author Rui Vilao(rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class Update extends Expression {
    /**
     * The table.
     */
    private final Expression table;
    /**
     * The set expression.
     */
    private final List<Expression> columns = new ArrayList<Expression>();
    /**
     * The where clause.
     */
    private Expression where = null;

    /**
     * Creates a new instance of {@link Update}.
     *
     * @param table The table to update.
     */
    public Update(final Expression table) {
        this.table = table;

    }

    /**
     * The set keyword.
     *
     * @param exps The expressions.
     * @return This expression.
     */
    public Update set(final Expression... exps) {
        this.columns.addAll(Arrays.asList(exps));

        return this;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }

    /**
     * Gets the table.
     *
     * @return The table.
     */
    public Expression getTable() {
        return table;
    }

    /**
     * Gets the columns.
     *
     * @return The columns.
     */
    public List<Expression> getColumns() {
        return ImmutableList.copyOf(columns);
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
     * The set keyword.
     *
     * @param exps The expressions.
     * @return This expression.
     */
    public Update set(final Collection<? extends Expression> exps) {
        this.columns.addAll(exps);

        return this;
    }

    /**
     * Adds the WHERE expression.
     *
     * @param where The WHERE expression.
     * @return This expression.
     */
    public Update where(final Expression where) {
        this.where = where;

        return this;
    }
}
