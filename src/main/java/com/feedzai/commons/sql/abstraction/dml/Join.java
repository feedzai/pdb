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

import org.apache.commons.lang.StringEscapeUtils;

/**
 * Represents a SQL join operator.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class Join extends Expression {
    /**
     * The join type represented in a String (INNER, OUTER, etc).
     */
    private String join = null;
    /**
     * The Table to join.
     */
    private Expression joinTable = null;
    /**
     * The Expression to join.
     */
    private Expression joinExpr = null;

    /**
     * Creates a new instance of {@link Join}.
     *
     * @param join      The join string.
     * @param joinTable The join table.
     * @param joinExpr  The join expression.
     */
    public Join(final String join, final Expression joinTable, final Expression joinExpr) {
        this.join = StringEscapeUtils.escapeSql(join);
        this.joinTable = joinTable;
        this.joinExpr = joinExpr;
    }

    /**
     * Gets the join type represented in a String.
     *
     * @return The join type.
     */
    public String getJoin() {
        return join;
    }

    /**
     * Gets the join table.
     *
     * @return The join table.
     */
    public Expression getJoinTable() {
        return joinTable;
    }

    /**
     * Gets the join expression.
     *
     * @return The join expression.
     */
    public Expression getJoinExpr() {
        return joinExpr;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
