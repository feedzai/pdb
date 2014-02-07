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

/**
 * Represents the BETWEEN operator.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class Between extends Expression {
    /**
     * The column.
     */
    private final Expression column;
    /**
     * The AND expression.
     */
    private final Expression and;
    /**
     * Negates the expression.
     */
    private boolean not = false;

    /**
     * Creates a new instance of {@link Between}.
     *
     * @param column The column.
     * @param exp    The AND expression.
     */
    public Between(final Expression column, final Expression exp) {
        this.column = column;
        this.and = exp;
    }

    /**
     * Negates the expression.
     *
     * @return This expression.
     */
    public Between not() {
        not = true;

        return this;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }

    /**
     * Gets the column in the expression.
     *
     * @return The column in the expression.
     */
    public Expression getColumn() {
        return column;
    }

    /**
     * Gets the AND expression.
     *
     * @return The AND expression.
     */
    public Expression getAnd() {
        return and;
    }

    /**
     * Checks if the expression is to be negated.
     *
     * @return {@code true} if the expression is to be negated, {@code false} otherwise.
     */
    public boolean isNot() {
        return not;
    }
}
