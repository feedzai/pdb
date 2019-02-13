/*
 * Copyright 2018 Feedzai
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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.List;

/**
 * With SQL Expression.
 *
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.3.1
 */
public class With extends Expression {

    /**
     * The clauses.
     */
    private final List<ImmutablePair<Name, Expression>> clauses;

    /**
     * Expression to execute given the with clauses.
     */
    private Expression then;

    /**
     * Creates a new WITH expression.
     *
     * @param alias expression alias.
     * @param expression expression to be aliased.
     */
    public With(final String alias, final Expression expression) {
        this.clauses = Lists.newArrayList(new ImmutablePair<>(new Name(alias), expression));
    }

    /**
     * Gets the clauses.
     *
     * @return the clauses.
     */
    public List<ImmutablePair<Name, Expression>> getClauses() {
        return clauses;
    }

    /**
     * Gets the then expression.
     *
     * @return the then expression.
     */
    public Expression getThen() {
        return then;
    }

    /**
     * Adds an alias and expression to the with clause.
     *
     * @param alias expression alias
     * @param expression expression to be aliased.
     * @return this object.
     */
    public With andWith(final String alias, final Expression expression) {
        this.clauses.add(new ImmutablePair<>(new Name(alias), expression));
        return this;
    }

    /**
     * Sets the expression to be executed given the clauses.
     *
     * @param thenExpression expression to be executed given the clauses.
     * @return this object.
     */
    public With then(final Expression thenExpression) {
        this.then = thenExpression;
        return this;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
