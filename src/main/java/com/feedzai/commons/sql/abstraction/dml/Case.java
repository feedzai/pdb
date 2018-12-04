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

import java.util.ArrayList;
import java.util.List;

import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.k;

/**
 * Case SQL Expression.
 *
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.2.3
 */
public class Case extends Expression {

    /**
     * List of when clauses.
     */
    public final List<When> whens;

    /**
     * Action to be executed if the condition is false.
     */
    private Expression falseAction;

    /**
     * Creates an empty case.
     */
    private Case() {
        this.whens = new ArrayList<>();
        this.falseAction = null;
    }

    /**
     * Gets the false action.
     *
     * @return the false action.
     */
    public Expression getFalseAction() {
        return falseAction;
    }

    /**
     * Returns a new empty case.
     *
     * @return a new case.
     */
    public static Case caseWhen() {
        return new Case();
    }

    /**
     * Returns a new "case when" that does the trueAction considering the condition.
     *
     * @param condition condition to verify.
     * @param trueAction action to be executed if the condition is true.
     * @return a new "case when" that does the trueAction considering the condition.
     */
    public static Case caseWhen(final Expression condition, final Expression trueAction) {
        return caseWhen().when(condition, trueAction);
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }

    /**
     * Adds a new when clause to this case.
     *
     * @param condition condition to verify.
     * @param action action to be executed if the condition is true.
     * @return this case.
     */
    public Case when(final Expression condition, final Expression action) {
        whens.add(When.when(condition, action));
        return this;
    }


    /**
     * Sets the false action.
     *
     * @param falseAction action done if the condition is false.
     * @return this case.
     */
    public Case otherwise(Expression falseAction){
        this.falseAction = falseAction;
        return this;
    }
}
