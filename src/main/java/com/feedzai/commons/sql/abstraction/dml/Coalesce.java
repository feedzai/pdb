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
 * Represents the coalesce operator.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class Coalesce extends Expression {
    /**
     * The expression to test.
     */
    private final Expression exp;
    /**
     * The alternative expressions.
     */
    private final Expression[] alternative;

    /**
     * Creates a new instance of {@link Coalesce}.
     *
     * @param exp         The expression to test.
     * @param alternative The alternative expressions.
     */
    public Coalesce(final Expression exp, final Expression... alternative) {
        this.exp = exp;
        this.alternative = alternative;
    }

    /**
     * Gets the expression to test.
     *
     * @return The expression to test.
     */
    public Expression getExp() {
        return exp;
    }

    /**
     * Gets the alternative expressions.
     *
     * @return The alternative expressions.
     */
    public Expression[] getAlternative() {
        return alternative;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
