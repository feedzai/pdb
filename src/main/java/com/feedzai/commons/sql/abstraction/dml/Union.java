/*
 * Copyright 2019 Feedzai
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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * The union clause.
 *
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.3.1
 */
public class Union extends Expression {

    /**
     * The expressions to union.
     */
    private final List<Expression> expressions;

    /**
     * Signals if it is a UNION ALL.
     */
    private boolean all;

    /**
     * Creates a new UNION expression.
     *
     * @param expressions expressions to union.
     */
    public Union(final Collection<Expression> expressions) {
        this.expressions = new LinkedList<>(expressions);
        this.all = false;
    }

    /**
     * Creates a new UNION expression.
     *
     * @param expressions expressions to union.
     */
    public Union(final Expression... expressions) {
        this.expressions = Arrays.asList(expressions);
        this.all = false;
    }

    /**
     * Sets this UNION to UNION ALL.
     *
     * @return this UNION ALL.
     */
    public Union all() {
        all = true;
        return this;
    }

    /**
     * Gets the expressions to union.
     *
     * @return the expressions to union.
     */
    public List<Expression> getExpressions() {
        return expressions;
    }

    /**
     * Returns true if this is an UNION ALL expression, false otherwise.
     *
     * @return true if this is an UNION ALL expression, false otherwise.
     */
    public boolean isAll() {
        return all;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
