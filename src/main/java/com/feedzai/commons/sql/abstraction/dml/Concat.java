/*
 * Copyright 2020 Feedzai
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

import java.util.Collection;

/**
 * PDB Concat Operator.
 *
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since @@@feedzai.next.release@@@
 */
public class Concat extends Expression {

    /**
     * The concatenation delimiter.
     */
    private final Expression delimiter;

    /**
     * The expressions to concatenate.
     */
    private final Collection<Expression> expressions;

    /**
     * Creates a new {@link Concat} object.
     *
     * @param delimiter The concatenation delimiter.
     * @param expressions The expressions to concatenate.
     */
    public Concat(final Expression delimiter, final Collection<Expression> expressions) {
        this.delimiter = delimiter;
        this.expressions = expressions;
    }

    /**
     * Adds an expression to concatenate.
     *
     * @param expression The expression to concatenate.
     */
    public Concat with(final Expression expression) {
        expressions.add(expression);
        return this;
    }

    /**
     * Gets the concatenation delimiter.
     *
     * @return the concatenation delimiter.
     */
    public Expression getDelimiter() {
        return delimiter;
    }

    /**
     * Gets the expressions to concatenate.
     *
     * @return the expressions to concatenate.
     */
    public Collection<Expression> getExpressions() {
        return expressions;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
