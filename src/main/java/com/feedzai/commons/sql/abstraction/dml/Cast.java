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

import com.feedzai.commons.sql.abstraction.ddl.DbColumnType;

/**
 * The cast expression.
 *
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.4.0
 */
public class Cast extends Expression {

    /**
     * The expression.
     */
    private final Expression expression;

    /**
     * The type to be converted.
     */
    private final DbColumnType type;

    /**
     * Creates a new cast expression.
     *
     * @param expression the expression.
     * @param type the type to be converted.
     */
    public Cast(final Expression expression, final DbColumnType type) {
        this.expression = expression;
        this.type = type;
    }

    /**
     * Gets the expression.
     *
     * @return The expression.
     */
    public Expression getExpression() {
        return expression;
    }

    /**
     * Gets the type to be converted.
     *
     * @return The type to be converted.
     */
    public DbColumnType getType() {
        return type;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
