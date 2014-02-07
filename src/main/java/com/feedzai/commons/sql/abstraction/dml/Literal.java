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
 * Represents a literal object that will have no specific translation.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class Literal extends Expression {

    /**
     * The literal object.
     */
    private final Object literal;

    /**
     * Creates a new instance of {@link Literal}.
     *
     * @param o The literal.
     */
    public Literal(final Object o) {
        this.literal = o;
    }

    /**
     * Gets the literal object.
     *
     * @return The literal object.
     */
    public Object getLiteral() {
        return literal;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
