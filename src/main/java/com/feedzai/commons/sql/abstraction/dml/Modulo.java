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
 * The MOD operator.
 *
 * @author Joao Martins (joao.martins@feedzai.com)
 * @since 2.0.0
 */
public class Modulo extends Expression {
    /**
     * The dividend.
     */
    private final Expression dividend;
    /**
     * The divisor.
     */
    private final Expression divisor;

    /**
     * Creates a new instance of {@link Modulo}.
     *
     * @param dividend The dividend.
     * @param divisor  The divisor.
     */
    public Modulo(final Expression dividend, final Expression divisor) {
        this.dividend = dividend;
        this.divisor = divisor;
    }

    /**
     * Gets the dividend.
     *
     * @return The dividend
     */
    public Expression getDividend() {
        return dividend;
    }

    /**
     * Gets the divisor.
     *
     * @return The divisor.
     */
    public Expression getDivisor() {
        return divisor;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
