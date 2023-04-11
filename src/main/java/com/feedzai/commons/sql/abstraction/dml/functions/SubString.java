/*
 * Copyright 2023 Feedzai
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

package com.feedzai.commons.sql.abstraction.dml.functions;

import com.feedzai.commons.sql.abstraction.dml.Expression;

/**
 * SQL function "SUBSTRING", which returns a string that is a substring of the string in the colum expression.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
public class SubString extends Expression {

    /**
     * @see #getColumn()
     */
    public final Expression column;

    /**
     * @see #getStart()
     */
    private final Expression start;

    /**
     * @see #getLength()
     */
    private final Expression length;

    /**
     * Constructor for a {@link SubString} SQL expression, which returns a substring of the string in the colum
     * expression.
     *
     * @param column The column for which to obtain a substring.
     * @param start  The start position. The first position in string is 1.
     * @param length The number of characters to extract. Must be a positive number.
     */
    public SubString(final Expression column, final Expression start, final Expression length) {
        this.column = column;
        this.start = start;
        this.length = length;
    }

    /**
     * Returns the column expression for which to obtain a substring.
     *
     * @return The column for which to obtain a substring.
     */
    public Expression getColumn() {
        return column;
    }

    /**
     * Returns The start position of the substring in the original string. The first position is 1.
     *
     * @return The start position.
     */
    public Expression getStart() {
        return start;
    }

    /**
     * Returns the length of the substring, which is the number of characters to extract from the original string.
     * Must be a positive number.
     *
     * @return The number of characters to extract.
     */
    public Expression getLength() {
        return length;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
