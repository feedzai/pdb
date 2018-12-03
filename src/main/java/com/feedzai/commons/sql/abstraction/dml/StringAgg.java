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

/**
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.2.3
 */
public class StringAgg extends Expression {

    /**
     * The column to aggregate.
     */
    public final Expression column;

    /**
     * The delimiter.
     */
    private char delimiter;

    /**
     * Is it distinct.
     */
    private boolean distinct;

    /**
     * @param column column to be aggregated.
     */
    private StringAgg(final Expression column) {
        this.column = column;
        this.delimiter = ',';
        this.distinct = false;
    }

    /**
     * Returns a new StringAgg.
     *
     * @param column column to be aggregated.
     * @return a new StringAgg.
     */
    public static StringAgg stringAgg(final Expression column) {
        return new StringAgg(column);
    }

    /**
     * Returns the column to be aggregated.
     *
     * @return column to be aggregated.
     */
    public Expression getColumn() {
        return column;
    }

    /**
     * Returns the delimiter.
     *
     * @return the delimiter.
     */
    public char getDelimiter() {
        return delimiter;
    }

    /**
     * Returns if it should apply DISTINCT.
     *
     * @return if it should apply DISTINCT.
     */
    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }

    /**
     * Apply distinct.
     *
     * @return this
     */
    public StringAgg distinct() {
        this.distinct = true;
        return this;
    }

    /**
     * Sets the delimiter.
     *
     * @param delimiter char that splits records aggregated.
     * @return this
     */
    public StringAgg delimiter(final char delimiter){
        this.delimiter = delimiter;
        return this;
    }
}
