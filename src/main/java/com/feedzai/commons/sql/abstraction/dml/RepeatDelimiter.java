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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Represents expressions that ca be repeated.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class RepeatDelimiter extends Expression {
    /**
     * The AND delimited.
     */
    public static final String AND = " AND ";
    /**
     * The OR delimited.
     */
    public static final String OR = " OR ";
    /**
     * The + delimited.
     */
    public static final String PLUS = " + ";
    /**
     * The - delimited.
     */
    public static final String MINUS = " - ";
    /**
     * The * delimited.
     */
    public static final String MULT = " * ";
    /**
     * The / delimited.
     */
    public static final String DIV = " / ";
    /**
     * The &gt; delimited.
     */
    public static final String GT = " > ";
    /**
     * The &lt; delimited.
     */
    public static final String LT = " < ";
    /**
     * The &#8805; delimited.
     */
    public static final String GTEQ = " >= ";
    /**
     * The &#8804; delimited.
     */
    public static final String LTEQ = " <= ";
    /**
     * The LIKE delimited.
     */
    public static final String LIKE = " LIKE ";
    /**
     * The EQ delimited.
     */
    public static final String EQ = " = ";
    /**
     * The NEQ delimiter.
     */
    public static final String NEQ = " <> ";
    /**
     * The COMMA (,) delimiter.
     */
    public static final String COMMA = ", ";
    /**
     * The IN delimited.
     */
    public static final String IN = " IN ";
    /**
     * The NOT IN delimited.
     */
    public static final String NOTIN = " NOT IN ";

    /**
     * The delimiter.
     */
    private String delimiter;
    /**
     * The list of expressions.
     */
    private final List<Expression> expressions = new ArrayList<Expression>();

    /**
     * Creates a new instance of {@link RepeatDelimiter}.
     *
     * @param delimiter The delimiter to use.
     * @param dbe       The list of expressions.
     */
    public RepeatDelimiter(final String delimiter, final Expression... dbe) {
        this.delimiter = delimiter;
        this.expressions.addAll(Arrays.asList(dbe));
    }

    /**
     * Creates a new instance of {@link RepeatDelimiter}.
     *
     * @param delimiter The delimiter to use.
     * @param dbe       The collection of expressions.
     */
    public RepeatDelimiter(final String delimiter, final Collection<? extends Expression> dbe) {
        this.delimiter = delimiter;
        this.expressions.addAll(dbe);
    }

    /**
     * Gets the expressions.
     *
     * @return The expressions.
     */
    public List<Expression> getExpressions() {
        return ImmutableList.copyOf(expressions);
    }

    /**
     * Gets the delimiter for repetitions.
     *
     * @return The delimiter.
     */
    public String getDelimiter() {
        return delimiter;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
