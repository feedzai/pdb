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

import com.feedzai.commons.sql.abstraction.util.StringUtils;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Represents SQL functions.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class Function extends Expression {
    /**
     * The MAX function.
     */
    public static final String MAX = "MAX";
    /**
     * The MIN function.
     */
    public static final String MIN = "MIN";
    /**
     * The AVG function.
     */
    public static final String AVG = "AVG";
    /**
     * The COUNT function.
     */
    public static final String COUNT = "COUNT";
    /**
     * The STDDEV function.
     */
    public static final String STDDEV = "STDDEV";
    /**
     * The SUM function.
     */
    public static final String SUM = "SUM";
    /**
     * The UPPER function.
     */
    public static final String UPPER = "UPPER";
    /**
     * The LOWER function.
     */
    public static final String LOWER = "lower";
    /**
     * The FLOOR function.
     */
    public static final String FLOOR = "FLOOR";
    /**
     * The FLOOR function.
     */
    public static final String CEILING = "CEILING";
    /**
     * The list of functions.
     */
    public static final Set<String> FUNCTIONS;

    static {
        FUNCTIONS = new ImmutableSet.Builder<String>()
                .add(MAX)
                .add(MIN)
                .add(AVG)
                .add(COUNT)
                .add(STDDEV)
                .add(SUM)
                .add(UPPER)
                .add(LOWER)
                .add(FLOOR)
                .add(CEILING)
                .build();
    }

    /**
     * The function.
     */
    private String function;
    /**
     * The expression enclosed in the function.
     */
    private Expression exp;

    /**
     * Creates a new instance of {@link Function}.
     *
     * @param function The function.
     */
    public Function(final String function) {
        this(function, null);
    }

    /**
     * Creates a new instance of {@link Function}.
     *
     * @param function The function.
     * @param exp      The expression.
     */
    public Function(final String function, final Expression exp) {
        this.function = StringUtils.escapeSql(function);
        this.exp = exp;
    }

    /**
     * Gets the function.
     *
     * @return The function.
     */
    public String getFunction() {
        return function;
    }

    /**
     * Gets the expression in the function.
     *
     * @return The expression in the function.
     */
    public Expression getExp() {
        return exp;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }

    /**
     * Checks if this function is a UDF.
     *
     * @return {@code true} if the function is user defined, {@code false} otherwise.
     */
    public boolean isUDF() {
        return !FUNCTIONS.contains(function);
    }
}
