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

import org.apache.commons.lang.StringEscapeUtils;


/**
 * Represents a named expression.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class Name extends Expression {
    /**
     * The environment.
     */
    private String environment = null;
    /**
     * The object name.
     */
    private final String name;
    /**
     * When {@code true} IS NULL is appended to the expression.
     */
    private boolean isNull = false;
    /**
     * When {@code true} IS NOT NULL is appended to the expression.
     */
    private boolean isNotNull = false;

    /**
     * Creates a new instance of {@link Name}.
     *
     * @param name The object name.
     */
    public Name(final String name) {
        this.name = name;
    }

    /**
     * Creates a new instance of {@link Name}.
     *
     * @param tableName The environment.
     * @param name      The name.
     */
    public Name(final String tableName, final String name) {
        this.environment = StringEscapeUtils.escapeSql(tableName);
        this.name = StringEscapeUtils.escapeSql(name);
    }

    /**
     * Appends "IS NULL" to this expression.
     *
     * @return This expression.
     */
    public Name isNull() {
        this.isNull = true;

        return this;
    }

    /**
     * Appends "IS NOT NULL" to this expression.
     *
     * @return This expression.
     */
    public Name isNotNull() {
        this.isNotNull = true;

        return this;
    }

    /**
     * Gets the name.
     *
     * @return The name.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the environment.
     *
     * @return The environment.
     */
    public String getEnvironment() {
        return environment;
    }

    /**
     * Checks if IS NULL is to be appended to this expression.
     *
     * @return {@code true} if it is to be appended, {@code false} otherwise.
     */
    public boolean isIsNull() {
        return isNull;
    }

    /**
     * Checks if IS NOT NULL is to be appended to this expression.
     *
     * @return {@code true} if it is to be appended, {@code false} otherwise.
     */
    public boolean isIsNotNull() {
        return isNotNull;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
