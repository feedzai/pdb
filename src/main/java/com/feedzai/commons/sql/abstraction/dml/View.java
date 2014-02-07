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
 * Represents a SQL view.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class View extends Expression {
    /**
     * The name of the view.
     */
    private final String name;
    /**
     * Signals if the view is to replace.
     */
    private boolean replace = false;
    /**
     * The expression.
     */
    private Expression as;

    /**
     * Creates a new instance of {@link View}.
     *
     * @param name The name of the view.
     */
    public View(final String name) {
        this.name = StringEscapeUtils.escapeSql(name);
    }

    /**
     * Gets the AS expression.
     *
     * @return The AS expression.
     */
    public Expression getAs() {
        return as;
    }

    /**
     * Sets this view to be replaced.
     *
     * @return This expression.
     */
    public View replace() {
        this.replace = true;

        return this;
    }

    /**
     * Sets the AS expression.
     *
     * @param as The AS expression.
     * @return This expression.
     */
    public View as(final Expression as) {
        this.as = as;

        return this;
    }

    /**
     * Checks if this view is to be replaced.
     *
     * @return {@code true} if the view is to be replaced, {@code false} otherwise.
     */
    public boolean isReplace() {
        return replace;
    }

    /**
     * Gets the view name.
     *
     * @return The view name.
     */
    public String getName() {
        return name;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
