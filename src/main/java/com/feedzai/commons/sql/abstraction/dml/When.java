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
public class When extends Expression {

    /**
     * The condition to verify.
     */
    public final Expression condition;

    /**
     * The action to be executed if the condition is true.
     */
    public final Expression action;

    /**
     * Creates a new When expression.
     *
     * @param condition condition to verify.
     * @param action action to be executed if the condition is true.
     */
    private When(final Expression condition, final Expression action) {
        this.condition = condition;
        this.action = action;
    }

    /**
     * Creates a new When expression.
     *
     * @param condition condition to verify.
     * @param action action to be executed if the condition is true.
     * @return a new when.
     */
    public static When when(final Expression condition, final Expression action) {
        return new When(condition, action);
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
