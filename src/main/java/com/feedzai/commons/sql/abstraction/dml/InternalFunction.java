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
 * Represents functions that are internal to the database engine in place.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.1
 */
public class InternalFunction extends Function {
    /**
     * Creates a new instance of {@link InternalFunction}.
     *
     * @param function The function.
     */
    public InternalFunction(final String function) {
        super(function);
    }

    /**
     * Creates a new instance of {@link InternalFunction}.
     *
     * @param function The function.
     * @param exp      The expression.
     */
    public InternalFunction(final String function, final Expression exp) {
        super(function, exp);
    }

    @Override
    public boolean isUDF() {
        return false;
    }
}
