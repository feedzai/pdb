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
 * Represents a SQL constant.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class K extends Expression {
    /**
     * The constant object.
     */
    private final Object constant;

    /**
     * Creates a new instance of {@link K}.
     *
     * @param o The constant object.
     */
    public K(Object o) {
        this.constant = o;
    }

    /**
     * Gets the constant.
     *
     * @return The constant.
     */
    public Object getConstant() {
        return constant;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
