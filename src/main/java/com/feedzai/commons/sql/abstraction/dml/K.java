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

import java.util.Objects;

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
     * @param constant The constant object.
     */
    public K(final Object constant) {
        this.constant = constant;
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

    @Override
    public int hashCode() {
        return Objects.hashCode(this.constant);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        final K other = (K) obj;
        return Objects.equals(this.constant, other.constant);
    }

    @Override
    public String toString() {
        return String.format("K(%s)", this.constant);
    }
}
