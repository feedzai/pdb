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
package com.feedzai.commons.sql.abstraction.ddl;

/**
 * Represents the available column constraints.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public enum DbColumnConstraint {
    /**
     * The unique constraint.
     */
    UNIQUE {
        @Override
        public String translate() {
            return "UNIQUE";
        }
    },
    /**
     * The not null constraint.
     */
    NOT_NULL {
        @Override
        public String translate() {
            return "NOT NULL";
        }
    };

    /**
     * The default translation.
     *
     * @return The default translation.
     */
    public abstract String translate();
}
