/*
 * Copyright 2019 Feedzai
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * The Values clause.
 *
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.4.0
 */
public class Values extends Expression {

    /**
     * The columns' aliases.
     */
    private final String[] aliases;

    /**
     * The rows.
     */
    private final List<Row> rows;

    /**
     * Creates a new Values.
     * 
     * @param aliases the columns' aliases.
     */
    public Values(final String... aliases) {
        this.aliases = aliases;
        this.rows = new ArrayList<>();
    }

    /**
     * Creates a new Values.
     *
     * @param aliases the columns' aliases.
     */
    public Values(final Collection<String> aliases) {
        this.aliases = aliases.toArray(new String[0]);
        this.rows = new ArrayList<>();
    }

    /**
     * Gets the columns' aliases.
     *
     * @return the columns' aliases.
     */
    public String[] getAliases() {
        return aliases;
    }

    /**
     * Gets the rows.
     *
     * @return the rows.
     */
    public List<Row> getRows() {
        return rows;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }

    /**
     * Adds rows to values.
     *
     * @param newRows new rows to be added.
     * @return this values.
     */
    public Values rows(final Row... newRows) {
        this.rows.addAll(Arrays.asList(newRows));
        return this;
    }

    /**
     * Adds rows to values.
     *
     * @param newRows new rows to be added.
     * @return this values.
     */
    public Values rows(final Collection<Row> newRows) {
        this.rows.addAll(newRows);
        return this;
    }

    /**
     * A Row belonging to a Values clause.
     *
     * @author Francisco Santos (francisco.santos@feedzai.com)
     * @since 2.4.0
     */
    public static class Row extends Expression {
        
        /**
         * The list of expressions on the row.
         */
        private final List<Expression> expressions;

        /**
         * Creates a new row.
         * 
         * @param expressions the expressions on the row.
         */
        public Row(final Collection<Expression> expressions) {
            this.expressions = new ArrayList<>(expressions);
        }

        /**
         * Creates a new row.
         *
         * @param expressions the expressions on the row.
         */
        public Row(final Expression... expressions) {
            this.expressions = Arrays.asList(expressions);
        }

        /**
         * Gets the list of expressions.
         * 
         * @return the list of expressions.
         */
        public List<Expression> getExpressions() {
            return expressions;
        }

        @Override
        public String translate() {
            return translator.translate(this);
        }

    }
}
