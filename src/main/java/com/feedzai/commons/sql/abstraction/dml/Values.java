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

import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.union;

/**
 * The union clause.
 *
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.3.1
 */
public class Values extends Expression {

    private final List<Row> rows;

    private final String[] names;

    public Values(String... names) {
        this.names = names;
        this.rows = new ArrayList<>();
    }

    public List<Row> getRows() {
        return rows;
    }

    public String[] getNames() {
        return names;
    }

    @Override
    public String translate() {
        return translator.translate(this);
        /*if (this.translator instanceof PostgreSqlTranslator) {
            // Engines that support VALUES.
            this.rows.forEach(expression -> injector.injectMembers(expression));
            final String translation = "VALUES " + this.rows.stream()
                    .map(row -> {
                        row.enclose();
                        return row.translate();
                    })
                    .collect(Collectors.joining(", "));

            if (this.names != null && this.names.length > 0) {
                final String namesTranslation = Arrays.stream(this.names)
                        .map(StringUtils::quotize)
                        .collect(Collectors.joining(", "));
                return "SELECT * FROM (" + translation + ") as \"temp\"(" + namesTranslation + ")";
            } else {
                return translation;
            }

        } else {

            if (this.names != null && this.names.length > 0) {
                this.rows.forEach(row -> row.names(this.names));
            }
            final Set<Expression> literals = this.rows.stream()
                    .map(SqlBuilder::select)
                    .collect(Collectors.toSet());

            final Union union = union(literals).all();

            injector.injectMembers(union);
            return "SELECT * FROM (" + union.translate() + ") as \"temp\"";
        }*/
    }

    public Values rows(final Row... newRows) {
        this.rows.addAll(Arrays.asList(newRows));
        return this;
    }

    public Values rows(final Collection<Row> newRows) {
        this.rows.addAll(newRows);
        return this;
    }


    public static class Row extends Expression {
        private final List<Expression> expressions;

        public Row(final Collection<Expression> expressions) {
            this.expressions = new ArrayList<>(expressions);
        }

        public Row(final Expression... expressions) {
            this.expressions = Arrays.asList(expressions);
        }

        public List<Expression> getExpressions() {
            return expressions;
        }

        @Override
        public String translate() {
            return translator.translate(this);
            /*if (names != null) {
                for (int i = 0; i < this.names.length && i < expressions.size(); i++) {
                    expressions.get(i).alias(this.names[i]);
                }
            }
            expressions.forEach(expression -> injector.injectMembers(expression));
            final String translation = expressions.stream()
                    .map(Expression::translate)
                    .collect(Collectors.joining(", "));
            if (isEnclosed()) {
                return "(" + translation + ")";
            } else {
                return translation;
            }*/
        }


    }
}
