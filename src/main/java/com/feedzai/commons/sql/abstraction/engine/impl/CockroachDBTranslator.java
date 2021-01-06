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

package com.feedzai.commons.sql.abstraction.engine.impl;

import com.feedzai.commons.sql.abstraction.ddl.DbColumn;
import com.feedzai.commons.sql.abstraction.dml.Cast;
import com.feedzai.commons.sql.abstraction.dml.Expression;
import com.feedzai.commons.sql.abstraction.dml.RepeatDelimiter;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.feedzai.commons.sql.abstraction.engine.OperationNotSupportedRuntimeException;

import java.util.ArrayList;
import java.util.List;

import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.VARCHAR_SIZE;
import static java.lang.String.format;

/**
 * Provides SQL translation for CockroachDB.
 *
 * @author Mário Pereira (mario.arzileiro@feedzai.com)
 * @since 2.5.0
 */
public class CockroachDBTranslator extends PostgreSqlTranslator {

    @Override
    public String translate(final DbColumn c) {
        switch (c.getDbColumnType()) {
            case BOOLEAN:
                return "BOOLEAN";

            case DOUBLE:
                return "DOUBLE PRECISION";

            case INT:
                return "INT4";

            case LONG:
                return "INT8";

            case STRING:
                return format("VARCHAR(%s)", c.isSizeSet() ? c.getSize().toString() : properties.getProperty(VARCHAR_SIZE));

            case CLOB:
                return "TEXT";

            case BLOB:
                return "BYTEA";

            case JSON:
                return "JSONB";

            default:
                throw new DatabaseEngineRuntimeException(format(
                        "Mapping not found for '%s'. Please report this error.",
                        c.getDbColumnType()
                ));
        }
    }

    @Override
    public String translate(final Cast cast) {
        final String type;

        // Cast to type.
        switch (cast.getType()) {
            case BOOLEAN:
                type = "BOOLEAN";
                break;
            case DOUBLE:
                type = "DOUBLE PRECISION";
                break;
            case INT:
                type = "INT4";
                break;
            case LONG:
                type = "INT8";
                break;
            case STRING:
                type = "VARCHAR";
                break;
            default:
                throw new OperationNotSupportedRuntimeException(format("Cannot cast to '%s'.", cast.getType()));
        }

        inject(cast.getExpression());
        final String translation = format("CAST(%s AS %s)",
                cast.getExpression().translate(),
                type);

        return cast.isEnclosed() ? "(" + translation + ")" : translation;
    }

    @Override
    public String translate(final RepeatDelimiter rd) {
        final String delimiter = rd.getDelimiter();
        final List<String> all = new ArrayList<>();

        // unlike PostgreSQL, CockroachDB needs *all* parameters in the division to be CAST as DOUBLE
        for (final Expression expression : rd.getExpressions()) {
            inject(expression);
            if (RepeatDelimiter.DIV.equals(delimiter)) {
                all.add(String.format("CAST(%s AS DOUBLE PRECISION)", expression.translate()));
            } else {
                all.add(expression.translate());
            }
        }

        if (rd.isEnclosed()) {
            return "(" + join(all, delimiter) + ")";
        } else {
            return join(all, delimiter);
        }
    }
}