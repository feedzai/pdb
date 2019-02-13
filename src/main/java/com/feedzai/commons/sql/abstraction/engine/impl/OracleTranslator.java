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
package com.feedzai.commons.sql.abstraction.engine.impl;

import com.feedzai.commons.sql.abstraction.ddl.AlterColumn;
import com.feedzai.commons.sql.abstraction.ddl.DbColumn;
import com.feedzai.commons.sql.abstraction.ddl.DbColumnConstraint;
import com.feedzai.commons.sql.abstraction.ddl.DbColumnType;
import com.feedzai.commons.sql.abstraction.ddl.DropPrimaryKey;
import com.feedzai.commons.sql.abstraction.ddl.Rename;
import com.feedzai.commons.sql.abstraction.dml.Cast;
import com.feedzai.commons.sql.abstraction.dml.Expression;
import com.feedzai.commons.sql.abstraction.dml.Function;
import com.feedzai.commons.sql.abstraction.dml.Join;
import com.feedzai.commons.sql.abstraction.dml.K;
import com.feedzai.commons.sql.abstraction.dml.Modulo;
import com.feedzai.commons.sql.abstraction.dml.Name;
import com.feedzai.commons.sql.abstraction.dml.Query;
import com.feedzai.commons.sql.abstraction.dml.RepeatDelimiter;
import com.feedzai.commons.sql.abstraction.dml.StringAgg;
import com.feedzai.commons.sql.abstraction.dml.View;
import com.feedzai.commons.sql.abstraction.engine.AbstractTranslator;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.feedzai.commons.sql.abstraction.engine.OperationNotSupportedRuntimeException;
import com.feedzai.commons.sql.abstraction.util.StringUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.VARCHAR_SIZE;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.readString;
import static java.lang.String.format;

/**
 * Provides SQL translation for Oracle.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class OracleTranslator extends AbstractTranslator {
    @Override
    public String translate(AlterColumn ac) {
        final DbColumn column = ac.getColumn();
        final Expression table = ac.getTable();
        final Name name = new Name(column.getName());
        inject(table, name);

        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(table.translate())
                .append(" MODIFY (")
                .append(name.translate())
                .append(" ")
                .append(translate(column))
                .append(" ");

        List<Object> trans = Lists.transform(column.getColumnConstraints(), DbColumnConstraint::translate);


        sb.append(Joiner.on(" ").join(trans));
        sb.append(")");


        return sb.toString();
    }

    @Override
    public String translate(DropPrimaryKey dpk) {
        final Expression table = dpk.getTable();
        inject(table);

        if (!(table instanceof Name)) {
            throw new DatabaseEngineRuntimeException("DropPrimaryKey must receive a Name object");
        }

        final String tableName = ((Name) table).getName();

        final String pkName = StringUtils.md5(format("PK_%s", tableName), properties.getMaxIdentifierSize());

        return String.format("ALTER TABLE %s DROP CONSTRAINT %s", table.translate(), StringUtils.quotize(pkName));
    }

    @Override
    public String translate(Function f) {
        final String function = f.getFunction();
        final Expression exp = f.getExp();
        inject(exp);

        String expTranslated = "";

        if (exp != null) {
            expTranslated = exp.translate();
        }

        return function + "(" + expTranslated + ")";
    }

    @Override
    public String translate(Modulo m) {
        final Expression dividend = m.getDividend();
        final Expression divisor = m.getDivisor();
        inject(dividend, divisor);

        return String.format("MOD(%s, %s)", dividend.translate(), divisor.translate());
    }

    @Override
    public String translate(Rename r) {
        final Expression oldName = r.getOldName();
        final Expression newName = r.getNewName();
        inject(oldName, newName);

        return String.format("ALTER TABLE %s RENAME TO %s", oldName.translate(), newName.translate());
    }

    @Override
    public String translate(RepeatDelimiter rd) {
        final String delimiter = rd.getDelimiter();

        List<Object> all = Lists.transform(rd.getExpressions(), input -> {
            inject(input);
            return input.translate();
        });

        if (rd.isEnclosed()) {
            return "(" + org.apache.commons.lang3.StringUtils.join(all, delimiter) + ")";
        } else {
            return org.apache.commons.lang3.StringUtils.join(all, delimiter);
        }
    }

    @Override
    public String translate(Query q) {
        final List<Expression> fromColumns = q.getFromColumns();
        final List<Expression> groupbyColumns = q.getGroupbyColumns();
        final List<Expression> orderbyColumns = q.getOrderbyColumns();
        final List<Expression> selectColumns = q.getSelectColumns();
        final Expression having = q.getHaving();
        final Expression where = q.getWhere();
        inject(fromColumns);
        inject(groupbyColumns);
        inject(orderbyColumns);
        inject(selectColumns);
        inject(having, where);

        final List<String> query = new ArrayList<>();

        // SELECT CLAUSE
        query.add("SELECT" + (q.isDistinct() ? " DISTINCT" : ""));
        final List<String> querySelectColumns = new ArrayList<>();
        for (Expression dbe : selectColumns) {
            if (dbe instanceof Query) {
                querySelectColumns.add("(" + dbe.translate() + (!dbe.isAliased() ? ")" : ") AS " + quotize(dbe.getAlias())));
            } else {
                querySelectColumns.add(dbe.translate() + (!dbe.isAliased() ? "" : " AS " + quotize(dbe.getAlias())));
            }
        }
        query.add(join(querySelectColumns, ", "));

        // FROM CLAUSE
        query.add("FROM");
        if (!fromColumns.isEmpty()) {
            final List<String> queryFromColumns = new ArrayList<>();
            for (Expression dbe : fromColumns) {
                final List<String> insideFrom = new ArrayList<>();
                if (dbe instanceof Query) {
                    insideFrom.add("(" + dbe.translate() + (!dbe.isAliased() ? ")" : ") " + quotize(dbe.getAlias())));
                } else {
                    insideFrom.add(dbe.translate() + (!dbe.isAliased() ? "" : " " + quotize(dbe.getAlias())));
                }

                final List<Join> joins = dbe.getJoins();
                if (!joins.isEmpty()) {
                    for (Join j : joins) {
                        inject(j);
                        insideFrom.add(j.translate());
                    }
                }

                queryFromColumns.add(join(insideFrom, " "));
            }
            query.add(join(queryFromColumns, ", "));
        } else {
            query.add(quotize("DUAL"));
        }

        // WHERE CLAUSE
        if (where != null) {
            query.add("WHERE");
            query.add(where.translate());
        }

        // GROUP BY CLAUSE
        if (!groupbyColumns.isEmpty()) {
            query.add("GROUP BY");
            final List<String> queryGroupByColumns = new ArrayList<>();
            for (Expression column : groupbyColumns) {
                queryGroupByColumns.add(column.translate());
            }
            query.add(join(queryGroupByColumns, ", "));
        }

        // HAVING CLAUSE
        if (having != null) {
            query.add("HAVING");
            query.add(having.translate());
        }

        // ORDER BY
        if (!orderbyColumns.isEmpty()) {
            query.add("ORDER BY");
            final List<String> queryOrderByColumns = new ArrayList<>();
            for (Expression column : orderbyColumns) {
                queryOrderByColumns.add(column.translate());
            }
            query.add(join(queryOrderByColumns, ", "));
        }

        String finalQuery = join(query, " ");

        // LIMIT AND OFFSET
        if (q.getLimit() > 0) {
            if (q.getOffset() > 0) {
                finalQuery = String.format("SELECT * FROM (SELECT rownum rnum ,offlim.* FROM (%s) offlim WHERE rownum <= %d) WHERE rnum > %d", finalQuery,
                        q.getLimit() + q.getOffset(), q.getOffset());
            } else {
                finalQuery = String.format("SELECT * FROM (%s) a WHERE rownum <= %d", finalQuery, q.getLimit());
            }
        }

        return q.isEnclosed() ? ("(" + finalQuery + ")") : finalQuery;
    }

    @Override
    public String translate(View v) {
        final Expression as = v.getAs();
        final String name = v.getName();
        inject(as);

        final List<String> res = new ArrayList<>();
        res.add("CREATE");

        if (v.isReplace()) {
            res.add("OR REPLACE");
        }

        res.add("VIEW");
        res.add(quotize(name));
        res.add("AS " + as.translate());

        return join(res, " ");
    }

    @Override
    public String translate(DbColumn c) {
        inject(c.getDefaultValue());
        switch (c.getDbColumnType()) {
            case BOOLEAN:
                return format("char %s check (%s in ('0', '1'))", c.isDefaultValueSet() ? "DEFAULT " + c.getDefaultValue().translate() : "",
                        quotize(c.getName()));

            case DOUBLE:
                return "BINARY_DOUBLE";

            case INT:
                return "INT";

            case LONG:
                return "NUMBER(19,0)";

            case STRING:
                return format("VARCHAR(%s)", c.isSizeSet() ? c.getSize().toString() : properties.getProperty(VARCHAR_SIZE));

            case CLOB:
            case JSON:
                return "CLOB";

            case BLOB:
                return "BLOB";

            default:
                throw new DatabaseEngineRuntimeException(format("Mapping not found for '%s'. Please report this error.", c.getDbColumnType()));
        }
    }

    @Override
    public String translate(final DbColumnType type) {
        switch (type) {
            case BOOLEAN:
                return "NUMBER(1,0)";

            case DOUBLE:
                return "BINARY_DOUBLE";

            case INT:
                return "INT";

            case LONG:
                return "NUMBER(19,0)";

            case STRING:
                return format("VARCHAR(%s)", properties.getProperty(VARCHAR_SIZE));

            default:
                throw new OperationNotSupportedRuntimeException(format("Cannot cast to '%s'.", type));
        }
    }

    @Override
    public String translate(final StringAgg stringAgg) {
        if (stringAgg.isDistinct()) {
            throw new OperationNotSupportedRuntimeException("LISTAGG does not support distinct. If you really need it, " +
                                                             "you may do it using regex or a subquery. " +
                                                             "Check this: https://dba.stackexchange.com/a/8478 or this:" +
                                                             "https://stackoverflow.com/a/50589222");
        }
        inject(stringAgg.column);
        String column = stringAgg.getColumn().translate();
        return String.format(
                "LISTAGG(%s, '%c') WITHIN GROUP (ORDER BY %s)",
                column,
                stringAgg.getDelimiter(),
                column
        );
    }

    @Override
    public String translateEscape() {
        return "\"";
    }

    @Override
    public String translateTrue() {
        return "'1'";
    }

    @Override
    public String translateFalse() {
        return "'0'";
    }

    @Override
    public String translate(Cast cast) {
        final Expression expression = cast.getExpression();
        inject(expression);

        if (cast.getType() == DbColumnType.BOOLEAN) {
            if (expression instanceof K) {
                final String translation = expression.translate()
                        .replaceAll("'", "")
                        .toLowerCase();

                if (translation.matches("t|true|1")) {
                    return "CAST(1 AS INT)";
                } else if (translation.matches("f|false|0")) {
                    return "CAST(0 AS INT)";
                }
            }
            // If expression is a K, but it does not match the regex
            // OR
            // expression is not a K
            return String.format("CAST(%s AS INT)", expression.translate());
        } else {
            return super.translate(cast);
        }
    }
}
