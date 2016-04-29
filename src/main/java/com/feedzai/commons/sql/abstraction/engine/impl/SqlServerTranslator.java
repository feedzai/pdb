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

import com.feedzai.commons.sql.abstraction.ddl.*;
import com.feedzai.commons.sql.abstraction.dml.*;
import com.feedzai.commons.sql.abstraction.engine.AbstractTranslator;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.feedzai.commons.sql.abstraction.util.StringUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.MAX_BLOB_SIZE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.VARCHAR_SIZE;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static java.lang.String.format;

/**
 * Provides SQL translation for SQLServer.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class SqlServerTranslator extends AbstractTranslator {
    @Override
    public String translate(AlterColumn ac) {
        final DbColumn column = ac.getColumn();
        final Expression table = ac.getTable();
        final Name name = new Name(column.getName());
        inject(table, name);

        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(table.translate())
                .append(" ALTER COLUMN ")
                .append(name.translate())
                .append(" ")
                .append(translate(column))
                .append(" ");

        List<Object> trans = Lists.transform(column.getColumnConstraints(), new com.google.common.base.Function<DbColumnConstraint, Object>() {
            @Override
            public Object apply(DbColumnConstraint input) {
                return input.translate();
            }
        });

        sb.append(Joiner.on(" ").join(trans));

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
        String function = f.getFunction();
        final Expression exp = f.getExp();
        inject(exp);

        String expTranslated = "";

        if (exp != null) {
            expTranslated = exp.translate();
        }

        if (Function.STDDEV.equals(function)) {
            function = "STDEV";
        }

        if (Function.AVG.equals(function)) {
            expTranslated = String.format("CONVERT(DOUBLE PRECISION, %s)", expTranslated);
        }

        // if it is a user-defined function
        if (f.isUDF() && properties.isSchemaSet()) {
            return properties.getSchema() + "." + function + "(" + expTranslated + ")";
        } else {
            return function + "(" + expTranslated + ")";
        }
    }

    @Override
    public String translate(Join j) {
        final String join = j.getJoin();
        final Expression joinExpr = j.getJoinExpr();
        final Expression joinTable = j.getJoinTable();
        inject(joinExpr, joinTable);

        if (joinTable.isAliased()) {
            return String.format("%s %s %s %s ON (%s)", join, joinTable.translate(), quotize(joinTable.getAlias()),
                    joinTable.isWithNoLock() ? " WITH(NOLOCK)" : "", joinExpr.translate());
        } else {
            return String.format("%s %s %s ON (%s)", join, joinTable.translate(), joinTable.isWithNoLock() ? " WITH(NOLOCK)" : "", joinExpr.translate());
        }
    }

    @Override
    public String translate(Modulo m) {
        final Expression dividend = m.getDividend();
        final Expression divisor = m.getDivisor();
        inject(dividend, divisor);

        String result = String.format("%s %% %s", dividend.translate(), divisor.translate());

        return m.isEnclosed() ? ("(" + result + ")") : result;
    }

    @Override
    public String translate(Rename r) {
        final Expression oldName = r.getOldName();
        final Expression newName = r.getNewName();
        inject(oldName, newName);

        return String.format("sp_rename %s, %s", oldName.translate(), newName.translate());
    }

    @Override
    public String translate(RepeatDelimiter rd) {
        final String delimiter = rd.getDelimiter();
        final List<Expression> exps = rd.getExpressions();

        List<String> all = new ArrayList<>();


        final Expression expression = exps.get(0);
        inject(expression);
        if (RepeatDelimiter.DIV.equals(delimiter)) {
            all.add(String.format("CONVERT(DOUBLE PRECISION, %s)", expression.translate()));
        } else {
            all.add(expression.translate());

        }


        for (int i = 1; i < exps.size(); i++) {
            Expression nthExpression = exps.get(i);
            inject(nthExpression);
            all.add(nthExpression.translate());
        }

        if (rd.isEnclosed()) {
            return "(" + org.apache.commons.lang.StringUtils.join(all, delimiter) + ")";
        } else {
            return org.apache.commons.lang.StringUtils.join(all, delimiter);
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
        // SQL Server does not have a pretty limit/offset syntax, though if is only a limit, we use top, otherwise we will use the ROW_NUM approach
        query.add("SELECT" + (q.isDistinct() ? " DISTINCT" : "") + ((q.getLimit() <= 0 || q.getOffset() > 0) ? "" : " TOP " + q.getLimit()));
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
        if (!fromColumns.isEmpty()) {
            query.add("FROM");
            final List<String> queryFromColumns = new ArrayList<>();
            for (Expression dbe : fromColumns) {
                final List<String> insideFrom = new ArrayList<>();
                if (dbe instanceof Query) {
                    insideFrom.add("(" + dbe.translate() + (!dbe.isAliased() ? ")" : ") " + quotize(dbe.getAlias())));
                } else {
                    insideFrom.add(dbe.translate() + (!dbe.isAliased() ? "" : " " + quotize(dbe.getAlias())) + (dbe.isWithNoLock() ? " WITH(NOLOCK)" : ""));
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

        // ORDER BY - Only place order by here if there is no offset defined
        if (q.getOffset() <= 0 && !orderbyColumns.isEmpty()) {
            query.add("ORDER BY");
            final List<String> queryOrderByColumns = new ArrayList<>();
            for (Expression column : orderbyColumns) {
                queryOrderByColumns.add(column.translate());
            }
            query.add(join(queryOrderByColumns, ", "));
        }

        String finalQuery = join(query, " ");

        // LIMIT AND OFFSET
        if (q.getLimit() > 0 && q.getOffset() > 0) {
            String orderByClause;

            /* When there is an order by we must place it onside both the inner and outer query */
            if (!orderbyColumns.isEmpty()) {
                final List<String> queryOrderByColumns = new ArrayList<>();
                for (Expression column : orderbyColumns) {
                    queryOrderByColumns.add(column.translate());
                }
                orderByClause = join(queryOrderByColumns, ", ");
                finalQuery = String.format("SELECT * FROM (SELECT ROW_NUMBER() OVER(ORDER BY %s) rnum ,offlim.* FROM (%s) offlim) offlim WHERE rnum <= %d and rnum > %d ORDER BY %s",
                        orderByClause, finalQuery, q.getLimit() + q.getOffset(), q.getOffset(), orderByClause);
            } else {
                finalQuery = String.format("SELECT * FROM (SELECT ROW_NUMBER() OVER(ORDER BY (SELECT 1)) rnum ,offlim.* FROM (%s) offlim) offlim WHERE rnum <= %d and rnum > %d",
                        finalQuery, q.getLimit() + q.getOffset(), q.getOffset());
            }
        }

        return q.isEnclosed() ? ("(" + finalQuery + ")") : finalQuery;
    }

    @Override
    public String translate(View v) {
        final Expression as = v.getAs();
        final String name = v.getName();
        inject(as);

        final List<String> res = new ArrayList<String>();

        res.add("CREATE VIEW");
        res.add(quotize(name));
        res.add("AS " + as.translate());

        return join(res, " ");
    }

    @Override
    public String translate(DbColumn c) {
        switch (c.getDbColumnType()) {
            case BOOLEAN:
                return "BIT";

            case DOUBLE:
                return "DOUBLE PRECISION";

            case INT:
                return "INT";

            case LONG:
                return "BIGINT";

            case STRING:
                return format("NVARCHAR(%s)", c.isSizeSet() ? c.getSize().toString() : properties.getProperty(VARCHAR_SIZE));

            case JSON:
            case CLOB:
                return "NVARCHAR(MAX)";

            case BLOB:
                if (properties.isMaxBlobSizeSet()) {
                    return format("VARBINARY(%s)", properties.getProperty(MAX_BLOB_SIZE));
                } else { // Use the default of the buffer, since it can't be greater than that.
                    return format("VARBINARY(MAX)");
                }


            default:
                throw new DatabaseEngineRuntimeException(format("Mapping not found for '%s'. Please report this error.", c.getDbColumnType()));
        }
    }

    @Override
    public String translate(Update u) {
        final List<Expression> columns = u.getColumns();
        final Expression table = u.getTable();
        final Expression where = u.getWhere();
        inject(table, where);

        final List<String> temp = new ArrayList<>();

        temp.add("UPDATE");
        if (table.isAliased()) {
            temp.add(quotize(table.getAlias(), translateEscape()));
        } else {
            temp.add(table.translate());
        }

        temp.add("SET");
        List<String> setTranslations = new ArrayList<>();
        for (Expression e : columns) {
            inject(e);
            setTranslations.add(e.translate());
        }
        temp.add(join(setTranslations, ", "));

        temp.add("FROM");
        temp.add(table.translate());
        if (table.isAliased()) {
            temp.add(quotize(table.getAlias()));
        }

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translate());
        }

        return join(temp, " ");
    }

    @Override
    public String translateEscape() {
        return "\"";
    }

    @Override
    public String translateTrue() {
        return "1";
    }

    @Override
    public String translateFalse() {
        return "0";
    }
}
