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
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.feedzai.commons.sql.abstraction.engine.OperationNotSupportedRuntimeException;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.union;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.VARCHAR_SIZE;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.md5;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * Provides SQL translation for MySQL.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class MySqlTranslator extends AbstractTranslator {

    @Override
    public String translate(AlterColumn ac) {
        final DbColumn column = ac.getColumn();
        final Expression table = ac.getTable();
        final Name name = new Name(column.getName());

        inject(table, name);

        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(table.translate())
                .append(" MODIFY ")
                .append(name.translate())
                .append(" ")
                .append(translate(column))
                .append(" ");

        List<Object> trans = Lists.transform(column.getColumnConstraints(), DbColumnConstraint::translate);


        sb.append(Joiner.on(" ").join(trans));


        return sb.toString();
    }

    @Override
    public String translate(DropPrimaryKey dpk) {
        final Expression table = dpk.getTable();
        inject(table);

        return String.format("ALTER TABLE %s DROP PRIMARY KEY", table.translate());
    }

    @Override
    public String translate(com.feedzai.commons.sql.abstraction.dml.Function f) {
        String function = f.getFunction();
        final Expression exp = f.getExp();
        inject(exp);

        String expTranslated = "";

        if (exp != null) {
            expTranslated = exp.translate();
        }

        if (Function.STDDEV.equals(function)) {
            function = "STDDEV_SAMP";
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

        return String.format("RENAME TABLE %s TO %s", oldName.translate(), newName.translate());
    }

    @Override
    public String translate(RepeatDelimiter rd) {
        final String delimiter = rd.getDelimiter();

        final List<Object> all = rd.getExpressions().stream()
                .map(input -> {
                    inject(input);
                    return input.translate();
                })
                .collect(Collectors.toList());

        if (rd.isEnclosed()) {
            return "(" + StringUtils.join(all, delimiter) + ")";
        } else {
            return StringUtils.join(all, delimiter);
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
                querySelectColumns.add("(" + dbe.translate() + (!dbe.isAliased() ? ")" : ") AS " + quotize(dbe.getAlias(), translateEscape())));
            } else {
                querySelectColumns.add(dbe.translate() + (!dbe.isAliased() ? "" : " AS " + quotize(dbe.getAlias(), translateEscape())));
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
                    insideFrom.add("(" + dbe.translate() + (!dbe.isAliased() ? ")" : ") " + quotize(dbe.getAlias(), translateEscape())));
                } else {
                    insideFrom.add(dbe.translate() + (!dbe.isAliased() ? "" : " " + quotize(dbe.getAlias(), translateEscape())));
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

        // ORDER BY
        if (!orderbyColumns.isEmpty()) {
            query.add("ORDER BY");
            final List<String> queryOrderByColumns = new ArrayList<>();
            for (Expression column : orderbyColumns) {
                queryOrderByColumns.add(column.translate());
            }
            query.add(join(queryOrderByColumns, ", "));
        }

        // LIMIT AND OFFSET
        if (q.getLimit() > 0) {
            if (q.getOffset() > 0) {
                query.add(String.format("LIMIT %d,%d", q.getOffset(), q.getLimit()));
            } else {
                query.add(String.format("LIMIT %d", q.getLimit()));
            }
        }

        String finalQuery = join(query, " ");
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
        res.add(quotize(name, translateEscape()));
        res.add("AS " + as.translate());

        return StringUtils.join(res, " ");
    }

    @Override
    public String translate(DbColumn c) {
        switch (c.getDbColumnType()) {
            case BOOLEAN:
                return "BOOLEAN";

            case DOUBLE:
                return "DOUBLE PRECISION";

            case INT:
                return "INT";

            case LONG:
                return "BIGINT";

            case STRING:
                return format("VARCHAR(%s)", c.isSizeSet() ? c.getSize().toString() : properties.getProperty(VARCHAR_SIZE));

            case CLOB:
            case JSON:
                return "LONGTEXT";

            case BLOB:
                //return format("VARBINARY(%s)", properties.getProperty(MAX_BLOB_SIZE));
                return "LONGBLOB";

            default:
                throw new DatabaseEngineRuntimeException(format("Mapping not found for '%s'. Please report this error.", c.getDbColumnType()));
        }
    }

    @Override
    public String translate(final Cast cast) {
        throw new OperationNotSupportedRuntimeException("PDB does not support Cast in MySQL.");
    }

    @Override
    public String translate(final With with) {
        throw new OperationNotSupportedRuntimeException("MySQL does not support WITH.");
    }

    @Override
    protected Union rowsToUnion(final List<Expression> rows) {
        // By default, use UNION ALL to express VALUES.
        // MySQL does not support modeling UNION using binary tree due to a bug prior to 8.0
        // https://bugs.mysql.com/bug.php?id=25734
        return union(rows).all();
    }

    @Override
    public String translate(final StringAgg stringAgg) {
        inject(stringAgg.column);
        return String.format(
                "GROUP_CONCAT(%s %s SEPARATOR '%c')",
                stringAgg.isDistinct() ? "DISTINCT" : "",
                stringAgg.getColumn().translate(),
                stringAgg.getDelimiter()
        );
    }

    @Override
    public String translateEscape() {
        return "`";
    }

    @Override
    public String translateTrue() {
        return "1";
    }

    @Override
    public String translateFalse() {
        return "0";
    }

    @Override
    public String translateCreateTable(final DbEntity entity) {
        final List<String> createTable = new ArrayList<>();

        createTable.add("CREATE TABLE");
        createTable.add(quotize(entity.getName(), translateEscape()));
        final List<String> columns = new ArrayList<>();
        String autoIncName = "";
        // Remember that MySQL only supports one!
        int numberOfAutoIncs = 0;
        for (DbColumn c : entity.getColumns()) {
            final List<String> column = new ArrayList<>();
            column.add(quotize(c.getName(), translateEscape()));

            column.add(translate(c));

            /*
             * In MySQL only one column can be auto incremented and it must
             * be set as primary key.
             */
            if (c.isAutoInc()) {
                autoIncName = c.getName();
                column.add("AUTO_INCREMENT");
                numberOfAutoIncs++;
            }

            for (DbColumnConstraint cc : c.getColumnConstraints()) {
                column.add(cc.translate());
            }

            if (c.isDefaultValueSet()) {
                column.add("DEFAULT");
                column.add(translate(c.getDefaultValue()));
            }

            columns.add(join(column, " "));
        }

        if (numberOfAutoIncs > 1) {
            throw new DatabaseEngineRuntimeException("In MySQL you can only define one auto increment column");
        }

        String pks = "";
        if (numberOfAutoIncs == 1) {
            if (entity.getPkFields().size() == 0) {
                pks = ", PRIMARY KEY(" + autoIncName + ")";
            } else {

                pks = ", PRIMARY KEY(" + join(entity.getPkFields(), ", ") + ")";
            }
        }

        createTable.add("(" + join(columns, ", ") + pks + ")");

        return join(createTable, " ");
    }

    @Override
    public String translatePrimaryKeysConstraints(final DbEntity entity) {
        final List<String> pks = new ArrayList<>();
        for (String pk : entity.getPkFields()) {
            pks.add(quotize(pk, translateEscape()));
        }

        final String pkName = md5(format("PK_%s", entity.getName()), properties.getMaxIdentifierSize());

        final List<String> statement = new ArrayList<>();
        statement.add("ALTER TABLE");
        statement.add(quotize(entity.getName(), translateEscape()));
        statement.add("ADD CONSTRAINT");
        statement.add(quotize(pkName, translateEscape()));
        statement.add("PRIMARY KEY");
        statement.add("(" + join(pks, ", ") + ")");

        return join(statement, " ");
    }
}
