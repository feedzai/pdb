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
import com.feedzai.commons.sql.abstraction.engine.OperationNotSupportedRuntimeException;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.VARCHAR_SIZE;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.md5;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * Provides SQL translation for H2.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class H2Translator extends AbstractTranslator {

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
    public String translate(Function f) {
        final Expression exp = f.getExp();
        final String function = f.getFunction();
        inject(exp);

        String expTranslated = "";

        if (exp != null) {
            expTranslated = exp.translate();
        }

        if (Function.AVG.equals(function)) {
            expTranslated = String.format("CONVERT(%s, DOUBLE PRECISION)", expTranslated);
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
        final List<Expression> exps = rd.getExpressions();

        List<String> all = new ArrayList<>();
        final Expression expression = exps.get(0);
        inject(expression);

        if (RepeatDelimiter.DIV.equals(delimiter)) {
            all.add(String.format("CAST(%s AS DOUBLE PRECISION)", expression.translate()));
        } else {
            all.add(expression.translate());

        }

        for (int i = 1; i < exps.size(); i++) {
            final Expression nthExpression = exps.get(i);
            inject(nthExpression);
            all.add(nthExpression.translate());
        }

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
                query.add(String.format("LIMIT %d OFFSET %d", q.getLimit(), q.getOffset()));
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
        res.add(quotize(name));
        res.add("AS " + as.translate());

        return StringUtils.join(res, " ");
    }

    @Override
    public String translate(DbColumn c) {
        switch (c.getDbColumnType()) {
            case BOOLEAN:
                return "BOOLEAN";

            case DOUBLE:
                return "DOUBLE";

            case INT:
                if (c.isAutoInc()) {
                    return "INTEGER AUTO_INCREMENT";
                } else {
                    return "INTEGER";
                }

            case LONG:
                if (c.isAutoInc()) {
                    return "IDENTITY";
                } else {
                    return "BIGINT";
                }

            case STRING:
                return format("VARCHAR(%s)", c.isSizeSet() ? c.getSize().toString() : properties.getProperty(VARCHAR_SIZE));

            case BLOB:
                return "BLOB";

            case CLOB:
            case JSON:
                return "CLOB";

            default:
                throw new DatabaseEngineRuntimeException(format("Mapping not found for '%s'. Please report this error.", c.getDbColumnType()));
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
                type = "DOUBLE";
                break;
            case INT:
                type = "INTEGER";
                break;
            case LONG:
                type = "BIGINT";
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
    public String translate(final StringAgg stringAgg) {
        inject(stringAgg.column);
        return String.format(
                "STRING_AGG(%s %s, '%c')",
                stringAgg.isDistinct() ? "DISTINCT" : "",
                stringAgg.getColumn().translate(),
                stringAgg.getDelimiter()
        );
    }

    @Override
    public String translateEscape() {
        return "\"";
    }

    @Override
    public String translateTrue() {
        return "TRUE";
    }

    @Override
    public String translateFalse() {
        return "FALSE";
    }

    @Override
    public String translateCreateTable(final DbEntity entity) {
        final List<String> createTable = new ArrayList<>();

        createTable.add("CREATE TABLE");
        createTable.add(quotize(entity.getName()));
        final List<String> columns = new ArrayList<>();
        final List<String> pkFields = entity.getPkFields();
        for (DbColumn c : entity.getColumns()) {
            final List<String> column = new ArrayList<>();
            column.add(quotize(c.getName()));
            column.add(translate(c));

            // If this column is PK, it must be forced to be NOT NULL (only if it's not already...)
            if (pkFields.contains(c.getName()) && !c.getColumnConstraints().contains(DbColumnConstraint.NOT_NULL)) {
                // Create a NOT NULL constraint
                c.getColumnConstraints().add(DbColumnConstraint.NOT_NULL);
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
        createTable.add("(" + join(columns, ", ") + ")");

        return join(createTable, " ");
    }

    @Override
    public String translatePrimaryKeysConstraints(final DbEntity entity) {
        final List<String> pks = new ArrayList<>();
        for (String pk : entity.getPkFields()) {
            pks.add(quotize(pk));
        }

        final String pkName = md5(format("PK_%s", entity.getName()), properties.getMaxIdentifierSize());

        final List<String> statement = new ArrayList<>();
        statement.add("ALTER TABLE");
        statement.add(quotize(entity.getName()));
        statement.add("ADD CONSTRAINT");
        statement.add(quotize(pkName));
        statement.add("PRIMARY KEY");
        statement.add("(" + join(pks, ", ") + ")");

        return join(statement, " ");
    }

    @Override
    public List<String> translateForeignKey(final DbEntity entity) {
        final List<String> alterTables = new ArrayList<>();

        for (DbFk fk : entity.getFks()) {
            final List<String> quotizedLocalColumns = new ArrayList<>();
            for (String s : fk.getLocalColumns()) {
                quotizedLocalColumns.add(quotize(s));
            }

            final List<String> quotizedForeignColumns = new ArrayList<>();
            for (String s : fk.getForeignColumns()) {
                quotizedForeignColumns.add(quotize(s));
            }

            final String table = quotize(entity.getName());
            final String quotizedLocalColumnsSting = join(quotizedLocalColumns, ", ");
            final String quotizedForeignColumnsString = join(quotizedForeignColumns, ", ");

            final String alterTable =
                    format("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
                            table,
                            quotize(md5("FK_" + table + quotizedLocalColumnsSting + quotizedForeignColumnsString,
                                        properties.getMaxIdentifierSize()
                            )),
                            quotizedLocalColumnsSting,
                            quotize(fk.getForeignTable()),
                            quotizedForeignColumnsString
            );

            alterTables.add(alterTable);
        }

        return alterTables;
    }

    @Override
    public List<String> translateCreateIndexes(final DbEntity entity) {
        final List<DbIndex> indexes = entity.getIndexes();
        final List<String> createIndexes = new ArrayList<>();

        for (final DbIndex index : indexes) {

            final List<String> createIndex = new ArrayList<>();
            createIndex.add("CREATE");
            if (index.isUnique()) {
                createIndex.add("UNIQUE");
            }
            createIndex.add("INDEX");

            final List<String> columns = new ArrayList<>();
            final List<String> columnsForName = new ArrayList<>();
            for (final String column : index.getColumns()) {
                columns.add(quotize(column));
                columnsForName.add(column);
            }

            final String idxName = md5(format("%s_%s_IDX", entity.getName(),
                                              join(columnsForName, "_")), properties.getMaxIdentifierSize());
            createIndex.add(quotize(idxName));
            createIndex.add("ON");
            createIndex.add(quotize(entity.getName()));
            createIndex.add("(" + join(columns, ", ") + ")");

            final String statement = join(createIndex, " ");
            createIndexes.add(statement);
        }
        return createIndexes;
    }
}
