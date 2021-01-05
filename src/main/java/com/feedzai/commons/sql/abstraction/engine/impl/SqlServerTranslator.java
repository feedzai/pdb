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
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.ddl.DbFk;
import com.feedzai.commons.sql.abstraction.ddl.DbIndex;
import com.feedzai.commons.sql.abstraction.ddl.DropPrimaryKey;
import com.feedzai.commons.sql.abstraction.ddl.Rename;
import com.feedzai.commons.sql.abstraction.dml.Cast;
import com.feedzai.commons.sql.abstraction.dml.Expression;
import com.feedzai.commons.sql.abstraction.dml.Function;
import com.feedzai.commons.sql.abstraction.dml.Join;
import com.feedzai.commons.sql.abstraction.dml.Modulo;
import com.feedzai.commons.sql.abstraction.dml.Name;
import com.feedzai.commons.sql.abstraction.dml.Query;
import com.feedzai.commons.sql.abstraction.dml.RepeatDelimiter;
import com.feedzai.commons.sql.abstraction.dml.StringAgg;
import com.feedzai.commons.sql.abstraction.dml.Update;
import com.feedzai.commons.sql.abstraction.dml.View;
import com.feedzai.commons.sql.abstraction.engine.AbstractTranslator;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.feedzai.commons.sql.abstraction.engine.OperationNotSupportedRuntimeException;
import com.feedzai.commons.sql.abstraction.util.StringUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.all;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.column;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.MAX_BLOB_SIZE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.VARCHAR_SIZE;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.md5;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.join;

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

        List<Object> trans = Lists.transform(column.getColumnConstraints(), DbColumnConstraint::translate);

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

        if (Function.CEILING.equals(function)) {
            function = "CEILING";
        }

        // if it is a user-defined function
        if (f.isUDF()) {
            // a schema must always be used for functions, use default SQL Server schema "dbo" if no schema is set
            final String schema = properties.isSchemaSet() ? quotize(properties.getSchema(), translateEscape())  : "dbo";
            return schema + "." + function + "(" + expTranslated + ")";
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
                    final Expression parsedColumn = getParsedOrderByColumn(column);
                    queryOrderByColumns.add(parsedColumn.translate());
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

        final List<String> res = new ArrayList<>();

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
                    return "VARBINARY(MAX)";
                }


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
                type = "BIT";
                break;
            case DOUBLE:
                type = "DOUBLE PRECISION";
                break;
            case INT:
                type = "INT";
                break;
            case LONG:
                type = "BIGINT";
                break;
            case STRING:
                type = "NVARCHAR";
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
        if (stringAgg.isDistinct()) {
            throw new OperationNotSupportedRuntimeException("STRING_AGG does not support distinct. If you really need it, " +
                                                             "you may do it using a subquery. " +
                                                             "Check this: https://stackoverflow.com/a/50589222");
        }
        inject(stringAgg.column);
        return String.format(
                "STRING_AGG(%s, '%c')",
                stringAgg.getColumn().translate(),
                stringAgg.getDelimiter()
        );
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

    @Override
    public String translateCreateTable(final DbEntity entity) {
        final List<String> createTable = new ArrayList<>();

        createTable.add("CREATE TABLE");
        createTable.add(quotize(entity.getName()));
        final List<String> columns = new ArrayList<>();

        int numberOfAutoIncs = 0;
        for (DbColumn c : entity.getColumns()) {
            final List<String> column = new ArrayList<>();
            column.add(quotize(c.getName()));

            column.add(translate(c));

            if (c.isAutoInc()) {
                column.add("IDENTITY");
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
            throw new DatabaseEngineRuntimeException("In SQLServer you can only define one auto increment column");
        }

        createTable.add("(" + join(columns, ", ") + ")");

        return join(createTable, " ");
    }

    @Override
    public String translatePrimaryKeysNotNull(final DbEntity entity) {
        final List<String> notNull = new ArrayList<>();
        for (final String pk : entity.getPkFields()) {
            DbColumn toAlter = null;
            for (DbColumn col : entity.getColumns()) {
                if (col.getName().equals(pk)) {
                    toAlter = col;
                    break;
                }
            }

            if (toAlter == null) {
                throw new DatabaseEngineRuntimeException("The column you specified for Primary Key does not exist.");
            } else {
                boolean isNotNull = false;
                final List<String> cons = new ArrayList<>();
                cons.add(quotize(toAlter.getName()));
                cons.add(translate(toAlter));
                for (DbColumnConstraint cc : toAlter.getColumnConstraints()) {
                    if (cc == DbColumnConstraint.NOT_NULL) {
                        isNotNull = true;
                    }

                    cons.add(cc.translate());
                }

                if (!isNotNull) {
                    cons.add(DbColumnConstraint.NOT_NULL.translate());

                    final String alterTable = format("ALTER TABLE %s ALTER COLUMN %s",
                                                     quotize(entity.getName()), join(cons, " "));
                    notNull.add(alterTable);
                }
            }
        }

        return join(notNull, " ; ");
    }

    @Override
    public String translatePrimaryKeysConstraints(final DbEntity entity) {
        final List<String> pks = new ArrayList<>();
        for (final String pk : entity.getPkFields()) {
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

        for (final DbFk fk : entity.getFks()) {
            final List<String> quotizedLocalColumns = new ArrayList<>();
            for (String s : fk.getLocalColumns()) {
                quotizedLocalColumns.add(quotize(s));
            }

            final List<String> quotizedForeignColumns = new ArrayList<>();
            for (final String s : fk.getForeignColumns()) {
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
        final List<String> createIndexes = new ArrayList<>();
        final List<DbIndex> indexes = entity.getIndexes();

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

    /**
     * Helper method which removes the environment parameter from a column when it is used inside an order by statement
     * and in a paginated query. This is needed in order to avoid "The multi-part identifier could not be bound" error
     * which only happens in SQL Server.
     *
     * @param column The column that will be parsed.
     * @return The new column which does not contain any environment value.
     * @since 2.1.13
     */
    private Expression getParsedOrderByColumn(final Expression column) {
        if (column instanceof Name) {
            final Name columnName = (Name) column;
            final String environment = columnName.getEnvironment();

            if (environment != null && !environment.isEmpty()) {
                final Expression parsedColumn = column(columnName.getName());
                inject(parsedColumn);
                switch (column.getOrdering()) {
                    case "DESC":
                        parsedColumn.desc();
                        break;
                    default:
                        parsedColumn.asc();
                        break;
                }

                return parsedColumn;
            }
        }

        return column;
    }
}
