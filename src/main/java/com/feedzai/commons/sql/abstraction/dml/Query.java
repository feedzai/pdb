/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.dml;

import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.impl.MySqlEngine;
import com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder;
import com.feedzai.commons.sql.abstraction.util.StringUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * A Query representation.
 */
public class Query extends Expression {
    /**
     * The list of object in the select clause.
     */
    private final List<Expression> selectColumns = new ArrayList<Expression>();
    /**
     * The list of object in the from clause.
     */
    private final List<Expression> fromColumns = new ArrayList<Expression>();
    /**
     * The where clause.
     */
    private Expression where = null;
    /**
     * The list of objects in the group by clause.
     */
    private final List<Expression> groupbyColumns = new ArrayList<Expression>();
    /**
     * The having clause.
     */
    private Expression having = null;
    /**
     * The list of object in the order by clause.
     */
    private final List<Expression> orderbyColumns = new ArrayList<Expression>();
    /**
     * Limit the number of rows.
     */
    private Integer limit = 0;
    /**
     * Offset before the limit of rows.
     */
    private Integer offset = 0;
    /**
     * Signals the DISTINCT keyword.
     */
    private boolean distinct = false;

    /**
     * Creates a new instance of {@link Query}.
     */
    public Query() {
    }

    /**
     * Sets to distinct this query.
     *
     * @return This object.
     */
    public Query distinct() {
        this.distinct = true;

        return this;
    }

    /**
     * The select clause.
     *
     * @param selectColumns The objects.
     * @return This object.
     */
    public Query select(final Expression... selectColumns) {
        if (selectColumns == null) {
            return this;
        }

        this.selectColumns.addAll(Arrays.asList(selectColumns));

        return this;
    }

    /**
     * The select clause.
     *
     * @param selectColumns The collection of select columns.
     * @return This object.
     */
    public Query select(final Collection<? extends Expression> selectColumns) {
        this.selectColumns.addAll(selectColumns);

        return this;
    }

    /**
     * The from clause.
     *
     * @param fromColumns The objects.
     * @return This object.
     */
    public Query from(final Expression... fromColumns) {
        if (fromColumns == null) {
            return this;
        }

        this.fromColumns.addAll(Arrays.asList(fromColumns));

        return this;
    }

    /**
     * The from clause.
     *
     * @param fromColumns The from columns.
     * @return This object.
     */
    public Query from(final Collection<? extends Expression> fromColumns) {
        if (fromColumns == null) {
            return this;
        }

        this.fromColumns.addAll(fromColumns);

        return this;
    }

    /**
     * The where clause.
     * <p/>
     * If there is no where clause already defined, sets to the defined value.
     * <p/>
     * If there is already an expression for where defined, defines an <code>and</code> expression between old and new clauses.
     * <p/>
     * Example:
     * <pre>
     *     query.where(eq(column("col1"),k(1)).andWhere(eq(column("col2"),k(2))
     *
     *     will be translated to
     *
     *     ( "col1" = 1 ) AND ( "col2" = 2 )
     * </pre>
     *
     * @param where The object.
     * @return This object.
     */
    public Query andWhere(final Expression where) {
        if (this.where == null) {
            this.where = where;
        } else {
            this.where = SqlBuilder.and(this.where.enclose(), where.enclose());
        }

        return this;
    }

    /**
     * The where clause.
     * <b>Note:</b>This method replaces any <code>where</code> clauses previously defined.
     *
     * @param where The object.
     * @return This object.
     */
    public Query where(final Expression where) {
        this.where = where;

        return this;
    }

    /**
     * The group by clause.
     *
     * @param groupbyColumns The objects.
     * @return This object.
     */
    public Query groupby(final Expression... groupbyColumns) {
        if (groupbyColumns == null) {
            return this;
        }

        this.groupbyColumns.addAll(Arrays.asList(groupbyColumns));

        return this;
    }

    /**
     * The group by clause.
     *
     * @param groupbyColumns The group by columns.
     * @return This object.
     */
    public Query groupby(final Collection<? extends Expression> groupbyColumns) {
        if (groupbyColumns == null) {
            return this;
        }

        this.groupbyColumns.addAll(groupbyColumns);

        return this;
    }

    /**
     * The having clause.
     *
     * @param having The object.
     * @return This object.
     */
    public Query having(final Expression having) {
        this.having = having;

        return this;
    }

    /**
     * The order by clause.
     *
     * @param orderbyColumns The objects.
     * @return This object.
     */
    public Query orderby(final Expression... orderbyColumns) {
        if (orderbyColumns == null) {
            return this;
        }

        this.orderbyColumns.addAll(Arrays.asList(orderbyColumns));

        return this;
    }

    /**
     * The order by clause.
     *
     * @param orderbyColumns The order by columns.
     * @return This object.
     */
    public Query orderby(final Collection<? extends Expression> orderbyColumns) {
        if (orderbyColumns == null) {
            return this;
        }

        this.orderbyColumns.addAll(orderbyColumns);

        return this;
    }

    /**
     * The limit clause.
     *
     * @param limit The number of rows that the query returns.
     * @return This object.
     */
    public Query limit(final Integer limit) {
        this.limit = limit;

        return this;
    }

    /**
     * The offset clause.
     *
     * @param offset The start position
     * @return This object.
     */
    public Query offset(final Integer offset) {
        this.offset = offset;

        /** If we defined offset without limit, force a limit to handle MySQL and others gracefully */
        if (limit <= 0) {
            limit = Integer.MAX_VALUE - offset;
        }

        return this;
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        final List<String> query = new ArrayList<String>();

        // SELECT CLAUSE
        query.add("SELECT" + (distinct ? " DISTINCT" : ""));
        final List<String> querySelectColumns = new ArrayList<String>();
        for (Expression dbe : selectColumns) {
            if (dbe instanceof Query) {
                querySelectColumns.add("(" + dbe.translateDB2(properties) + (dbe.alias == null ? ")" : ") AS " + StringUtil.quotize(dbe.alias)));
            } else {
                querySelectColumns.add(dbe.translateDB2(properties) + (dbe.alias == null ? "" : " AS " + StringUtil.quotize(dbe.alias)));
            }
        }
        query.add(StringUtil.join(querySelectColumns, ", "));

        // FROM CLAUSE
        query.add("FROM");
        if (!fromColumns.isEmpty()) {
            final List<String> queryFromColumns = new ArrayList<String>();
            for (Expression dbe : fromColumns) {
                final List<String> insideFrom = new ArrayList<String>();
                if (dbe instanceof Query) {
                    insideFrom.add("(" + dbe.translateDB2(properties) + (dbe.alias == null ? ")" : ") " + StringUtil.quotize(dbe.alias)));
                } else {
                    insideFrom.add(dbe.translateDB2(properties) + (dbe.alias == null ? "" : " " + StringUtil.quotize(dbe.alias)));
                }


                if (!dbe.joins.isEmpty()) {
                    for (Join j : dbe.joins) {
                        insideFrom.add(j.translateDB2(properties));
                    }
                }

                queryFromColumns.add(StringUtil.join(insideFrom, " "));
            }
            query.add(StringUtil.join(queryFromColumns, ", "));
        } else {
            query.add("sysibm.sysdummy1");
        }

        // WHERE CLAUSE
        if (where != null) {
            query.add("WHERE");
            query.add(where.translateDB2(properties));
        }

        // GROUP BY CLAUSE
        if (!groupbyColumns.isEmpty()) {
            query.add("GROUP BY");
            final List<String> queryGroupByColumns = new ArrayList<String>();
            for (Expression column : groupbyColumns) {
                queryGroupByColumns.add(column.translateDB2(properties));
            }
            query.add(StringUtil.join(queryGroupByColumns, ", "));
        }

        // HAVING CLAUSE
        if (having != null) {
            query.add("HAVING");
            query.add(having.translateDB2(properties));
        }

        // ORDER BY
        if (!orderbyColumns.isEmpty()) {
            query.add("ORDER BY");
            final List<String> queryOrderByColumns = new ArrayList<String>();
            for (Expression column : orderbyColumns) {
                queryOrderByColumns.add(column.translateDB2(properties));
            }
            query.add(StringUtil.join(queryOrderByColumns, ", "));
        }

        String finalQuery = StringUtil.join(query, " ");

        // LIMIT AND OFFSET
        if (limit > 0) {
            if (offset > 0) {
                finalQuery = String.format("SELECT * FROM (SELECT ROW_NUMBER() OVER() rnum ,offlim.* FROM (%s) offlim) WHERE rnum <= %d AND rnum > %d", finalQuery, limit + offset, offset);
            } else {
                finalQuery = String.format("SELECT * FROM (%s) FETCH FIRST %d ROWS ONLY", finalQuery, limit);
            }
        }

        return isEnclosed() ? ("(" + finalQuery + ")") : finalQuery;
    }

    @Override
    public String translateOracle(final PdbProperties properties) {
        final List<String> query = new ArrayList<String>();

        // SELECT CLAUSE
        query.add("SELECT" + (distinct ? " DISTINCT" : ""));
        final List<String> querySelectColumns = new ArrayList<String>();
        for (Expression dbe : selectColumns) {
            if (dbe instanceof Query) {
                querySelectColumns.add("(" + dbe.translateOracle(properties) + (dbe.alias == null ? ")" : ") AS " + StringUtil.quotize(dbe.alias)));
            } else {
                querySelectColumns.add(dbe.translateOracle(properties) + (dbe.alias == null ? "" : " AS " + StringUtil.quotize(dbe.alias)));
            }
        }
        query.add(StringUtil.join(querySelectColumns, ", "));

        // FROM CLAUSE
        query.add("FROM");
        if (!fromColumns.isEmpty()) {
            final List<String> queryFromColumns = new ArrayList<String>();
            for (Expression dbe : fromColumns) {
                final List<String> insideFrom = new ArrayList<String>();
                if (dbe instanceof Query) {
                    insideFrom.add("(" + dbe.translateOracle(properties) + (dbe.alias == null ? ")" : ") " + StringUtil.quotize(dbe.alias)));
                } else {
                    insideFrom.add(dbe.translateOracle(properties) + (dbe.alias == null ? "" : " " + StringUtil.quotize(dbe.alias)));
                }


                if (!dbe.joins.isEmpty()) {
                    for (Join j : dbe.joins) {
                        insideFrom.add(j.translateOracle(properties));
                    }
                }

                queryFromColumns.add(StringUtil.join(insideFrom, " "));
            }
            query.add(StringUtil.join(queryFromColumns, ", "));
        } else {
            query.add(StringUtil.quotize("DUAL"));
        }

        // WHERE CLAUSE
        if (where != null) {
            query.add("WHERE");
            query.add(where.translateOracle(properties));
        }

        // GROUP BY CLAUSE
        if (!groupbyColumns.isEmpty()) {
            query.add("GROUP BY");
            final List<String> queryGroupByColumns = new ArrayList<String>();
            for (Expression column : groupbyColumns) {
                queryGroupByColumns.add(column.translateOracle(properties));
            }
            query.add(StringUtil.join(queryGroupByColumns, ", "));
        }

        // HAVING CLAUSE
        if (having != null) {
            query.add("HAVING");
            query.add(having.translateOracle(properties));
        }

        // ORDER BY
        if (!orderbyColumns.isEmpty()) {
            query.add("ORDER BY");
            final List<String> queryOrderByColumns = new ArrayList<String>();
            for (Expression column : orderbyColumns) {
                queryOrderByColumns.add(column.translateOracle(properties));
            }
            query.add(StringUtil.join(queryOrderByColumns, ", "));
        }

        String finalQuery = StringUtil.join(query, " ");

        // LIMIT AND OFFSET
        if (limit > 0) {
            if (offset > 0) {
                finalQuery = String.format("SELECT * FROM (SELECT rownum rnum ,offlim.* FROM (%s) offlim WHERE rownum <= %d) WHERE rnum > %d", finalQuery, limit + offset, offset);
            } else {
                finalQuery = String.format("SELECT * FROM (%s) a WHERE rownum <= %d", finalQuery, limit);
            }
        }

        return isEnclosed() ? ("(" + finalQuery + ")") : finalQuery;
    }

    @Override
    public String translateMySQL(final PdbProperties properties) {
        final List<String> query = new ArrayList<String>();

        // SELECT CLAUSE
        query.add("SELECT" + (distinct ? " DISTINCT" : ""));
        final List<String> querySelectColumns = new ArrayList<String>();
        for (Expression dbe : selectColumns) {
            if (dbe instanceof Query) {
                querySelectColumns.add("(" + dbe.translateMySQL(properties) + (dbe.alias == null ? ")" : ") AS " + StringUtil.quotize(dbe.alias, MySqlEngine.ESCAPE_CHARACTER)));
            } else {
                querySelectColumns.add(dbe.translateMySQL(properties) + (dbe.alias == null ? "" : " AS " + StringUtil.quotize(dbe.alias, MySqlEngine.ESCAPE_CHARACTER)));
            }
        }
        query.add(StringUtil.join(querySelectColumns, ", "));

        // FROM CLAUSE
        if (!fromColumns.isEmpty()) {
            query.add("FROM");
            final List<String> queryFromColumns = new ArrayList<String>();
            for (Expression dbe : fromColumns) {
                final List<String> insideFrom = new ArrayList<String>();
                if (dbe instanceof Query) {
                    insideFrom.add("(" + dbe.translateMySQL(properties) + (dbe.alias == null ? ")" : ") " + StringUtil.quotize(dbe.alias, MySqlEngine.ESCAPE_CHARACTER)));
                } else {
                    insideFrom.add(dbe.translateMySQL(properties) + (dbe.alias == null ? "" : " " + StringUtil.quotize(dbe.alias, MySqlEngine.ESCAPE_CHARACTER)));
                }


                if (!dbe.joins.isEmpty()) {
                    for (Join j : dbe.joins) {
                        insideFrom.add(j.translateMySQL(properties));
                    }
                }

                queryFromColumns.add(StringUtil.join(insideFrom, " "));
            }
            query.add(StringUtil.join(queryFromColumns, ", "));
        }

        // WHERE CLAUSE
        if (where != null) {
            query.add("WHERE");
            query.add(where.translateMySQL(properties));
        }

        // GROUP BY CLAUSE
        if (!groupbyColumns.isEmpty()) {
            query.add("GROUP BY");
            final List<String> queryGroupByColumns = new ArrayList<String>();
            for (Expression column : groupbyColumns) {
                queryGroupByColumns.add(column.translateMySQL(properties));
            }
            query.add(StringUtil.join(queryGroupByColumns, ", "));
        }

        // HAVING CLAUSE
        if (having != null) {
            query.add("HAVING");
            query.add(having.translateMySQL(properties));
        }

        // ORDER BY
        if (!orderbyColumns.isEmpty()) {
            query.add("ORDER BY");
            final List<String> queryOrderByColumns = new ArrayList<String>();
            for (Expression column : orderbyColumns) {
                queryOrderByColumns.add(column.translateMySQL(properties));
            }
            query.add(StringUtil.join(queryOrderByColumns, ", "));
        }

        // LIMIT AND OFFSET
        if (limit > 0) {
            if (offset > 0) {
                query.add(String.format("LIMIT %d,%d", offset, limit));
            } else {
                query.add(String.format("LIMIT %d", limit));
            }
        }

        String finalQuery = StringUtil.join(query, " ");
        return isEnclosed() ? ("(" + finalQuery + ")") : finalQuery;
    }

    @Override
    public String translateSQLServer(final PdbProperties properties) {
        final List<String> query = new ArrayList<String>();

        // SELECT CLAUSE
        // SQL Server does not have a pretty limit/offset syntax, though if is only a limit, we use top, otherwise we will use the ROW_NUM approach
        query.add("SELECT" + (distinct ? " DISTINCT" : "") + ((limit <= 0 || offset > 0) ? "" : " TOP " + limit));
        final List<String> querySelectColumns = new ArrayList<String>();
        for (Expression dbe : selectColumns) {
            if (dbe instanceof Query) {
                querySelectColumns.add("(" + dbe.translateSQLServer(properties) + (dbe.alias == null ? ")" : ") AS " + StringUtil.quotize(dbe.alias)));
            } else {
                querySelectColumns.add(dbe.translateSQLServer(properties) + (dbe.alias == null ? "" : " AS " + StringUtil.quotize(dbe.alias)));
            }
        }
        query.add(StringUtil.join(querySelectColumns, ", "));

        // FROM CLAUSE
        if (!fromColumns.isEmpty()) {
            query.add("FROM");
            final List<String> queryFromColumns = new ArrayList<String>();
            for (Expression dbe : fromColumns) {
                final List<String> insideFrom = new ArrayList<String>();
                if (dbe instanceof Query) {
                    insideFrom.add("(" + dbe.translateSQLServer(properties) + (dbe.alias == null ? ")" : ") " + StringUtil.quotize(dbe.alias)));
                } else {
                    insideFrom.add(dbe.translateSQLServer(properties) + (dbe.alias == null ? "" : " " + StringUtil.quotize(dbe.alias)) + (dbe.isWithNoLock() ? " WITH(NOLOCK)" : ""));
                }


                if (!dbe.joins.isEmpty()) {
                    for (Join j : dbe.joins) {
                        insideFrom.add(j.translateSQLServer(properties));
                    }
                }

                queryFromColumns.add(StringUtil.join(insideFrom, " "));
            }
            query.add(StringUtil.join(queryFromColumns, ", "));
        }

        // WHERE CLAUSE
        if (where != null) {
            query.add("WHERE");
            query.add(where.translateSQLServer(properties));
        }

        // GROUP BY CLAUSE
        if (!groupbyColumns.isEmpty()) {
            query.add("GROUP BY");
            final List<String> queryGroupByColumns = new ArrayList<String>();
            for (Expression column : groupbyColumns) {
                queryGroupByColumns.add(column.translateSQLServer(properties));
            }
            query.add(StringUtil.join(queryGroupByColumns, ", "));
        }

        // HAVING CLAUSE
        if (having != null) {
            query.add("HAVING");
            query.add(having.translateSQLServer(properties));
        }

        // ORDER BY - Only place order by here if there is no offset defined
        if (offset <= 0 && !orderbyColumns.isEmpty()) {
            query.add("ORDER BY");
            final List<String> queryOrderByColumns = new ArrayList<String>();
            for (Expression column : orderbyColumns) {
                queryOrderByColumns.add(column.translateSQLServer(properties));
            }
            query.add(StringUtil.join(queryOrderByColumns, ", "));
        }

        String finalQuery = StringUtil.join(query, " ");

        // LIMIT AND OFFSET
        if (limit > 0 && offset > 0) {
            String orderByClause;

            /** When there is an order by we must place it onside both the inner and outer query */
            if (!orderbyColumns.isEmpty()) {
                final List<String> queryOrderByColumns = new ArrayList<String>();
                for (Expression column : orderbyColumns) {
                    queryOrderByColumns.add(column.translateSQLServer(properties));
                }
                orderByClause = StringUtil.join(queryOrderByColumns, ", ");
                finalQuery = String.format("SELECT * FROM (SELECT ROW_NUMBER() OVER(ORDER BY %s) rnum ,offlim.* FROM (%s) offlim) offlim WHERE rnum <= %d and rnum > %d ORDER BY %s",
                        orderByClause, finalQuery, limit + offset, offset, orderByClause);
            } else {
                finalQuery = String.format("SELECT * FROM (SELECT ROW_NUMBER() OVER(ORDER BY (SELECT 1)) rnum ,offlim.* FROM (%s) offlim) offlim WHERE rnum <= %d and rnum > %d",
                        finalQuery, limit + offset, offset);
            }
        }

        return isEnclosed() ? ("(" + finalQuery + ")") : finalQuery;
    }

    @Override
    public String translatePostgreSQL(final PdbProperties properties) {
        final List<String> query = new ArrayList<String>();

        // SELECT CLAUSE
        query.add("SELECT" + (distinct ? " DISTINCT" : ""));
        final List<String> querySelectColumns = new ArrayList<String>();
        for (Expression dbe : selectColumns) {
            if (dbe instanceof Query) {
                querySelectColumns.add("(" + dbe.translatePostgreSQL(properties) + (dbe.alias == null ? ")" : ") AS " + StringUtil.quotize(dbe.alias)));
            } else {
                querySelectColumns.add(dbe.translatePostgreSQL(properties) + (dbe.alias == null ? "" : " AS " + StringUtil.quotize(dbe.alias)));
            }
        }
        query.add(StringUtil.join(querySelectColumns, ", "));

        // FROM CLAUSE
        if (!fromColumns.isEmpty()) {
            query.add("FROM");
            final List<String> queryFromColumns = new ArrayList<String>();
            for (Expression dbe : fromColumns) {
                final List<String> insideFrom = new ArrayList<String>();
                if (dbe instanceof Query) {
                    insideFrom.add("(" + dbe.translatePostgreSQL(properties) + (dbe.alias == null ? ")" : ") " + StringUtil.quotize(dbe.alias)));
                } else {
                    insideFrom.add(dbe.translatePostgreSQL(properties) + (dbe.alias == null ? "" : " " + StringUtil.quotize(dbe.alias)));
                }


                if (!dbe.joins.isEmpty()) {
                    for (Join j : dbe.joins) {
                        insideFrom.add(j.translatePostgreSQL(properties));
                    }
                }

                queryFromColumns.add(StringUtil.join(insideFrom, " "));
            }
            query.add(StringUtil.join(queryFromColumns, ", "));
        }

        // WHERE CLAUSE
        if (where != null) {
            query.add("WHERE");
            query.add(where.translatePostgreSQL(properties));
        }

        // GROUP BY CLAUSE
        if (!groupbyColumns.isEmpty()) {
            query.add("GROUP BY");
            final List<String> queryGroupByColumns = new ArrayList<String>();
            for (Expression column : groupbyColumns) {
                queryGroupByColumns.add(column.translatePostgreSQL(properties));
            }
            query.add(StringUtil.join(queryGroupByColumns, ", "));
        }

        // HAVING CLAUSE
        if (having != null) {
            query.add("HAVING");
            query.add(having.translatePostgreSQL(properties));
        }

        // ORDER BY
        if (!orderbyColumns.isEmpty()) {
            query.add("ORDER BY");
            final List<String> queryOrderByColumns = new ArrayList<String>();
            for (Expression column : orderbyColumns) {
                queryOrderByColumns.add(column.translatePostgreSQL(properties));
            }
            query.add(StringUtil.join(queryOrderByColumns, ", "));
        }

        // LIMIT AND OFFSET
        if (limit > 0) {
            if (offset > 0) {
                query.add(String.format("LIMIT %d OFFSET %d", limit, offset));
            } else {
                query.add(String.format("LIMIT %d", limit));
            }
        }

        String finalQuery = StringUtil.join(query, " ");
        return isEnclosed() ? ("(" + finalQuery + ")") : finalQuery;
    }

    @Override
    public String translateH2(PdbProperties properties) {
        final List<String> query = new ArrayList<String>();

        // SELECT CLAUSE
        query.add("SELECT" + (distinct ? " DISTINCT" : ""));
        final List<String> querySelectColumns = new ArrayList<String>();
        for (Expression dbe : selectColumns) {
            if (dbe instanceof Query) {
                querySelectColumns.add("(" + dbe.translateH2(properties) + (dbe.alias == null ? ")" : ") AS " + StringUtil.quotize(dbe.alias)));
            } else {
                querySelectColumns.add(dbe.translateH2(properties) + (dbe.alias == null ? "" : " AS " + StringUtil.quotize(dbe.alias)));
            }
        }
        query.add(StringUtil.join(querySelectColumns, ", "));

        // FROM CLAUSE
        if (!fromColumns.isEmpty()) {
            query.add("FROM");
            final List<String> queryFromColumns = new ArrayList<String>();
            for (Expression dbe : fromColumns) {
                final List<String> insideFrom = new ArrayList<String>();
                if (dbe instanceof Query) {
                    insideFrom.add("(" + dbe.translateH2(properties) + (dbe.alias == null ? ")" : ") " + StringUtil.quotize(dbe.alias)));
                } else {
                    insideFrom.add(dbe.translateH2(properties) + (dbe.alias == null ? "" : " " + StringUtil.quotize(dbe.alias)));
                }


                if (!dbe.joins.isEmpty()) {
                    for (Join j : dbe.joins) {
                        insideFrom.add(j.translatePostgreSQL(properties));
                    }
                }

                queryFromColumns.add(StringUtil.join(insideFrom, " "));
            }
            query.add(StringUtil.join(queryFromColumns, ", "));
        }

        // WHERE CLAUSE
        if (where != null) {
            query.add("WHERE");
            query.add(where.translateH2(properties));
        }

        // GROUP BY CLAUSE
        if (!groupbyColumns.isEmpty()) {
            query.add("GROUP BY");
            final List<String> queryGroupByColumns = new ArrayList<String>();
            for (Expression column : groupbyColumns) {
                queryGroupByColumns.add(column.translateH2(properties));
            }
            query.add(StringUtil.join(queryGroupByColumns, ", "));
        }

        // HAVING CLAUSE
        if (having != null) {
            query.add("HAVING");
            query.add(having.translateH2(properties));
        }

        // ORDER BY
        if (!orderbyColumns.isEmpty()) {
            query.add("ORDER BY");
            final List<String> queryOrderByColumns = new ArrayList<String>();
            for (Expression column : orderbyColumns) {
                queryOrderByColumns.add(column.translateH2(properties));
            }
            query.add(StringUtil.join(queryOrderByColumns, ", "));
        }

        // LIMIT AND OFFSET
        if (limit > 0) {
            if (offset > 0) {
                query.add(String.format("LIMIT %d OFFSET %d", limit, offset));
            } else {
                query.add(String.format("LIMIT %d", limit));
            }
        }

        String finalQuery = StringUtil.join(query, " ");
        return isEnclosed() ? ("(" + finalQuery + ")") : finalQuery;
    }
}
