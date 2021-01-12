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
package com.feedzai.commons.sql.abstraction.engine;

import com.feedzai.commons.sql.abstraction.ddl.AlterColumn;
import com.feedzai.commons.sql.abstraction.ddl.DbColumn;
import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.ddl.DropPrimaryKey;
import com.feedzai.commons.sql.abstraction.ddl.Rename;
import com.feedzai.commons.sql.abstraction.dml.Between;
import com.feedzai.commons.sql.abstraction.dml.Case;
import com.feedzai.commons.sql.abstraction.dml.Cast;
import com.feedzai.commons.sql.abstraction.dml.Coalesce;
import com.feedzai.commons.sql.abstraction.dml.Delete;
import com.feedzai.commons.sql.abstraction.dml.Expression;
import com.feedzai.commons.sql.abstraction.dml.Function;
import com.feedzai.commons.sql.abstraction.dml.Join;
import com.feedzai.commons.sql.abstraction.dml.K;
import com.feedzai.commons.sql.abstraction.dml.Literal;
import com.feedzai.commons.sql.abstraction.dml.Modulo;
import com.feedzai.commons.sql.abstraction.dml.Name;
import com.feedzai.commons.sql.abstraction.dml.Query;
import com.feedzai.commons.sql.abstraction.dml.RepeatDelimiter;
import com.feedzai.commons.sql.abstraction.dml.StringAgg;
import com.feedzai.commons.sql.abstraction.dml.Truncate;
import com.feedzai.commons.sql.abstraction.dml.Union;
import com.feedzai.commons.sql.abstraction.dml.Update;
import com.feedzai.commons.sql.abstraction.dml.Values;
import com.feedzai.commons.sql.abstraction.dml.View;
import com.feedzai.commons.sql.abstraction.dml.When;
import com.feedzai.commons.sql.abstraction.dml.With;
import com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.google.common.base.Joiner;
import java.util.Collections;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.union;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.escapeSql;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.quotize;
import static com.feedzai.commons.sql.abstraction.util.StringUtils.singleQuotize;

/**
 * Abstract translator to be extended by specific implementations.
 * <p/>
 * This class already provides translations that are common to all databases.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public abstract class AbstractTranslator {
    /**
     * The properties in place.
     */
    @Inject
    protected PdbProperties properties;
    /**
     * The Guice injector.
     */
    @Inject
    protected Injector injector;

    /**
     * Injects dependencies on the given objects.
     *
     * @param objs The objects to be injected.
     */
    protected void inject(Expression... objs) {
        for (Object o : objs) {
            if (o == null) {
                continue;
            }

            injector.injectMembers(o);
        }
    }

    /**
     * Injects dependencies on the given objects.
     *
     * @param objs The objects to be injected.
     */
    protected void inject(Collection<? extends Expression> objs) {
        for (Object o : objs) {
            if (o == null) {
                continue;
            }

            injector.injectMembers(o);
        }
    }

    /**
     * Joins a collection of objects given the delimiter.
     *
     * @param list      The collection of objects to join.
     * @param delimiter The delimiter.
     * @return A String representing the given objects separated by the delimiter.
     */
    protected String join(Collection<?> list, String delimiter) {
        return Joiner.on(delimiter).join(list);
    }

    /**
     * Translates {@link Name}.
     *
     * @param n The object to translate.
     * @return The string representation of the given object.
     */
    public String translate(Name n) {
        final String name = n.getName();
        final String environment = n.getEnvironment();
        final List<String> res = new ArrayList<>();

        if (environment != null) {
            res.add(quotize(environment, translateEscape()) + "." + (n.isQuote() ? quotize(name, translateEscape()) : name));
        } else {
            res.add(n.isQuote() ? quotize(name, translateEscape()) : name);
        }

        if (n.getOrdering() != null) {
            res.add(n.getOrdering());
        }

        if (n.isIsNull()) {
            res.add("IS NULL");
        }

        if (n.isIsNotNull()) {
            res.add("IS NOT NULL");
        }

        if (n.isEnclosed()) {
            return "(" + StringUtils.join(res, " ") + ")";
        } else {
            return StringUtils.join(res, " ");
        }
    }

    /**
     * Translates {@link Between}.
     *
     * @param b The object to translate.
     * @return The string representation of the given object.
     */
    public String translate(Between b) {
        final Expression and = b.getAnd();
        final Expression column = b.getColumn();
        inject(and, column);

        String modifier = "BETWEEN";
        if (b.isNot()) {
            modifier = "NOT " + modifier;
        }

        String result = String.format("%s %s %s", column.translate(), modifier, and.translate());

        if (b.isEnclosed()) {
            result = "(" + result + ")";
        }

        return result;
    }

    /**
     * Translates {@link Coalesce}.
     *
     * @param c The object to translate.
     * @return The string representation of the given object.
     */
    public String translate(Coalesce c) {
        final Expression[] alternative = c.getAlternative();
        Expression exp = c.getExp();
        inject(exp);

        final String[] alts = new String[alternative.length];
        int i = 0;
        for (Expression e : alternative) {
            inject(e);
            alts[i] = e.translate();
            i++;
        }

        return String.format("COALESCE(%s, " + Joiner.on(", ").join(alts) + ")", exp.translate());
    }

    /**
     * Translates {@link Delete}.
     *
     * @param d The object to translate.
     * @return The string representation of the given object.
     */
    public String translate(Delete d) {
        final Expression table = d.getTable();
        final Expression where = d.getWhere();
        inject(table, where);

        final List<String> temp = new ArrayList<>();

        temp.add("DELETE FROM");
        temp.add(table.translate());

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translate());
        }

        return Joiner.on(" ").join(temp);
    }

    /**
     * Translates {@link Join}.
     *
     * @param j The object to translate.
     * @return The string representation of the given object.
     */
    public String translate(Join j) {
        final String join = j.getJoin();
        final Expression joinExpr = j.getJoinExpr();
        final Expression joinTable = j.getJoinTable();
        inject(joinExpr, joinTable);


        if (joinTable.isAliased()) {
            return String.format("%s %s %s ON (%s)", join, joinTable.translate(), quotize(joinTable.getAlias(), translateEscape()), joinExpr.translate());
        } else {
            return String.format("%s %s ON (%s)", join, joinTable.translate(), joinExpr.translate());
        }
    }


    /**
     * Translates {@link K}.
     *
     * @param k The object to translate.
     * @return The string representation of the given object.
     */
    public String translate(K k) {
        final Object o = k.getConstant();

        String result;

        if (o != null) {
            if (!k.isQuote()) {
                result = o.toString();
            } else if (o instanceof String) {
                result = singleQuotize(escapeSql((String) o));
            } else if (o instanceof Boolean) {
                result = (Boolean) o ? translateTrue() : translateFalse();
            } else {
                result = o.toString();
            }
        } else {
            result = "NULL";
        }

        return k.isEnclosed() ? ("(" + result + ")") : result;
    }


    /**
     * Translates {@link Literal}.
     *
     * @param l The object to translate.
     * @return The string representation of the given object.
     */
    public String translate(Literal l) {
        return l.getLiteral().toString();
    }


    /**
     * Translates {@link Truncate}.
     *
     * @param t The object to translate.
     * @return The string representation of the given object.
     */
    public String translate(Truncate t) {
        final Expression table = t.getTable();
        inject(table);

        final List<String> temp = new ArrayList<>();

        temp.add("TRUNCATE TABLE");
        temp.add(table.translate());

        return join(temp, " ");
    }

    /**
     * Translates {@link Update}.
     *
     * @param u The object to translate.
     * @return The string representation of the given object.
     */
    public String translate(Update u) {
        final List<Expression> columns = u.getColumns();
        final Expression table = u.getTable();
        final Expression where = u.getWhere();
        inject(table, where);


        final List<String> temp = new ArrayList<>();

        temp.add("UPDATE");
        temp.add(table.translate());
        if (table.isAliased()) {
            temp.add(quotize(table.getAlias(), translateEscape()));
        }
        temp.add("SET");
        List<String> setTranslations = new ArrayList<>();
        for (Expression e : columns) {
            inject(e);
            setTranslations.add(e.translate());
        }
        temp.add(join(setTranslations, ", "));

        if (where != null) {
            temp.add("WHERE");
            temp.add(where.translate());
        }

        return join(temp, " ");
    }

    public String translate(final With with) {

        final List<ImmutablePair<Name, Expression>> clauses = with.getClauses();

        clauses.forEach(clause -> {
            injector.injectMembers(clause.getLeft());
            injector.injectMembers(clause.getRight());
        });

        final String withStatements = clauses.stream()
                .map(clause -> clause.getLeft().translate() + " AS (" + clause.getRight().translate() + ")")
                .collect(Collectors.joining(", "));

        final Expression then = with.getThen();

        final String translation;
        if (then != null) {
            inject(then);
            translation =  String.format("WITH %s %s", withStatements, then.translate());
        } else {
            translation = String.format("WITH %s", withStatements);
        }

        return with.isEnclosed() ? "(" + translation + ")" : translation;
    }

    /**
     * Translates When.
     *
     * @param when a when.
     * @return when translation.
     */
    public String translate(final When when) {
        inject(when.condition);
        inject(when.action);

        return String.format("WHEN %s THEN %s",
                             when.condition.translate(),
                             when.action.translate());
    }

    /**
     * Translates Case.
     *
     * @param aCase a case.
     * @return case translation.
     */
    public String translate(final Case aCase) {
        String elseString = "";
        if (aCase.getFalseAction() != null) {
            inject(aCase.getFalseAction());
            elseString = String.format("ELSE %s", aCase.getFalseAction().translate());
        }

        final String whens = aCase.whens.stream()
                .peek(this::inject)
                .map(When::translate)
                .collect(Collectors.joining(" "));

        return String.format("CASE %s %s END",
                             whens,
                             elseString);
    }

    /**
     * Translates Cast.
     *
     * @param cast a cast expression.
     * @return cast translation.
     */
    public abstract String translate(Cast cast);

    /**
     * Translates {@link Union}.
     *
     * @param union a union.
     * @return union translation.
     */
    public String translate(final Union union) {
        final List<Expression> expressions = union.getExpressions();
        final String delimiter = union.isAll() ? " UNION ALL " : " UNION ";

        inject(expressions);
        final String translation = expressions.stream()
                .map(Expression::translate)
                .collect(Collectors.joining(delimiter));
        return union.isEnclosed() ? "(" + translation + ")": translation;
    }

    /**
     * Translates Values.
     *
     * @param values a values.
     * @return values translation.
     */
    public String translate(final Values values) {
        final ArrayList<Values.Row> rows = new ArrayList<>(values.getRows());
        final String[] aliases = values.getAliases();

        // If aliases does not exist, throw an exception.
        // Otherwise, apply them to the columns.
        if (aliases == null || aliases.length == 0) {
            throw new DatabaseEngineRuntimeException("Values requires aliases to avoid ambiguous columns names.");
        } else {
            rows.forEach(row -> {
                final List<Expression> expressions = row.getExpressions();
                for (int i = 0; i < expressions.size() && i < aliases.length; i++) {
                    // DISCLAIMER : May have side-effects because will change the state of the row's expressions.
                    expressions.get(i).alias(aliases[i]);
                }
            });
        }

        // Put each row on a select for union operator to work.
        final List<Expression> rowsWithSelect = rows.stream()
                .map(SqlBuilder::select)
                .collect(Collectors.toList());

        // By default, use UNION ALL to express VALUES.
        // This way, only engines that support VALUES will implement it.
        // Since the amount of values can be quite large, a linear UNION can cause Stack Overflow.
        // To avoid it, we model this UNION as a binary tree.
        final Union union = rowsToUnion(rowsWithSelect);

        if (values.isEnclosed()) {
            union.enclose();
        }

        return translate(union);
    }

    /**
     * Translates Values.Row.
     *
     * @param row a values.row.
     * @return values.row translation.
     */
    public String translate(final Values.Row row) {
        inject(row.getExpressions());

        final String translation = row.getExpressions().stream()
                .map(expression -> {
                    // Enforce aliases to be translated.
                    final String alias = expression.isAliased() ? " AS " + quotize(expression.getAlias()) : "";
                    return expression.translate() + alias;
                })
                .collect(Collectors.joining(", "));

        return row.isEnclosed() ? "(" + translation + ")" : translation;
    }

    /**
     * Transform values' rows into a union.
     *
     * @param rows the values' rows
     * @return the resulting union
     */
    protected Union rowsToUnion(final List<Expression> rows) {
        // Create an union in form of a binary tree from a list of rows.
        // The tree shape is to prevent stack overflow on some
        // database engines when the list of rows is too big.

        List<Expression> rowsWithSelect = new ArrayList<>(rows);

        while (rowsWithSelect.size() > 2) {
            final List<Expression> newRowsWithSelect = new ArrayList<>();

            // Put the first and second rowsWithSelect together forming an UNION ALL.
            // Then the third and forth, and so on.
            // To do this, we move though the rows with a delta of 2, to group the rows in pairs.
            for (int i = 1; i < rowsWithSelect.size(); i+=2) {
                final Expression left = rowsWithSelect.get(i - 1);
                final Expression right = rowsWithSelect.get(i);

                newRowsWithSelect.add(union(left, right).all().enclose());
            }

            // If the number of rowsWithSelect is odd, it will remain an expression at the end.
            // In this case, this last expression will be the right leaf of the root of the union tree.
            if (rowsWithSelect.size() % 2 == 1) {
                newRowsWithSelect.add(rowsWithSelect.get(rowsWithSelect.size() - 1));
            }

            rowsWithSelect = newRowsWithSelect;
        }

        return union(rowsWithSelect).all();
    }

    /**
     * Translates the escape character.
     *
     * @return The string representation of the escape character.
     */
    public abstract String translateEscape();

    /**
     * Translates the boolean true.
     *
     * @return The string representation of the true keyword.
     */
    public abstract String translateTrue();

    /**
     * Translates the boolean false.
     *
     * @return The string representation of the false keyword.
     */
    public abstract String translateFalse();

    /**
     * Translates {@link Name}.
     *
     * @param ac The object to translate.
     * @return The string representation of the given object.
     */
    public abstract String translate(AlterColumn ac);

    /**
     * Translates {@link DropPrimaryKey}.
     *
     * @param dpk The object to translate.
     * @return The string representation of the given object.
     */
    public abstract String translate(DropPrimaryKey dpk);

    /**
     * Translates {@link Function}.
     *
     * @param f The object to translate.
     * @return The string representation of the given object.
     */
    public abstract String translate(Function f);

    /**
     * Translates {@link Modulo}.
     *
     * @param m The object to translate.
     * @return The string representation of the given object.
     */
    public abstract String translate(Modulo m);

    /**
     * Translates {@link Rename}.
     *
     * @param r The object to translate.
     * @return The string representation of the given object.
     */
    public abstract String translate(Rename r);

    /**
     * Translates {@link RepeatDelimiter}.
     *
     * @param rd The object to translate.
     * @return The string representation of the given object.
     */
    public abstract String translate(RepeatDelimiter rd);

    /**
     * Translates {@link Query}.
     *
     * @param q The object to translate.
     * @return The string representation of the given object.
     */
    public abstract String translate(Query q);

    /**
     * Translates {@link View}.
     *
     * @param v The object to translate.
     * @return The string representation of the given object.
     */
    public abstract String translate(View v);

    /**
     * Translates {@link DbColumn}.
     *
     * @param dc The object to translate.
     * @return The string representation of the given object.
     */
    public abstract String translate(DbColumn dc);

    /**
     * Translates {@link StringAgg}.
     *
     * @param stringAgg The object to translate.
     * @return The string representation of the given object.
     */
    public abstract String translate(StringAgg stringAgg);

    /**
     * Translates the given entity table creation to the current dialect.
     *
     * @param entity The entity to translate.
     * @return The create table translation result.
     */
    public abstract String translateCreateTable(DbEntity entity);

    /**
     * Translates the primary key not null constraints of the given entity table to the current dialect.
     *
     * @param entity The entity to translate.
     * @return The primary key not null constraints translation result.
     */
    public String translatePrimaryKeysNotNull(DbEntity entity) {
        // usually engines don't need to specify columns as not nulls to be PK.
        return "";
    }

    /**
     * Translates the primary key constraints of the given entity table to the current dialect.
     *
     * @param entity The entity to translate.
     * @return The primary key constraints translation result.
     */
    public abstract String translatePrimaryKeysConstraints(DbEntity entity);

    /**
     * Translates the foreign key constraints of the given entity table to the current dialect.
     *
     * @param entity The entity to translate.
     * @return The foreign key constraints translation result.
     */
    public abstract List<String> translateForeignKey(DbEntity entity);

    /**
     * Translates the index creation of the given entity table to the current dialect.
     *
     * @param entity The entity to translate.
     * @return The index creation translation result.
     */
    public abstract List<String> translateCreateIndexes(DbEntity entity);

    /**
     * Translates the sequence creation of the given entity table to the current dialect.
     *
     * @param entity The entity to translate.
     * @return The sequence creation translation result.
     */
    public List<String> translateCreateSequences(DbEntity entity) {
        // the majority of engines don't need additional SQL to create sequences.
        return Collections.emptyList();
    }
}
