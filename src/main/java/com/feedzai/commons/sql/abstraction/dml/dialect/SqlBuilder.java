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
package com.feedzai.commons.sql.abstraction.dml.dialect;

import com.feedzai.commons.sql.abstraction.ddl.*;
import com.feedzai.commons.sql.abstraction.dml.*;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;

import java.util.Collection;

import static com.feedzai.commons.sql.abstraction.dml.Function.*;
import static com.feedzai.commons.sql.abstraction.dml.RepeatDelimiter.*;

/**
 * The SQL Builder that allows representing SQL queries
 * in a similar way they're written.
 * <p/>
 * It is intended to be used like the following.
 * <p/>
 * <pre>
 * {@code
 *      import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.*;
 *
 *      (...)
 *
 *      select(all()).from(table("TABLE_NAME"))
 * }
 * </pre>
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public final class SqlBuilder {

    /**
     * Starts a new query.
     *
     * @param select The objects.
     * @return A new query.
     */
    public static Query select(final Expression... select) {
        return new Query().select(select);
    }

    /**
     * Starts a new query.
     *
     * @param select The collection of select columns.
     * @return A new query.
     */
    public static Query select(final Collection<? extends Expression> select) {
        return new Query().select(select);
    }

    /**
     * Selects all columns in a row.
     *
     * @return The expression.
     */
    public static Expression all() {
        return new All().unquote();
    }

    /**
     * Selects all columns in a table.
     *
     * @param tableName The table.
     * @return The expression.
     */
    public static Expression all(final String tableName) {
        return new All(tableName).unquote();
    }

    /**
     * A column.
     *
     * @param name The column name.
     * @return The expression.
     */
    public static Name column(final String name) {
        return new Name(name);
    }

    /**
     * A column from a table.
     *
     * @param tableName The table name.
     * @param name      The column name.
     * @return The expression.
     */
    public static Name column(final String tableName, final String name) {
        return new Name(tableName, name);
    }

    /**
     * A table.
     *
     * @param name The table name.
     * @return The expression.
     */
    public static Name table(final String name) {
        return new Name(name);
    }

    /**
     * The equals expression.
     *
     * @param e1 The expressions.
     * @return The expression.
     */
    public static Expression eq(final Expression... e1) {
        return new RepeatDelimiter(EQ, e1);
    }

    /**
     * The equals expression.
     *
     * @param e1 The expressions.
     * @return The expression.
     */
    public static Expression eq(final Collection<? extends Expression> e1) {
        return new RepeatDelimiter(EQ, e1);
    }

    /**
     * A constant.
     *
     * @param o The constant.
     * @return The expression.
     */
    public static Expression k(final Object o) {
        return new K(o);
    }

    /**
     * The AND operator.
     *
     * @param exps The expressions.
     * @return The AND representation.
     */
    public static Expression and(final Expression... exps) {
        return new RepeatDelimiter(AND, exps);
    }

    /**
     * The AND operator.
     *
     * @param exps The expressions.
     * @return The AND representation.
     */
    public static Expression and(final Collection<? extends Expression> exps) {
        return new RepeatDelimiter(AND, exps);
    }

    /**
     * The OR operator.
     *
     * @param exps The expressions.
     * @return The OR representation.
     */
    public static Expression or(final Expression... exps) {
        return new RepeatDelimiter(OR, exps);
    }

    /**
     * The OR operator.
     *
     * @param exps The expressions.
     * @return The OR representation.
     */
    public static Expression or(final Collection<? extends Expression> exps) {
        return new RepeatDelimiter(OR, exps);
    }

    /**
     * The DIV operator.
     *
     * @param exps The expressions.
     * @return The DIV representation.
     */
    public static Expression div(final Expression... exps) {
        return new RepeatDelimiter(DIV, exps);
    }

    /**
     * The DIV operator.
     *
     * @param exps The expressions.
     * @return The DIV representation.
     */
    public static Expression div(final Collection<? extends Expression> exps) {
        return new RepeatDelimiter(DIV, exps);
    }

    /**
     * The MOD operator.
     *
     * @param exp1 The dividend.
     * @param exp2 The divisor.
     * @return The DIV representation.
     */
    public static Expression mod(final Expression exp1, final Expression exp2) {
        return new Modulo(exp1, exp2);
    }

    /**
     * The GT operator.
     *
     * @param exps The expressions.
     * @return The GT representation.
     */
    public static Expression gt(final Expression... exps) {
        return new RepeatDelimiter(GT, exps);
    }

    /**
     * The GT operator.
     *
     * @param exps The expressions.
     * @return The GT representation.
     */
    public static Expression gt(final Collection<? extends Expression> exps) {
        return new RepeatDelimiter(GT, exps);
    }

    /**
     * The GTEQ operator.
     *
     * @param exps The expressions.
     * @return The GTEQ representation.
     */
    public static Expression gteq(final Expression... exps) {
        return new RepeatDelimiter(GTEQ, exps);
    }

    /**
     * The GTEQ operator.
     *
     * @param exps The expressions.
     * @return The GTEQ representation.
     */
    public static Expression gteq(final Collection<? extends Expression> exps) {
        return new RepeatDelimiter(GTEQ, exps);
    }

    /**
     * The LIKE operator.
     *
     * @param exps The expressions.
     * @return The LIKE representation.
     */
    public static Expression like(final Expression... exps) {
        return new RepeatDelimiter(LIKE, exps);
    }

    /**
     * The LIKE operator.
     *
     * @param exps The expressions.
     * @return The LIKE representation.
     */
    public static Expression like(final Collection<? extends Expression> exps) {
        return new RepeatDelimiter(LIKE, exps);
    }

    /**
     * The LT operator.
     *
     * @param exps The expressions.
     * @return The LT representation.
     */
    public static Expression lt(final Expression... exps) {
        return new RepeatDelimiter(LT, exps);
    }

    /**
     * The LT operator.
     *
     * @param exps The expressions.
     * @return The LT representation.
     */
    public static Expression lt(final Collection<? extends Expression> exps) {
        return new RepeatDelimiter(LT, exps);
    }

    /**
     * The LTEQ operator.
     *
     * @param exps The expressions.
     * @return The LTEQ representation.
     */
    public static Expression lteq(final Expression... exps) {
        return new RepeatDelimiter(LTEQ, exps);
    }

    /**
     * The LTEQ operator.
     *
     * @param exps The expressions.
     * @return The LTEQ representation.
     */
    public static Expression lteq(final Collection<? extends Expression> exps) {
        return new RepeatDelimiter(LTEQ, exps);
    }

    /**
     * The MINUS operator.
     *
     * @param exps The expressions.
     * @return The MINUS representation.
     */
    public static Expression minus(final Expression... exps) {
        return new RepeatDelimiter(MINUS, exps);
    }

    /**
     * The MINUS operator.
     *
     * @param exps The expressions.
     * @return The MINUS representation.
     */
    public static Expression minus(final Collection<? extends Expression> exps) {
        return new RepeatDelimiter(MINUS, exps);
    }

    /**
     * The MULT operator.
     *
     * @param exps The expressions.
     * @return The MULT representation.
     */
    public static Expression mult(final Expression... exps) {
        return new RepeatDelimiter(MULT, exps);
    }

    /**
     * The MULT operator.
     *
     * @param exps The expressions.
     * @return The MULT representation.
     */
    public static Expression mult(final Collection<? extends Expression> exps) {
        return new RepeatDelimiter(MULT, exps);
    }

    /**
     * The PLUS operator.
     *
     * @param exps The expressions.
     * @return The PLUS representation.
     */
    public static Expression plus(final Expression... exps) {
        return new RepeatDelimiter(PLUS, exps);
    }

    /**
     * The PLUS operator.
     *
     * @param exps The expressions.
     * @return The PLUS representation.
     */
    public static Expression plus(final Collection<? extends Expression> exps) {
        return new RepeatDelimiter(PLUS, exps);
    }

    /**
     * The MAX operator.
     *
     * @param exp The expression.
     * @return The MAX representation.
     */
    public static Expression max(final Expression exp) {
        return new Function(MAX, exp);
    }

    /**
     * The MIN operator.
     *
     * @param exp The expression.
     * @return The MIN representation.
     */
    public static Expression min(final Expression exp) {
        return new Function(MIN, exp);
    }

    /**
     * The STDDEV operator.
     *
     * @param exp The expression.
     * @return The STDDEV representation.
     */
    public static Expression stddev(final Expression exp) {
        return new Function(STDDEV, exp);
    }

    /**
     * The AVG operator.
     *
     * @param exp The expression.
     * @return The AVG representation.
     */
    public static Expression avg(final Expression exp) {
        return new Function(AVG, exp);
    }

    /**
     * The COUNT operator.
     *
     * @param exp The expression.
     * @return The COUNT representation.
     */
    public static Expression count(final Expression exp) {
        return new Function(COUNT, exp);
    }

    /**
     * The SUM operator.
     *
     * @param exp The expression.
     * @return The SUM representation.
     */
    public static Expression sum(final Expression exp) {
        return new Function(SUM, exp);
    }

    /**
     * The FLOOR operator.
     *
     * @param exp The expression.
     * @return The FLOOR representation.
     */
    public static Expression floor(final Expression exp) {
        return new Function(FLOOR, exp);
    }

    /**
     * The CEILING operator.
     *
     * @param exp The expression.
     * @return The CEILING representation.
     */
    public static Expression ceiling(final Expression exp) {
        return new Function(CEILING, exp);
    }

    /**
     * The Used Defined Function operator.
     *
     * @param udf The UDF name.
     * @return The UDF representation.
     */
    public static Expression udf(final String udf) {
        return new Function(udf);
    }

    /**
     * The Used Defined Function operator.
     *
     * @param udf The UDF name.
     * @param exp The expression.
     * @return The UDF representation.
     */
    public static Expression udf(final String udf, final Expression exp) {
        return new Function(udf, exp);
    }

    /**
     * The UPPER operator.
     *
     * @param exp The expression inside the operator.
     * @return The UPPER representation.
     */
    public static Expression upper(final Expression exp) {
        return new Function(UPPER, exp);
    }

    /**
     * The StringAgg function.
     *
     * @param column The expression inside the operator.
     * @return The StringAgg function.
     */
    public static StringAgg stringAgg(final Expression column) {
        return StringAgg.stringAgg(column);
    }

    /**
     * The LOWER operator.
     *
     * @param exp The expression inside the operator.
     * @return The LOWER representation.
     */
    public static Expression lower(final Expression exp) {
        return new Function(LOWER, exp);
    }

    /**
     * An internal function (provided by the engine in place).
     *
     * @param function The function.
     * @return The function representation.
     */
    public static Expression f(final String function) {
        return f(function, null);
    }

    /**
     * An internal function (provided by the engine in place).
     *
     * @param function The function.
     * @param exp      The expression.
     * @return The function representation.
     */
    public static final Expression f(final String function, final Expression exp) {
        return new InternalFunction(function, exp);
    }

    /**
     * The same of making eq(e1, e2).
     *
     * @param e1 The first expression.
     * @param e2 The second expression.
     * @return A join representation using the equal operator.
     */
    public static Expression join(final Expression e1, final Expression e2) {
        return new RepeatDelimiter(EQ, e1, e2);
    }

    /**
     * Creates a view.
     *
     * @param name The name of the view.
     * @return The view representation.
     */
    public static View createView(final String name) {
        return new View(name);
    }

    /**
     * Returns a new "case when".
     *
     * @return a new "case when".
     */
    public static Case caseWhen() {
        return Case.caseWhen();
    }

    /**
     * Creates a case expression.
     *
     * @param condition The name of the view.
     * @param trueAction The name of the view.
     * @return The case when representation.
     */
    public static Case caseWhen(final Expression condition, final Expression trueAction) {
        return Case.caseWhen(condition, trueAction);
    }

    /**
     * The not equal expression.
     *
     * @param exps The expressions.
     * @return The expression.
     */
    public static Expression neq(final Expression... exps) {
        return new RepeatDelimiter(NEQ, exps);
    }

    /**
     * The not equal expression.
     *
     * @param exps The expressions.
     * @return The expression.
     */
    public static Expression neq(final Collection<? extends Expression> exps) {
        return new RepeatDelimiter(NEQ, exps);
    }

    /**
     * A list of expressions enclosed.
     *
     * @param exps The expressions.
     * @return The expression.
     */
    public static Expression L(final Expression... exps) {
        return new RepeatDelimiter(COMMA, exps).enclose();
    }

    /**
     * A list of expression enclosed.
     *
     * @param exps The expressions.
     * @return The expression.
     */
    public static Expression L(final Collection<? extends Expression> exps) {
        return new RepeatDelimiter(COMMA, exps).enclose();
    }

    /**
     * The IN expression.
     *
     * @param e1 The first expression.
     * @param e2 The second expression.
     * @return The expression.
     */
    public static Expression in(final Expression e1, final Expression e2) {
        return new RepeatDelimiter(IN, e1.isEnclosed() ? e1 : e1.enclose(), e2.isEnclosed() ? e2 : e2.enclose());
    }

    /**
     * The NOT IN expression.
     *
     * @param e1 The first expression.
     * @param e2 The second expression.
     * @return The expression.
     */
    public static Expression notIn(final Expression e1, final Expression e2) {
        return new RepeatDelimiter(NOTIN, e1.isEnclosed() ? e1 : e1.enclose(), e2.isEnclosed() ? e2 : e2.enclose());
    }

    /**
     * The coalesce operator.
     *
     * @param exp         The expression to test if is NULL.
     * @param alternative The alternative expressions to use.
     * @return The coalesce operator.
     */
    public static Coalesce coalesce(final Expression exp, Expression... alternative) {
        return new Coalesce(exp, alternative);
    }

    /**
     * The BETWEEN operator.
     *
     * @param exp1 The column.
     * @param exp2 The first bound.
     * @param exp3 The second bound.
     * @return The between expression.
     */
    public static Between between(final Expression exp1, final Expression exp2, final Expression exp3) {
        return new Between(exp1, and(exp2, exp3));
    }

    /**
     * The NOT BETWEEN operator.
     *
     * @param exp1 The column.
     * @param exp2 The first bound.
     * @param exp3 The second bound.
     * @return The between expression.
     */
    public static Between notBetween(final Expression exp1, final Expression exp2, final Expression exp3) {
        return new Between(exp1, and(exp2, exp3)).not();
    }

    /**
     * The UPDATE operator.
     *
     * @param table The table.
     * @return The UPDATE expression.
     */
    public static Update update(final Expression table) {
        return new Update(table);
    }

    /**
     * The DELETE keyword.
     *
     * @param table The table.
     * @return The DELETE expression.
     */
    public static Delete delete(final Expression table) {
        return new Delete(table);
    }

    /**
     * Use this to add literals (strings, etc) when building the SQL statement.
     *
     * @param o The literal.
     * @return The literal object.
     */
    public static Literal lit(final Object o) {
        return new Literal(o);
    }

    /**
     * The TRUNCATE keyword.
     *
     * @param table The table.
     * @return The TRUNCATE object.
     */
    public static Truncate truncate(final Expression table) {
        return new Truncate(table);
    }


    /**
     * Rename table operator.
     *
     * @param oldName The table.
     * @return The rename instance.
     */
    public static Rename rename(final Expression oldName, final Expression newName) {
        return new Rename(oldName, newName);
    }

    /**
     * Drop primary key table operator.
     *
     * @param table The table.
     * @return The drop primary key instance.
     */
    public static DropPrimaryKey dropPK(final Expression table) {
        return new DropPrimaryKey(table);
    }

    /**
     * Alter column operator.
     *
     * @param table        The table containing the column.
     * @param column       The column of the table.
     * @param dbColumnType The db column type.
     * @param constraints  The constraints of the column.
     * @return The alter column operator.
     */
    public static AlterColumn alterColumn(Expression table, Name column, DbColumnType dbColumnType, DbColumnConstraint... constraints) {
        return alterColumn(
                table,
                dbColumn().name(column.getName()).type(dbColumnType).addConstraints(constraints).build());
    }

    /**
     * Alter column operator.
     *
     * @param table    The table containing the column.
     * @param dbColumn The database column definition.
     * @return The alter column operator.
     */
    public static AlterColumn alterColumn(Expression table, DbColumn dbColumn) {
        return new AlterColumn(table, dbColumn);
    }

    /**
     * Creates a Database Foreign Key builder.
     *
     * @return A Database Foreign Key builder.
     */
    public static DbFk.Builder dbFk() {
        return new DbFk.Builder();
    }

    /**
     * Creates a Database Entity builder.
     *
     * @return A Database Entity Key builder.
     */
    public static DbEntity.Builder dbEntity() {
        return new DbEntity.Builder();
    }

    /**
     * Creates a Database Index builder.
     *
     * @return A Database Index Key builder.
     */
    public static DbIndex.Builder dbIndex() {
        return new DbIndex.Builder();
    }

    /**
     * Creates a Database Column builder.
     *
     * @return A Database Column Key builder.
     */
    public static DbColumn.Builder dbColumn() {
        return new DbColumn.Builder();
    }

    /**
     * Creates a Database Column builder.
     *
     * @return A Database Column Key builder.
     */
    public static DbColumn.Builder dbColumn(String name, DbColumnType type, boolean autoInc) {
        return new DbColumn.Builder().name(name).type(type).autoInc(autoInc);
    }

    /**
     * Creates a Database Column builder.
     *
     * @return A Database Column Key builder.
     */
    public static DbColumn.Builder dbColumn(String name, DbColumnType type) {
        return new DbColumn.Builder().name(name).type(type);
    }

    /**
     * Creates a Database Column builder.
     *
     * @return A Database Column Key builder.
     */
    public static DbColumn.Builder dbColumn(String name, DbColumnType type, int size) {
        return new DbColumn.Builder().name(name).type(type).size(size);
    }

    /**
     * Creates a Database Entry builder.
     *
     * @return A Database Entry Key builder.
     */
    public static EntityEntry.Builder entry() {
        return new EntityEntry.Builder();
    }
}
