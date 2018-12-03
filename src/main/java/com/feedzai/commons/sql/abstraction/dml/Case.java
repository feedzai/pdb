/*
 * The copyright of this file belongs to Feedzai. The file cannot be
 * reproduced in whole or in part, stored in a retrieval system,
 * transmitted in any form, or by any means electronic, mechanical,
 * photocopying, or otherwise, without the prior permission of the owner.
 *
 * (c) 2018 Feedzai, Strictly Confidential
 */
package com.feedzai.commons.sql.abstraction.dml;

import java.util.ArrayList;
import java.util.List;

import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.k;

/**
 * Case SQL Expression.
 *
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.2.3
 */
public class Case extends Expression {

    /**
     * List of when clauses.
     */
    public final List<When> whens;

    /**
     * Action to be executed if the condition is false.
     */
    private Expression falseAction;

    /**
     * Creates an empty case.
     */
    protected Case() {
        this.whens = new ArrayList<>();
        this.falseAction = null;
    }

    /**
     * Gets the false action.
     *
     * @return the false action.
     */
    public Expression getFalseAction() {
        return falseAction;
    }

    /**
     * Returns a new empty case.
     *
     * @return a new case.
     */
    public static Case caseWhen() {
        return new Case();
    }

    /**
     * Returns a new "case when" that returns true or false considering the condition.
     *
     * @param condition condition to verify.
     * @return a new "case when" considering the condition.
     */
    public static Case caseWhen(final Expression condition) {
        return caseWhen(condition, k(true), k(false));
    }

    /**
     * Returns a new "case when" that does the trueAction considering the condition.
     *
     * @param condition condition to verify.
     * @param trueAction action to be executed if the condition is true.
     * @return a new "case when" that does the trueAction considering the condition.
     */
    public static Case caseWhen(final Expression condition, final Expression trueAction) {
        return caseWhen().when(condition, trueAction);
    }

    /**
     * @param condition condition to verify.
     * @param trueAction action to be executed if the condition is true.
     * @param falseAction action to be executed if the condition is false.
     * @return a new "case when" that does the trueAction if the condition is true. Otherwise it runs falseAction.
     */
    public static Case caseWhen(final Expression condition, final Expression trueAction,
                                    final Expression falseAction) {
        return caseWhen().when(condition, trueAction).otherwise(falseAction);
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }

    /**
     * Adds a new when clause to this case.
     *
     * @param condition condition to verify.
     * @param action action to be executed if the condition is true.
     * @return this case.
     */
    public Case when(final Expression condition, final Expression action) {
        whens.add(When.when(condition, action));
        return this;
    }


    /**
     * Sets the false action.
     *
     * @param falseAction action done if the condition is false.
     * @return this case.
     */
    public Case otherwise(Expression falseAction){
        this.falseAction = falseAction;
        return this;
    }
}
