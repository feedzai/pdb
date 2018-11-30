/*
 * The copyright of this file belongs to Feedzai. The file cannot be
 * reproduced in whole or in part, stored in a retrieval system,
 * transmitted in any form, or by any means electronic, mechanical,
 * photocopying, or falseAction, without the prior permission of the owner.
 *
 * (c) 2014 Feedzai, Strictly Confidential
 */
package com.feedzai.commons.sql.abstraction.dml;

import com.google.inject.Injector;

import javax.inject.Inject;

import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.k;

/**
 * Case SQL Expression.
 *
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.2.3
 */
public class CaseElse extends Case {

    /**
     * Action to be executed if the condition is false.
     */
    public final Expression falseAction;

    /**
     * Creats a CaseElse.
     *
     * @param condition condition to verify.
     * @param trueAction action to be executed if the condition is true.
     * @param falseAction action to be executed if the condition is false.
     */
    private CaseElse(final Expression condition, final Expression trueAction, final Expression falseAction) {
        super();
        super.when(condition, trueAction);
        this.falseAction = falseAction;
    }

    /**
     * @param condition condition to verify.
     * @return a new "case when" considering the condition.
     */
    public static CaseElse caseWhen(final Expression condition) {
        return new CaseElse(condition, k(true), k(false));
    }

    /**
     * @param condition condition to verify.
     * @param trueAction action to be executed if the condition is true.
     * @param falseAction action to be executed if the condition is false.
     * @return a new "case when" that does the trueAction if the condition is true. Otherwise it runs falseAction.
     */
    public static CaseElse caseWhenElse(final Expression condition, final Expression trueAction,
                                    final Expression falseAction) {
        return new CaseElse(condition, trueAction, falseAction);
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
