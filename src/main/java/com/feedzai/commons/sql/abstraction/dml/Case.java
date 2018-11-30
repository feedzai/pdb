/*
 * The copyright of this file belongs to Feedzai. The file cannot be
 * reproduced in whole or in part, stored in a retrieval system,
 * transmitted in any form, or by any means electronic, mechanical,
 * photocopying, or otherwise, without the prior permission of the owner.
 *
 * (c) 2014 Feedzai, Strictly Confidential
 */
package com.feedzai.commons.sql.abstraction.dml;

import com.google.inject.Injector;

import javax.inject.Inject;
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
     * Creates an empty case.
     */
    protected Case() {
        this.whens = new ArrayList<>();
    }

    /**
     * @return a new case.
     */
    public static Case caseWhen() {
        return new Case();
    }

    /**
     * @param condition condition to verify.
     * @param trueAction action to be executed if the condition is true.
     * @return a new "case when" that does the trueAction considering the condition.
     */
    public static Case caseWhen(final Expression condition, final Expression trueAction) {
        return caseWhen().when(condition, trueAction);
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }

    /**
     * @param condition condition to verify.
     * @param action action to be executed if the condition is true.
     * @return this case.
     */
    public Case when(final Expression condition, final Expression action) {
        whens.add(When.when(condition, action));
        return this;
    }

}
