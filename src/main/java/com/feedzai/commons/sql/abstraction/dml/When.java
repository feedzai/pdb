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

/**
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.2.3
 */
public final class When extends Expression {

    /**
     * The condition to verify.
     */
    public final Expression condition;

    /**
     * The action to be executed if the condition is true.
     */
    public final Expression action;

    /**
     * @param condition condition to verify.
     * @param action action to be executed if the condition is true.
     */
    private When(final Expression condition, final Expression action) {
        this.condition = condition;
        this.action = action;
    }

    /**
     * @param condition condition to verify.
     * @param action action to be executed if the condition is true.
     * @return a new when.
     */
    public static When when(final Expression condition, final Expression action) {
        return new When(condition, action);
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }
}
