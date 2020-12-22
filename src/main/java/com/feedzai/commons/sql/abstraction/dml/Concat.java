/*
 * The copyright of this file belongs to Feedzai. The file cannot be
 * reproduced in whole or in part, stored in a retrieval system,
 * transmitted in any form, or by any means electronic, mechanical,
 * photocopying, or otherwise, without the prior permission of the owner.
 *
 * © 2020 Feedzai, Strictly Confidential
 */
package com.feedzai.commons.sql.abstraction.dml;

import java.util.Collection;

/**
 * PDB Concat Operator.
 *
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since @@@feedzai.next.release@@@
 */
public final class Concat extends Expression {

    /**
     * The concatenation delimiter.
     */
    private final Expression delimiter;

    /**
     * The expressions to concatenate.
     */
    private final Collection<Expression> expressions;

    /**
     * Creates a new {@link Concat} object.
     *
     * @param delimiter The concatenation delimiter.
     * @param expressions The expressions to concatenate.
     */
    public Concat(final Expression delimiter, final Collection<Expression> expressions) {
        this.delimiter = delimiter;
        this.expressions = expressions;
    }

    /**
     * Adds an expression to concatenate.
     *
     * @param expression The expression to concatenate.
     */
    public Concat with(final Expression expression) {
        expressions.add(expression);
        return this;
    }

    /**
     * Gets the concatenation delimiter.
     *
     * @return the concatenation delimiter.
     */
    public Expression getDelimiter() {
        return delimiter;
    }

    /**
     * Gets the expressions to concatenate.
     *
     * @return the expressions to concatenate.
     */
    public Collection<Expression> getExpressions() {
        return expressions;
    }

    @Override
    public String translate() {
        return translator.translate(this);

        /*injector.injectMembers(delimiter);

        */
    }
}
