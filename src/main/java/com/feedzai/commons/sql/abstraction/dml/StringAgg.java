/*
 * The copyright of this file belongs to Feedzai. The file cannot be
 * reproduced in whole or in part, stored in a retrieval system,
 * transmitted in any form, or by any means electronic, mechanical,
 * photocopying, or otherwise, without the prior permission of the owner.
 *
 * (c) 2018 Feedzai, Strictly Confidential
 */
package com.feedzai.commons.sql.abstraction.dml;

import com.google.inject.Injector;

import javax.inject.Inject;

/**
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.2.3
 */
public final class StringAgg extends Expression {

    /**
     *
     */
    public final Expression column;

    /**
     *
     */
    private char delimiter;


    private boolean distinct;

    /**
     * @param column column to be aggregated.
     */
    private StringAgg(final Expression column) {
        this.column = column;
        this.delimiter = ',';
    }

    /**
     * @param column column to be aggregated.
     * @return a new stringagg.
     */
    public static StringAgg stringAgg(final Expression column) {
        return new StringAgg(column);
    }

    public Expression getColumn() {
        return column;
    }

    public char getDelimiter() {
        return delimiter;
    }

    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }

    public StringAgg distinct(){
        this.distinct = true;
        return this;
    }

    /**
     * @param delimiter char that splits records aggregated.
     * @return
     */
    public StringAgg delimiter(final char delimiter){
        this.delimiter = delimiter;
        return this;
    }
}
