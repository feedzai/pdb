/*
 * The copyright of this file belongs to Feedzai. The file cannot be
 * reproduced in whole or in part, stored in a retrieval system,
 * transmitted in any form, or by any means electronic, mechanical,
 * photocopying, or otherwise, without the prior permission of the owner.
 *
 * (c) 2018 Feedzai, Strictly Confidential
 */
package com.feedzai.commons.sql.abstraction.dml;

/**
 * @author Francisco Santos (francisco.santos@feedzai.com)
 * @since 2.2.3
 */
public class StringAgg extends Expression {

    /**
     * The column to aggregate.
     */
    public final Expression column;

    /**
     * The delimiter.
     */
    private char delimiter;

    /**
     * Is it distinct.
     */
    private String distinct;

    /**
     * @param column column to be aggregated.
     */
    private StringAgg(final Expression column) {
        this.column = column;
        this.delimiter = ',';
        this.distinct = "";
    }

    /**
     * Returns a new StringAgg.
     *
     * @param column column to be aggregated.
     * @return a new StringAgg.
     */
    public static StringAgg stringAgg(final Expression column) {
        return new StringAgg(column);
    }

    /**
     * Returns the column to be aggregated.
     *
     * @return column to be aggregated.
     */
    public Expression getColumn() {
        return column;
    }

    /**
     * Returns the delimiter.
     *
     * @return the delimiter.
     */
    public char getDelimiter() {
        return delimiter;
    }

    /**
     * Returns if it should apply DISTINCT.
     *
     * @return if it should apply DISTINCT.
     */
    public String getDistinct() {
        return distinct;
    }

    @Override
    public String translate() {
        return translator.translate(this);
    }

    /**
     * Apply distinct.
     *
     * @return this
     */
    public StringAgg distinct() {
        this.distinct = "DISTINCT";
        return this;
    }

    /**
     * Sets the delimiter.
     *
     * @param delimiter char that splits records aggregated.
     * @return this
     */
    public StringAgg delimiter(final char delimiter){
        this.delimiter = delimiter;
        return this;
    }
}
