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

/**
 * A literal to introduce in the SQL statement.
 */
public class Literal extends Expression {

    /** The literal object. */
    private final Object o;

    /**
     * Creates a new instance of {@link Literal}.
     * @param o The literal.
     */
    public Literal(final Object o) {
        this.o = o;
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        return o.toString();
    }

    @Override
    public String translateOracle(final PdbProperties properties) {
        return o.toString();
    }

    @Override
    public String translateMySQL(final PdbProperties properties) {
        return o.toString();
    }

    @Override
    public String translateSQLServer(final PdbProperties properties) {
        return o.toString();
    }

    @Override
    public String translatePostgreSQL(final PdbProperties properties) {
        return o.toString();
    }

    @Override
    public String translateH2(PdbProperties properties) {
        return o.toString();
    }

}
