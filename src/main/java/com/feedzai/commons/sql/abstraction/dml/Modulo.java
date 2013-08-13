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
 * The MOD operator.
 */
public class Modulo extends Expression {
    /** The dividend. */
    private final Expression dividend;
    /** The AND expression. */
    private final Expression divisor;

    /**
     * Creates a new instance of {@link Modulo}.
     * @param dividend The dividend.
     * @param divisor The divisor.
     */
    public Modulo(final Expression dividend, final Expression divisor) {
        this.dividend = dividend;
        this.divisor = divisor;
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        String result = String.format("MOD(%s, %s)",dividend.translateDB2(properties), divisor.translateDB2(properties));

        return result;
    }

    @Override
    public String translateOracle(PdbProperties properties) {
        String result = String.format("MOD(%s, %s)",dividend.translateOracle(properties), divisor.translateOracle(properties));

        return result;
    }

    @Override
    public String translateMySQL(PdbProperties properties) {
        String result = String.format("MOD(%s, %s)",dividend.translateMySQL(properties), divisor.translateMySQL(properties));

        return result;
    }

    @Override
    public String translateSQLServer(PdbProperties properties) {
        String result = String.format("%s %% %s",dividend.translateSQLServer(properties), divisor.translateSQLServer(properties));

        return isEnclosed() ? ("("+result+")"): result;
    }

    @Override
    public String translatePostgreSQL(PdbProperties properties) {
        String result = String.format("MOD(%s, %s)",dividend.translatePostgreSQL(properties), divisor.translatePostgreSQL(properties));

        return result;
    }

    @Override
    public String translateH2(PdbProperties properties) {
        String result = String.format("MOD(%s, %s)",dividend.translateH2(properties), divisor.translateH2(properties));

        return result;
    }
}
