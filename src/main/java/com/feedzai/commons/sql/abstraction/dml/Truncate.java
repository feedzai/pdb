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
import com.feedzai.commons.sql.abstraction.util.StringUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * The Truncate operator.
 */
public class Truncate extends Expression {
    /** The Table. */
    private final Expression table;

    /**
     * Creates a new instance of {@link com.feedzai.commons.sql.abstraction.dml.Truncate}.
     * @param table The table.
     */
    public Truncate(final Expression table) {
        this.table = table;
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("TRUNCATE TABLE");
        temp.add(table.translateDB2(properties));
        temp.add("IMMEDIATE");

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translateOracle(final PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("TRUNCATE TABLE");
        temp.add(table.translateOracle(properties));

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translateMySQL(final PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("TRUNCATE TABLE");
        temp.add(table.translateMySQL(properties));

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translateSQLServer(final PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("TRUNCATE TABLE");
        temp.add(table.translateSQLServer(properties));

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translatePostgreSQL(final PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("TRUNCATE TABLE");
        temp.add(table.translatePostgreSQL(properties));

        return StringUtil.join(temp, " ");
    }

    @Override
    public String translateH2(PdbProperties properties) {
        final List<String> temp = new ArrayList<String>();

        temp.add("TRUNCATE TABLE");
        temp.add(table.translateH2(properties));

        return StringUtil.join(temp, " ");
    }
}
