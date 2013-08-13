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
 * Expression to rename tables.
 *
 * @author Diogo Ferreira.
 *
 * Note: This is not a DML operation. In the future this must be moved from here.
 */
public class Rename extends Expression {
    private final Expression oldName;
    private final Expression newName;

    /**
     * Creates a new instance of {@link com.feedzai.commons.sql.abstraction.dml.Rename}.
     *
     * @param oldName The old table name.
     * @param newName The new table name.
     */
    public Rename(final Expression oldName, final Expression newName) {
        this.oldName = oldName;
        this.newName = newName;
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        return String.format("RENAME TABLE %s TO %s", oldName.translateDB2(properties), newName.translateDB2(properties));
    }

    @Override
    public String translateOracle(PdbProperties properties) {
        return String.format("ALTER TABLE %s RENAME TO %s", oldName.translateOracle(properties), newName.translateOracle(properties));
    }

    @Override
    public String translateMySQL(PdbProperties properties) {
        return String.format("RENAME TABLE %s TO %s", oldName.translateMySQL(properties), newName.translateMySQL(properties));
    }

    @Override
    public String translateSQLServer(PdbProperties properties) {
        return String.format("sp_rename %s, %s", oldName.translateSQLServer(properties), newName.translateSQLServer(properties));
    }

    @Override
    public String translatePostgreSQL(PdbProperties properties) {
        return String.format("ALTER TABLE %s RENAME TO %s", oldName.translatePostgreSQL(properties), newName.translatePostgreSQL(properties));
    }

    @Override
    public String translateH2(PdbProperties properties) {
        return String.format("ALTER TABLE %s RENAME TO %s", oldName.translateH2(properties), newName.translateH2(properties));
    }
}
