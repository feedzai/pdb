/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.ddl;

import com.feedzai.commons.sql.abstraction.dml.Expression;
import com.feedzai.commons.sql.abstraction.dml.Name;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.util.MathUtil;
import com.feedzai.commons.sql.abstraction.util.StringUtil;

import static java.lang.String.format;

/**
 * Translates to a statement that allows to drop the primary key of a table.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 13.1.0
 */
public class DropPrimaryKey extends Expression {
    /**
     * the table expression
     */
    private final Expression table;

    /**
     * Creates a new instance of {@link DropPrimaryKey}.
     * @param table The table.
     */
    public DropPrimaryKey(final Expression table) {
        this.table = table;
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        return String.format("ALTER TABLE %s DROP PRIMARY KEY", table.translateDB2(properties));
    }

    @Override
    public String translateOracle(PdbProperties properties) {
        if (!(table instanceof Name)) {
            throw new DatabaseEngineRuntimeException("DropPrimaryKey must receive a Name object");
        }

        final String tableName = ((Name) table).getName();

        final String pkName = MathUtil.md5(format("PK_%s", tableName), properties.getMaxIdentifierSize());

        return String.format("ALTER TABLE %s DROP CONSTRAINT %s", table.translatePostgreSQL(properties), StringUtil.quotize(pkName));
    }

    @Override
    public String translateMySQL(PdbProperties properties) {
        return String.format("ALTER TABLE %s DROP PRIMARY KEY", table.translateMySQL(properties));
    }

    @Override
    public String translateSQLServer(PdbProperties properties) {
        if (!(table instanceof Name)) {
            throw new DatabaseEngineRuntimeException("DropPrimaryKey must receive a Name object");
        }

        final String tableName = ((Name) table).getName();

        final String pkName = MathUtil.md5(format("PK_%s", tableName), properties.getMaxIdentifierSize());

        return String.format("ALTER TABLE %s DROP CONSTRAINT %s", table.translatePostgreSQL(properties), StringUtil.quotize(pkName));
    }

    @Override
    public String translatePostgreSQL(PdbProperties properties) {
        if (!(table instanceof Name)) {
            throw new DatabaseEngineRuntimeException("DropPrimaryKey must receive a Name object");
        }

        final String tableName = ((Name) table).getName();

        final String pkName = MathUtil.md5(format("PK_%s", tableName), properties.getMaxIdentifierSize());

        return String.format("ALTER TABLE %s DROP CONSTRAINT %s", table.translatePostgreSQL(properties), StringUtil.quotize(pkName));
    }

    @Override
    public String translateH2(PdbProperties properties) {
        return String.format("ALTER TABLE %s DROP PRIMARY KEY", table.translateH2(properties));
    }
}
