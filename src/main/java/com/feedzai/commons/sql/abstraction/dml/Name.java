/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.dml;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineImpl;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.impl.MySqlEngine;
import com.feedzai.commons.sql.abstraction.util.StringUtil;
import java.util.ArrayList;
import java.util.List;

/**
 * An object name.
 */
public class Name extends Expression {
    /** The environment. */
    private String environment = null;
    /** The object name. */
    private final String name;
    /** If it is to append IS NULL to the name. */
    private boolean isNull = false;
    /** If it is to append IS NOT NULL to the name. */
    private boolean isNotNull = false;

    /**
     * Creates a new instance of {@link Name}.
     * @param name The object name.
     */
    public Name(final String name) {
        this.name = name;
    }

    /**
     * Creates a new instance of {@link Name}.
     * @param tableName The environment.
     * @param name The name.
     */
    public Name(final String tableName, final String name) {
        this.environment = StringUtil.escapeSql(tableName);
        this.name = StringUtil.escapeSql(name);
    }

    @Override
    public String translateDB2(PdbProperties properties) {
        return getName(DatabaseEngineImpl.ESCAPE_CHARATER);
    }

    @Override
    public String translateOracle(final PdbProperties properties) {
        return getName(DatabaseEngineImpl.ESCAPE_CHARATER);
    }

    @Override
    public String translateMySQL(final PdbProperties properties) {
        return getName(MySqlEngine.ESCAPE_CHARACTER);
    }

    @Override
    public String translateSQLServer(final PdbProperties properties) {
        return getName(DatabaseEngineImpl.ESCAPE_CHARATER);
    }

    @Override
    public String translatePostgreSQL(final PdbProperties properties) {
        return getName(DatabaseEngineImpl.ESCAPE_CHARATER);
    }
    
    @Override
    public String translateH2(PdbProperties properties) {
        return getName(DatabaseEngineImpl.ESCAPE_CHARATER);
    }

    /**
     * Appends "IS NULL" to this field.
     * @return This object.
     */
    public Name isNull() {
        this.isNull = true;

        return this;
    }

    /**
     * Appends "IS NOT NULL" to this field.
     * @return This object.
     */
    public Name isNotNull() {
        this.isNotNull = true;

        return this;
    }

    /**
     * @return The translation of Name.
     */
    private String getName(String quoteCharacter) {
        final List<String> res = new ArrayList<String>();

        if (environment != null) {
            res.add(StringUtil.quotize(environment,quoteCharacter) + "." + (isQuote() ? StringUtil.quotize(name,quoteCharacter) : name));
        } else {
            res.add(isQuote() ? StringUtil.quotize(name,quoteCharacter) : name);
        }

        if (ordering != null) {
            res.add(ordering);
        }

        if (isNull) {
            res.add("IS NULL");
        }

        if (isNotNull) {
            res.add("IS NOT NULL");
        }

        if (isEnclosed()) {
            return "(" + StringUtil.join(res, " ") + ")";
        } else {
            return StringUtil.join(res, " ");
        }
    }

    public String getName(){
        return name;
    }
}
