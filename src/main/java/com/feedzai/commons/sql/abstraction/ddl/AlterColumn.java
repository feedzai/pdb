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
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.util.Constants;
import com.feedzai.commons.sql.abstraction.util.TypeTranslationUtils;
import com.feedzai.commons.sql.abstraction.util.StringUtil;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.util.List;

/**
 * Expression to translate {@code ALTER TABLE... ALTER COLUMN...} statements.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 13.1.0
 */
public class AlterColumn extends Expression {

    /**
     * the table where the column resides
     */
    private final Expression table;
    /**
     * the column definition
     */
    private final DbColumn column;

    /**
     * Creates a new instance of {@link AlterColumn}.
     *
     * @param table  The table where the column resides.
     * @param column The column definition.
     */
    public AlterColumn(Expression table, DbColumn column) {
        this.table = table;
        this.column = column;
    }

    /*
     * Couldn't get more than one DML statement to work in the same JDBC statement.
     * For now we'll use a small hack. A special character that separates both statements.
     * The engine will be responsible for splitting and executing all the statements in
     * separated JDBC statements.
     */
    @Override
    public String translateDB2(PdbProperties properties) {
        // This order avoids a reorg.
        StringBuilder sb = new StringBuilder();

        if (!column.getColumnConstraints().isEmpty()) {
            sb.append("ALTER TABLE ")
                    .append(table.translateDB2(properties))
                    .append(" ALTER COLUMN ")
                    .append(new Name(column.getName()).translateDB2(properties))
                    .append(" SET ");

            List<Object> trans = Lists.transform(column.getColumnConstraints(), new Function<DbColumnConstraint, Object>() {
                @Override
                public Object apply(DbColumnConstraint input) {
                    return input.translate();
                }
            });

            sb.append(StringUtil.join(trans, " "))
                    .append(Constants.UNIT_SEPARATOR_CHARACTER);
        }

        sb.append("ALTER TABLE ")
                .append(table.translateDB2(properties))
                .append(" ALTER COLUMN ")
                .append(new Name(column.getName()).translateDB2(properties))
                .append(" SET DATA TYPE ")
                .append(TypeTranslationUtils.translateDB2Type(column, properties));

        return sb.toString();
    }

    @Override
    public String translateOracle(PdbProperties properties) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(table.translateOracle(properties))
                .append(" MODIFY (")
                .append(new Name(column.getName()).translateOracle(properties))
                .append(" ")
                .append(TypeTranslationUtils.translateOracleType(column, properties))
                .append(" ");

        List<Object> trans = Lists.transform(column.getColumnConstraints(), new Function<DbColumnConstraint, Object>() {
            @Override
            public Object apply(DbColumnConstraint input) {
                return input.translate();
            }
        });


        sb.append(StringUtil.join(trans, " "));
        sb.append(")");


        return sb.toString();
    }

    @Override
    public String translateMySQL(PdbProperties properties) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(table.translateMySQL(properties))
                .append(" MODIFY ")
                .append(new Name(column.getName()).translateMySQL(properties))
                .append(" ")
                .append(TypeTranslationUtils.translateMySqlType(column, properties))
                .append(" ");

        List<Object> trans = Lists.transform(column.getColumnConstraints(), new Function<DbColumnConstraint, Object>() {
            @Override
            public Object apply(DbColumnConstraint input) {
                return input.translate();
            }
        });


        sb.append(StringUtil.join(trans, " "));


        return sb.toString();
    }

    @Override
    public String translateSQLServer(PdbProperties properties) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(table.translateSQLServer(properties))
                .append(" ALTER COLUMN ")
                .append(new Name(column.getName()).translateSQLServer(properties))
                .append(" ")
                .append(TypeTranslationUtils.translateSqlServerType(column, properties))
                .append(" ");

        List<Object> trans = Lists.transform(column.getColumnConstraints(), new Function<DbColumnConstraint, Object>() {
            @Override
            public Object apply(DbColumnConstraint input) {
                return input.translate();
            }
        });

        sb.append(StringUtil.join(trans, " "));

        return sb.toString();
    }

    @Override
    public String translatePostgreSQL(PdbProperties properties) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(table.translatePostgreSQL(properties))
                .append(" ALTER COLUMN ")
                .append(new Name(column.getName()).translatePostgreSQL(properties))
                .append(" TYPE ")
                .append(TypeTranslationUtils.translatePostgreSqlType(column, properties))
                .append("; ");

        if (!column.getColumnConstraints().isEmpty()) {
            sb.append("ALTER TABLE ")
                    .append(table.translatePostgreSQL(properties))
                    .append(" ALTER COLUMN ")
                    .append(new Name(column.getName()).translatePostgreSQL(properties))
                    .append(" SET ");

            List<Object> trans = Lists.transform(column.getColumnConstraints(), new Function<DbColumnConstraint, Object>() {
                @Override
                public Object apply(DbColumnConstraint input) {
                    return input.translate();
                }
            });

            sb.append(StringUtil.join(trans, " "));
        }


        return sb.toString();
    }

    @Override
    public String translateH2(PdbProperties properties) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(table.translateH2(properties))
                .append(" ALTER COLUMN ")
                .append(new Name(column.getName()).translateH2(properties))
                .append(" ")
                .append(TypeTranslationUtils.translateH2Type(column, properties))
                .append(" ");

        List<Object> trans = Lists.transform(column.getColumnConstraints(), new Function<DbColumnConstraint, Object>() {
            @Override
            public Object apply(DbColumnConstraint input) {
                return input.translate();
            }
        });

        sb.append(StringUtil.join(trans, " "));

        return sb.toString();
    }
}
