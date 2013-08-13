/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.util;

import com.feedzai.commons.sql.abstraction.ddl.DbColumn;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.impl.DB2Engine;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.MAX_BLOB_SIZE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.VARCHAR_SIZE;
import static java.lang.String.format;

/**
 * Utility to help with column translation to several database vendors.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 13.1.0
 */
public class TypeTranslationUtils {
    /**
     * Translates the given column to a string representation of H2.
     *
     * @param c          The column.
     * @param properties The properties in place.
     * @return The string representation of the type.
     */
    public static String translateH2Type(DbColumn c, PdbProperties properties) {
        switch (c.getDbColumnType()) {
            case BOOLEAN:
                return "BOOLEAN";

            case DOUBLE:
                return "DOUBLE";

            case INT:
                if (c.isAutoInc()) {
                    return "INTEGER AUTO_INCREMENT";
                } else {
                    return "INTEGER";
                }

            case LONG:
                if (c.isAutoInc()) {
                    return "IDENTITY";
                } else {
                    return "BIGINT";
                }

            case STRING:
                return format("VARCHAR(%s)", c.isSizeSet() ? c.getSize().toString() : properties.getProperty(VARCHAR_SIZE));

            case BLOB:
                return "BLOB";

            case CLOB:
                return "CLOB";
            default:
                throw new DatabaseEngineRuntimeException(format("Mapping not found for '%s'. Please report this error.", c.getDbColumnType()));
        }
    }

    /**
     * Translates the given column to a string representation of Oracle.
     *
     * @param c          The column.
     * @param properties The properties in place.
     * @return The string representation of the type.
     */
    public static String translateOracleType(DbColumn c, PdbProperties properties) {
        switch (c.getDbColumnType()) {
            case BOOLEAN:
                return format("char %s check (%s in ('0', '1'))", c.isDefaultValueSet() ? "DEFAULT " + c.getDefaultValue().translateOracle(properties) : "", StringUtil.quotize(c.getName()));

            case DOUBLE:
                return "DOUBLE PRECISION";

            case INT:
                return "INT";

            case LONG:
                return "NUMBER(19,0)";

            case STRING:
                return format("VARCHAR(%s)", c.isSizeSet() ? c.getSize().toString() : properties.getProperty(VARCHAR_SIZE));

            case CLOB:
                return "CLOB";

            case BLOB:
                return "BLOB";

            default:
                throw new DatabaseEngineRuntimeException(format("Mapping not found for '%s'. Please report this error.", c.getDbColumnType()));
        }
    }

    /**
     * Translates the given column to a string representation of SQL Server.
     *
     * @param c          The column.
     * @param properties The properties in place.
     * @return The string representation of the type.
     */
    public static String translateSqlServerType(DbColumn c, PdbProperties properties) {
        switch (c.getDbColumnType()) {
            case BOOLEAN:
                return "BIT";

            case DOUBLE:
                return "DOUBLE PRECISION";

            case INT:
                return "INT";

            case LONG:
                return "BIGINT";

            case STRING:
                return format("NVARCHAR(%s)", c.isSizeSet() ? c.getSize().toString() : properties.getProperty(VARCHAR_SIZE));

            case CLOB:
                return "NVARCHAR(MAX)";

            case BLOB:
                if (properties.isMaxBlobSizeSet()) {
                    return format("VARBINARY(%s)", properties.getProperty(MAX_BLOB_SIZE));
                } else { // Use the default of the buffer, since it can't be greater than that.
                    return format("VARBINARY(MAX)");
                }


            default:
                throw new DatabaseEngineRuntimeException(format("Mapping not found for '%s'. Please report this error.", c.getDbColumnType()));
        }
    }

    /**
     * Translates the given column to a string representation of MySQL.
     *
     * @param c          The column.
     * @param properties The properties in place.
     * @return The string representation of the type.
     */
    public static String translateMySqlType(DbColumn c, PdbProperties properties) {
        switch (c.getDbColumnType()) {
            case BOOLEAN:
                return "BOOLEAN";

            case DOUBLE:
                return "DOUBLE PRECISION";

            case INT:
                return "INT";

            case LONG:
                return "BIGINT";

            case STRING:
                return format("VARCHAR(%s)", c.isSizeSet() ? c.getSize().toString() : properties.getProperty(VARCHAR_SIZE));

            case CLOB:
                return "LONGTEXT";
            case BLOB:
                //return format("VARBINARY(%s)", properties.getProperty(MAX_BLOB_SIZE));
                return format("LONGBLOB");

            default:
                throw new DatabaseEngineRuntimeException(format("Mapping not found for '%s'. Please report this error.", c.getDbColumnType()));
        }
    }

    /**
     * Translates the given column to a string representation of PostgreSQL.
     *
     * @param c          The column.
     * @param properties The properties in place.
     * @return The string representation of the type.
     */
    public static String translatePostgreSqlType(DbColumn c, PdbProperties properties) {
        switch (c.getDbColumnType()) {
            case BOOLEAN:
                return "BOOLEAN";

            case DOUBLE:
                return "DOUBLE PRECISION";

            case INT:
                if (c.isAutoInc()) {
                    return "SERIAL";
                } else {
                    return "INT";
                }

            case LONG:
                if (c.isAutoInc()) {
                    return "BIGSERIAL";
                } else {
                    return "BIGINT";
                }

            case STRING:
                return format("VARCHAR(%s)", c.isSizeSet() ? c.getSize().toString() : properties.getProperty(VARCHAR_SIZE));

            case CLOB:
                return "TEXT";

            case BLOB:
                return format("BYTEA");

            default:
                throw new DatabaseEngineRuntimeException(format("Mapping not found for '%s'. Please report this error.", c.getDbColumnType()));
        }
    }

    /**
     * Translates the given column to a string representation of DB2.
     *
     * @param c          The column.
     * @param properties The properties in place.
     * @return The string representation of the type.
     */
    public static String translateDB2Type(DbColumn c, PdbProperties properties) {
        switch (c.getDbColumnType()) {
            case BOOLEAN:
                return format("char check (%s in ('0', '1'))", StringUtil.quotize(c.getName()));

            case DOUBLE:
                return "DOUBLE PRECISION";

            case INT:
                return "INT";

            case LONG:
                return "NUMERIC(19,0)";

            case STRING:
                return format("VARCHAR(%s)", c.isSizeSet() ? c.getSize().toString() : properties.getProperty(VARCHAR_SIZE));

            /* DB2 does not support CLOB (or at least the Java driver is not implemented. */
            case CLOB:
            case BLOB:
                if (properties.isMaxBlobSizeSet()) {
                    return format("BLOB(%s)", c.isSizeSet() ? c.getSize().toString() : properties.getProperty(MAX_BLOB_SIZE));
                } else {
                    return format("BLOB(%s)", DB2Engine.DB2_DEFAULT_BLOB_SIZE);
                }

            default:
                throw new DatabaseEngineRuntimeException(format("Mapping not found for '%s'. Please report this error.", c.getDbColumnType()));
        }
    }
}
