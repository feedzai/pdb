/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.dml.dialect;

/**
 * Enumeration to indicate the type of dialect.
 */
public enum Dialect {
    /**
     * Oracle SQL dialect.
     */
    ORACLE,
    /**
     * PostgreSQL SQL dialect.
     */
    POSTGRESQL,
    /**
     * MySQL SQL dialect.
     */
    MYSQL,
    /**
     * SQLServer SQL dialect.
     */
    SQLSERVER,
    /**
     * H2 SQL dialect.
     */
    H2,
    /**
     * DB2 SQL dialect.
     */
    DB2;
}
