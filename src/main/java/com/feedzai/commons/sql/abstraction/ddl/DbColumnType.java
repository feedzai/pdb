/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.ddl;

/**
 * The column types.
 */
public enum DbColumnType {
    /** The boolean type. */
    BOOLEAN,
    /** The string type. */
    STRING,
    /** The integer type. */
    INT,
    /** The double type. */
    DOUBLE,
    /** The long type. */
    LONG,
    /** The blob type. */
    BLOB,
    /** The clob type */
    CLOB,
    /** A type that is not mapped. */
    UNMAPPED;
}
