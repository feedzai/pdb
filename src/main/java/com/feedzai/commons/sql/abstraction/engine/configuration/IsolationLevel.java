/*
 *  The copyright of this file belongs to Feedzai. The file cannot be
 *  reproduced in whole or in part, stored in a retrieval system,
 *  transmitted in any form, or by any means electronic, mechanical,
 *  photocopying, or otherwise, without the prior permission of the owner.
 *
 *  (c) 2014 Feedzai, Strictly Confidential
 */
package com.feedzai.commons.sql.abstraction.engine.configuration;

/**
 * Enumerates the possible isolation levels to use when connecting to the database.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public enum IsolationLevel {
    /**
     * Read uncommitted isolation level.
     */
    READ_UNCOMMITTED,
    /**
     * Read committed isolation level.
     */
    READ_COMMITTED,
    /**
     * Repeatable read isolation level.
     */
    REPEATABLE_READ,
    /**
     * Serializable isolation level.
     */
    SERIALIZABLE;
}
