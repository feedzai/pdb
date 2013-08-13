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
 * The database entity types.
 */
public enum DbEntityType {
    /**
     * The table type.
     */
    TABLE,
    /**
     * The view type.
     */
    VIEW,
    /**
     * The system table type.
     */
    SYSTEM_TABLE,
    /**
     * The system view type.
     */
    SYSTEM_VIEW,
    /**
     * A type that is not mapped.
     */
    UNMAPPED;
}
