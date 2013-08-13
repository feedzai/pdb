/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.entry;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents an entry.
 */
public class EntityEntry implements Serializable {

    /**
     * The map where column/value pairs are stored.
     */
    private final Map<String, Object> map = new LinkedHashMap<>();

    /**
     * Creates a new entry of {@link EntityEntry}.
     */
    public EntityEntry() {
    }

    /**
     * Sets the specified field.
     *
     * @param k The key.
     * @param v The value.
     * @return This object.
     */
    public EntityEntry set(final String k, final Object v) {
        this.map.put(k, v);

        return this;
    }

    /**
     * Gets the field.
     *
     * @param k The key.
     * @return The value.
     */
    public Object get(final String k) {
        return this.map.get(k);
    }

    /**
     * Gets the a map representation of this {@link EntityEntry}.
     *
     * @return The map representation.
     */
    public Map<String, Object> getMap() {
        return Collections.unmodifiableMap(map);
    }

    @Override
    public String toString() {
        return map.toString();
    }
}
