/*
 * Copyright 2014 Feedzai
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.feedzai.commons.sql.abstraction.entry;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents an entry, i.e. the columns that are part of an entry.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class EntityEntry implements Serializable {

    /**
     * The map that holds the columns.
     */
    private final Map<String, Object> map;

    /**
     * Creates a new entry of {@link EntityEntry}.
     *
     * @param map The map of the entry.
     */
    private EntityEntry(LinkedHashMap<String, Object> map) {
        this.map = map;
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
     * Returns a new Builder out of this {@link EntityEntry}.
     *
     * @return A new Builder out of this {@link EntityEntry}.
     */
    public Builder newBuilder() {
        return new Builder().set(map);
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

    /**
     * Builder to create immutable {@link EntityEntry} objects.
     */
    public static class Builder implements com.feedzai.commons.sql.abstraction.util.Builder<EntityEntry>, Serializable {
        private final Map<String, Object> map = new LinkedHashMap<>();

        /**
         * Adds the specified key/value pair.
         *
         * @param k The key.
         * @param v The value.
         * @return This builder.
         */
        public Builder set(final String k, final Object v) {
            this.map.put(k, v);

            return this;
        }

        /**
         * Adds all the entries in given map to the entity entry.
         *
         * @param map The map to add.
         * @return This builder.
         */
        public Builder set(Map<String, Object> map) {
            this.map.putAll(map);

            return this;
        }

        /**
         * Gets the value given the pair.
         *
         * @param key The key.
         * @return The object hold by the key.
         */
        public Object get(String key) {
            return this.map.get(key);
        }

        @Override
        public EntityEntry build() {
            return new EntityEntry(new LinkedHashMap<>(map));
        }
    }
}
