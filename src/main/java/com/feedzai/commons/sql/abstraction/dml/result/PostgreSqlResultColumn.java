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
package com.feedzai.commons.sql.abstraction.dml.result;

import org.postgresql.util.PGobject;

/**
 * The PostgreSql column result implementation.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class PostgreSqlResultColumn extends ResultColumn {

    /**
     * Creates a new instance of {@link PostgreSqlResultColumn}.
     *
     * @param name The column name.
     * @param val  The value.
     */
    public PostgreSqlResultColumn(final String name, final Object val) {
        super(name, val);
    }

    /**
     * Overrides default behaviour for JSON values, that are converted to strings.
     *
     * @param o The object in need of some kind of processing before being set.
     * @return  The processed object.
     * @since 2.1.5
     */
    @Override
    protected Object processObject(Object o) {
        if (o instanceof PGobject &&
                        ((PGobject)o).getType().equals("jsonb")) {
            return ((PGobject) o).getValue();
        }
        return super.processObject(o);
    }
}
