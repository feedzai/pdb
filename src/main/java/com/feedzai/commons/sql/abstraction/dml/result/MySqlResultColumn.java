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

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;

/**
 * The MySql column result implementation.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class MySqlResultColumn extends ResultColumn {

    /**
     * Creates a new instance of {@link MySqlResultColumn}.
     *
     * @param name The column name.
     * @param val  The value.
     */
    public MySqlResultColumn(final String name, final Object val) {
        super(name, val);
    }

    @Override
    public Boolean toBoolean() {
        if (isNull()) {
            return null;
        }

        /*
        This is needed because when fetching a value from a column of type TINYINT(1), the driver automatically converts
        to Java boolean. When retrieving values from a computed column the driver doesn't know if it's boolean and
        just returns the numeric 1 or 0 values. See the commit message and the test EngineGeneralTest#testCaseToBoolean
        for more details.
         */
        if (super.val instanceof Boolean) {
            return (Boolean) super.val;
        }

        final String val = super.val.toString();

        if (val.equals("1")) {
            return true;
        }

        if (val.equals("0")) {
            return false;
        }

        throw new DatabaseEngineRuntimeException(val + " is not a boolean type");
    }
}
