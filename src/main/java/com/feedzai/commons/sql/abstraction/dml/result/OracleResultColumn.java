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

import com.feedzai.commons.sql.abstraction.engine.BlobEncoder;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import oracle.sql.BLOB;

/**
 * The Oracle column result implementation.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class OracleResultColumn extends ResultColumn {

    /**
     * Creates a new instance of {@link OracleResultColumn}.
     *
     * @param name The column name.
     * @param val  The value.
     */
    public OracleResultColumn(final String name, final Object val) {
        super(name, val);
    }

    @Override
    public Boolean toBoolean() {
        if (isNull()) {
            return null;
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

    @Override
    public <T> T toBlob() throws DatabaseEngineRuntimeException {
        if (isNull()) {
            return null;
        }

        if (!(val instanceof BLOB)) {
            throw new DatabaseEngineRuntimeException("Column is not a BLOB type");
        }
        try {
            return (T) BlobEncoder.decode(((BLOB) val).getBinaryStream());

        } catch (Exception e) {
            throw new DatabaseEngineRuntimeException("Error converting blob to object", e);
        }
    }

}
