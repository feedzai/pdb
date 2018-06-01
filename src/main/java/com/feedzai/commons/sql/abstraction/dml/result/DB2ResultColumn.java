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

import java.io.ObjectInputStream;
import java.sql.Blob;

/**
 * The DB2 column result implementation.
 * <p/>
 * ATTENTION: Blob columns must be processed at instantiation because the engine closes the result set and
 * the data won't be accessible hence the {@link DB2ResultColumn#processObject(Object)} implementation.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public class DB2ResultColumn extends ResultColumn {
    /**
     * Creates a new instance of {@link com.feedzai.commons.sql.abstraction.dml.result.DB2ResultColumn}.
     *
     * @param name The column name.
     * @param val  The value.
     */
    public DB2ResultColumn(final String name, final Object val) {
        super(name, val);
    }

    @Override
    protected Object processObject(Object o) {
        if (o instanceof Blob) {
            try {
                return new ObjectInputStream(((Blob) o).getBinaryStream()).readObject();
            } catch (Exception e) {
                throw new DatabaseEngineRuntimeException("Error eagerly converting blob to object", e);
            }
        }

        return o;
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

    // When this method is called the conversion is already performed by the reasons explained earlier.
    @Override
    public <T> T toBlob() throws DatabaseEngineRuntimeException {
        if (isNull()) {
            return null;
        }

        return (T) val;
    }

    // This 'emulates' the CLOB. CLOB like objects in DB2 do not exist and PDB internally converts them to byte[] and then
    // instantiates a string with the fetched bytes.
    @Override
    public String toString() {
        if (isNull()) {
            return null;
        }

        if (val instanceof byte[]) {
            return new String((byte[]) val);
        }

        return super.toString();
    }
}
