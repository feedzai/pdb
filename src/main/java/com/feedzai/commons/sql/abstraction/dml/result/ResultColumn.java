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

import java.io.*;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * Abstract result column to be extended by specific implementations.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public abstract class ResultColumn implements Serializable {
    /**
     * The value of the column.
     */
    protected final Object val;
    /**
     * The name of the column.
     */
    protected final String name;

    /**
     * Creates a new instance of {@link ResultColumn}.
     *
     * @param name The column name.
     * @param val  The column value.
     */
    public ResultColumn(final String name, final Object val) {
        this.name = name;
        this.val = processObject(val);
    }

    /**
     * Processes an object. This method is to be overwritten by implementations that need to do some processing before the {@link java.sql.ResultSet}
     * closes.
     *
     * @param o The object in need of some kind of processing before being set.
     * @return The processed object.
     * @see DB2ResultColumn
     */
    protected Object processObject(Object o) {
        return o;
    }

    /**
     * Checks if the value is null.
     *
     * @return True if the value is null, false otherwise.
     */
    public boolean isNull() {
        return val == null;
    }

    /**
     * @return The name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return This value in the form of a String.
     */
    @Override
    public String toString() {
        if (isNull()) {
            return null;
        }
        if (val instanceof Clob) {
            return getStringFromClob((Clob) val);
        } else {
            return val.toString();
        }
    }

    /**
     * @return This value in the form of an Integer.
     */
    public Integer toInt() {
        if (isNull()) {
            return null;
        }

        return (int) Double.parseDouble(val.toString());
    }

    /**
     * @return This value in the form of a Double.
     */
    public Double toDouble() {
        if (isNull()) {
            return null;
        }

        return Double.parseDouble(val.toString());
    }

    /**
     * @return This value in the form of a Float.
     */
    public Float toFloat() {
        if (isNull()) {
            return null;
        }

        return Float.parseFloat(val.toString());
    }

    /**
     * @return This value in the form of a Long.
     */
    public Long toLong() {
        if (isNull()) {
            return null;
        }

        try {
            // In most cases the value either doesn't have decimals or they
            // are zero (e.g., 13.0), and in this case Long.parseLong() is ok.
            return Long.parseLong(val.toString());

        } catch (final NumberFormatException e) {
            // We get here if the double has decimal digits (e.g, 13.5) and in this
            // case there is no precision overflow on using an intermediate Double
            // before because it means the value was not stored as a long.
            return (long) Double.parseDouble(val.toString());
        }
    }

    /**
     * @return The object value.
     */
    public Object toObject() {
        if (isNull()) {
            return null;
        }


        return val;
    }

    /**
     * @return This value in the form of a Boolean.
     */
    public Boolean toBoolean() {
        if (isNull()) {
            return null;
        }

        return (Boolean) val;
    }

    /**
     * Converts this result (in the form of blob) to the specified object type.
     *
     * @param <T> The type to convert.
     * @return The instance that was in the form of blob.
     * @throws DatabaseEngineRuntimeException If the value is not a blob or if
     *                                        something goes wrong when reading the object.
     */
    public <T> T toBlob() throws DatabaseEngineRuntimeException {
        if (isNull()) {
            return null;
        }

        InputStream is;
        if (val instanceof Blob) {
            try {
                is = ((Blob) val).getBinaryStream();
            } catch (final SQLException e) {
                throw new DatabaseEngineRuntimeException("Error getting blob input stream", e);
            }
        } else if (val instanceof byte[]) {
            is = new ByteArrayInputStream((byte[]) val);
        } else {
            throw new DatabaseEngineRuntimeException("Column is not a Blob neither a byte[]");
        }

        try {
            ObjectInputStream ois = new ObjectInputStream(is);
            return (T) ois.readObject();
        } catch (final Exception e) {
            throw new DatabaseEngineRuntimeException("Error converting blob to object", e);
        }
    }

    /**
     * Gets the clob object as a string.
     *
     * @return The string representation of the clob object.
     */
    private String getStringFromClob(final Clob clob) {
        try {
            Reader reader = clob.getCharacterStream();
            char[] buff = new char[512];
            int read;
            StringBuilder result = new StringBuilder();
            while ((read = reader.read(buff)) != -1) {
                result.append(buff, 0, read);
            }
            return result.toString();
        } catch (final Exception ex) {
            throw new DatabaseEngineRuntimeException("Unable to get string from clob", ex);
        }
    }
}
