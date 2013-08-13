/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.dml.result;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineRuntimeException;
import java.io.*;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * Abstract result column.
 */
public abstract class ResultColumn implements Serializable {

    /** The value of the column.*/
    protected final Object val;
    /** The name of the column. */
    protected final String name;

    /**
     * Creates a new instance of {@link ResultColumn}.
     * @param name The column name.
     * @param val The column value.
     */
    public ResultColumn(final String name, final Object val) {
        this.name = name;
        this.val = processObject(val);
    }

    /**
     * Processes an object. This method is to be overwritten by implementations that need to do some processing before the {@link java.sql.ResultSet}
     * closes.
     * @see DB2ResultColumn
     * @param o The object in need of some kind of processing before being set.
     * @return The processed object.
     */
    protected Object processObject(Object o) {
        return o;
    }

    /**
     * Checks if the value is null.
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

        return (long) Double.parseDouble(val.toString());
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
     * @param <T> The type to convert.
     * @return The instance that was in the form of blob.
     * @throws DatabaseEngineRuntimeException If the value is not a blob or if
     * something goes wrong when reading the object.
     */
    public <T> T toBlob() throws DatabaseEngineRuntimeException {
        if (isNull()) {
            return null;
        }

        InputStream is;
        if (val instanceof Blob) {
            try {
                is = ((Blob) val).getBinaryStream();
            } catch (SQLException e) {
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
        } catch (Exception e) {
            throw new DatabaseEngineRuntimeException("Error converting blob to object", e);
        }
    }

    /**
     * Reads the as a string from the clob object.
     * @return a string with clob's content
     */
    private String getStringFromClob(final Clob clob) {
        try {
            Reader reader = clob.getCharacterStream();
            char[] buff = new char[512];
            int read;
            StringBuilder result = new StringBuilder();
            while((read = reader.read(buff)) != -1) {
                result.append(buff, 0, read);
            }
            return result.toString();
        } catch (Exception ex) {
            throw new DatabaseEngineRuntimeException("Unable to get string from clob", ex);
        }
    }
}
