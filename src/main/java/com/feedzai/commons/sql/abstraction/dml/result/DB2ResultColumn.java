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
import java.io.ObjectInputStream;
import java.sql.Blob;

/**
 * The DB2 column result implementation.
 * ATTENTION: Blob columns must be processed at instantiation because the engine closes the result set and
 * the data won't be accessible hence the {@link DB2ResultColumn#processObject(Object)} implementation.
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

    // This 'emulates' the CLOB. CLOB like objects in DB do not exists and PdB internally converts them to byte[] and then
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
