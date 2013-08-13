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
import oracle.sql.BLOB;
import java.io.ObjectInputStream;

/**
 * The Oracle column result implementation.
 */
public class OracleResultColumn extends ResultColumn {

    /**
     * Creates a new instance of {@link OracleResultColumn}.
     * @param name The column name.
     * @param val The value.
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
            ObjectInputStream ois = new ObjectInputStream(((BLOB) val).getBinaryStream());

            return (T) ois.readObject();
        } catch (Exception e) {
            throw new DatabaseEngineRuntimeException("Error converting blob to object", e);
        }
    }

}
