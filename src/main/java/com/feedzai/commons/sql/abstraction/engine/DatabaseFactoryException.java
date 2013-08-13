/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine;

/**
 * Exceptions related with {@link DatabaseFactory}.
 */
public class DatabaseFactoryException extends Exception {

    /**
     * Creates a new instance of {@link DatabaseFactoryException}.
     */
    public DatabaseFactoryException() { }

    /**
     * Creates a new instance of {@link DatabaseFactoryException}.
     * @param msg The message.
     */
    public DatabaseFactoryException(String msg) {
        super(msg);
    }

    /**
     * Creates a new instance of {@link DatabaseFactoryException}.
     * @param msg The message.
     * @param cause The cause.
     */
    public DatabaseFactoryException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Creates a new instance of {@link DatabaseFactoryException}.
     * @param cause The cause.
     */
    public DatabaseFactoryException(Throwable cause) {
        super(cause);
    }
}
