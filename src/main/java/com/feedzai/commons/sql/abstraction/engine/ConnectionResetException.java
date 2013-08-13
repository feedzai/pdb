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
 * Exceptions related with {@link ConnectionResetException}.
 */
public class ConnectionResetException extends Exception {

    /**
     * Creates a new instance of {@link ConnectionResetException}.
     */
    public ConnectionResetException() { }

    /**
     * Creates a new instance of {@link ConnectionResetException}.
     * @param msg The message.
     */
    public ConnectionResetException(String msg) {
        super(msg);
    }

    /**
     * Creates a new instance of {@link ConnectionResetException}.
     * @param msg The message.
     * @param cause The cause.
     */
    public ConnectionResetException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Creates a new instance of {@link ConnectionResetException}.
     * @param cause The cause.
     */
    public ConnectionResetException(Throwable cause) {
        super(cause);
    }
}
