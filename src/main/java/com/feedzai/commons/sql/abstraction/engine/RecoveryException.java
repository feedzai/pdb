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
 * Exceptions related with {@link RecoveryException}.
 */
public class RecoveryException extends Exception {

    /**
     * Creates a new instance of {@link RecoveryException}.
     */
    public RecoveryException() { }

    /**
     * Creates a new instance of {@link RecoveryException}.
     * @param msg The message.
     */
    public RecoveryException(String msg) {
        super(msg);
    }

    /**
     * Creates a new instance of {@link RecoveryException}.
     * @param msg The message.
     * @param cause The cause.
     */
    public RecoveryException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Creates a new instance of {@link RecoveryException}.
     * @param cause The cause.
     */
    public RecoveryException(Throwable cause) {
        super(cause);
    }
}
