/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine.configuration;

/**
 * Indicates when a configuration has errors.
 */
public class PdbConfigurationException extends Exception {
    /**
     * Creates a new instance of {@link PdbConfigurationException}.
     */
    public PdbConfigurationException() { }

    /**
     * Creates a new instance of {@link PdbConfigurationException}.
     * @param msg The message.
     */
    public PdbConfigurationException(String msg) {
        super(msg);
    }

    /**
     * Creates a new instance of {@link PdbConfigurationException}.
     * @param msg The message.
     * @param cause The cause.
     */
    public PdbConfigurationException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
