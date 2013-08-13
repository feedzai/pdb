/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.batch;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;

/**
 * The default batch implementation.
 * <p/>
 * Behind the scenes, it will periodically flush pending statements. It has auto recovery
 * and will never thrown any exception.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @see AbstractBatch
 * @since 13.1.0
 */
public class DefaultBatch extends AbstractBatch {

    /**
     * Creates a new instance of {@link DefaultBatch}.
     *
     * @param de           The database engine reference.
     * @param batchSize    The batch size.
     * @param batchTimeout The timeout.
     */
    protected DefaultBatch(DatabaseEngine de, int batchSize, long batchTimeout) {
        super(de, batchSize, batchTimeout);
    }

    /**
     * <p>Creates a new instance of {@link DefaultBatch}.</p>
     * <p>Starts the timertask.</p>
     *
     * @param de           The database engine.
     * @param batchSize    The batch size.
     * @param batchTimeout The batch timeout.
     * @return The Batch.
     */
    public static DefaultBatch create(final DatabaseEngine de, final int batchSize, final long batchTimeout) {
        final DefaultBatch b = new DefaultBatch(de, batchSize, batchTimeout);
        b.start();

        return b;
    }
}
