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
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import java.util.*;

/**
 * A Batch that periodically flushes pending insertions to the database.
 * <p/>
 * Extending classes that want to be notified when a flush could not be performed after the timeout has been reached,
 * must override the {@link #onFlushFailure(BatchEntry[])}.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 13.1.0
 */
public abstract class AbstractBatch extends TimerTask {

    /**
     * The logger.
     */
    protected final Logger logger = LoggerFactory.getLogger(AbstractBatch.class);
    /**
     * The dev Marker.
     */
    protected final static Marker dev = MarkerFactory.getMarker("DEV");
    /**
     * Salt to avoid erroneous flushes.
     */
    protected static final int salt = 100;
    /**
     * The database engine.
     */
    protected final DatabaseEngine de;
    /**
     * The Timer that runs this task.
     */
    protected final Timer timer;
    /**
     * The batchSize.
     */
    protected final int batchSize;
    /**
     * The batch timeout.
     */
    protected final long batchTimeout;
    /**
     * The batch at the present moment.
     */
    protected int batch;
    /**
     * Timestamp of the last flush.
     */
    protected long lastFlush;
    /**
     * EntityEntry buffer.
     */
    protected LinkedList<BatchEntry> buffer = new LinkedList<>();

    /**
     * Creates a new instance of {@link AbstractBatch}.
     *
     * @param de           The database engine.
     * @param batchSize    The batch size.
     * @param batchTimeout The batch timeout.
     */
    protected AbstractBatch(final DatabaseEngine de, final int batchSize, final long batchTimeout) {
        this.timer = new Timer();
        this.de = de;
        this.batchSize = batchSize;
        this.batch = batchSize;
        this.batchTimeout = batchTimeout;
        this.lastFlush = System.currentTimeMillis();
    }

    /**
     * Starts the timer task.
     */
    protected void start() {
        timer.scheduleAtFixedRate(this, 0, batchTimeout + salt);
    }

    /**
     * Destroys this batch.
     */
    public synchronized void destroy() {
        logger.trace("Destroy called on Batch");
        cancel();
        flush();
        timer.cancel();
        timer.purge();
    }

    /**
     * Adds the fields to the batch.
     *
     * @param batchEntry The batch entry.
     * @throws DatabaseEngineException If an error with the database occurs.
     */
    public synchronized void add(BatchEntry batchEntry) throws DatabaseEngineException {
        buffer.add(batchEntry);
        batch--;

        if (batch == 0) {
            flush();
        }
    }

    /**
     * Adds the fields to the batch.
     *
     * @param entityName The table name.
     * @param ee         The entity entry.
     * @throws DatabaseEngineException If an error with the database occurs.
     */
    public synchronized void add(final String entityName, final EntityEntry ee) throws DatabaseEngineException {
        add(new BatchEntry(entityName, ee));
    }

    /**
     * Flushes the pending batches.
     */
    public synchronized void flush() {
        // If something goes wrong we still have a copy to recover.
        final List<BatchEntry> temp = new ArrayList<>();

        try {
            final long start = System.currentTimeMillis();
            while (!buffer.isEmpty()) {
                BatchEntry entry = buffer.poll();
                temp.add(entry);
                de.addBatch(entry.getTableName(), entry.getEntityEntry());
            }

            de.beginTransaction();

            try {
                de.flush();
                de.commit();
                logger.trace("Batch flushed. Took {} ms, {} rows ", (System.currentTimeMillis() - start), (batchSize - batch));
                batch = batchSize;
                lastFlush = System.currentTimeMillis();
            } finally {
                if (de.isTransactionActive()) {
                    de.rollback();
                }
            }
        } catch (Exception e) {
            logger.error(dev, "Error occurred while flushing.", e);
            /*
             * We cannot try any recovery here because we don't know why it failed. If it failed by a Constraint
             * violation for instance, we cannot try it again and again because it will always fail.
             *
             * The idea here is to hand the entries that could not be added and continue right where we were.
             *
             * So, temp will only have the entries until it failed, this can be all the entries or until de.addBatch() failed;
             * and buffer will have the events right after the failure.
             */
            onFlushFailure(temp.toArray(new BatchEntry[]{}));
        }

    }

    /**
     * Notifies about the pending entries on flush failure.
     *
     * @param entries The entries that are pending to be persisted.
     */
    public void onFlushFailure(BatchEntry[] entries) {

    }

    @Override
    public synchronized void run() {
        if (System.currentTimeMillis() - lastFlush >= batchTimeout && batch != batchSize) {
            logger.trace("Flush timeout occurred");
            flush();
        }
    }

    @Override
    public void finalize() throws Throwable {
        super.finalize();
        logger.trace(this + " died");
    }
}
