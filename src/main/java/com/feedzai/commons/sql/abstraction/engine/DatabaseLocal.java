/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine;

import com.feedzai.commons.sql.abstraction.dml.Expression;
import com.feedzai.commons.sql.abstraction.dml.result.ResultIterator;
import java.sql.Connection;

/**
 * <p>
 * Provides a set of functions to interact with the database.
 * </p>
 */
public interface DatabaseLocal extends DatabaseEngine {
    /**
     * Checks if the connection is available and returns it. If the connection is not available, it tries to reconnect
     * (the number of times defined in the properties with the delay there specified).
     *
     * @return The connection.
     * @throws RetryLimitExceededException If the retry limit is exceeded.
     * @throws InterruptedException        If the thread is interrupted during reconnection.
     */
    Connection getConnection() throws RetryLimitExceededException, InterruptedException, RecoveryException;

    /**
     * Creates an iterator for the given SQL sentence.
     *
     * @param query The query.
     * @return An iterator for the results of the given SQL query.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    ResultIterator iterator(final String query) throws DatabaseEngineException;

    /**
     * Creates an iterator for the given SQL expression.
     *
     * @param query The expression that represents the query.
     * @return An iterator for the results of the given SQL expression.
     * @throws DatabaseEngineException If a database access error occurs.
     */
    ResultIterator iterator(final Expression query) throws DatabaseEngineException;

    /**
     * Creates an iterator for the {@link java.sql.PreparedStatement} bound to the given name.
     *
     * @param name The name of the prepared statement.
     * @return An iterator for the results of the prepared statement of the given name.
     * @throws DatabaseEngineException
     */
    ResultIterator getPSIterator(final String name) throws DatabaseEngineException;

}
