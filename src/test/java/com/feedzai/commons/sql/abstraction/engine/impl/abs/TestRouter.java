/*
 * Copyright 2022 Feedzai
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.feedzai.commons.sql.abstraction.engine.impl.abs;

import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;

/**
 * A class that acts as a router, to forward data sent between a {@link DatabaseEngine} and the DB.
 * <p>
 * When running tests, this offers the possibility of interrupting connections without closing them.
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
public class TestRouter implements Closeable {

    /**
     * Logger for this class.
     */
    private static final Logger logger = LoggerFactory.getLogger(TestRouter.class);

    /**
     * The {@link ServerSocket} that will accept connections from PDB engines to this {@link TestRouter}.
     */
    private final ServerSocket serverSocket = new ServerSocket(0);

    /**
     * The {@link Socket} to connect this {@link TestRouter} to the database.
     */
    private final Socket socketRouterToDb = new Socket();

    /**
     * An {@link ExecutorService} to handle asynchronous tasks.
     */
    private final ExecutorService executor = Executors.newFixedThreadPool(3);

    /**
     * The database port.
     */
    private final int dbPort;

    /**
     * Whether this {@link TestRouter} is running.
     */
    private volatile boolean isRunning = true;

    /**
     * Constructor for this class.
     *
     * @param dbPort   The real DB server port.
     * @throws IOException if some problem occurs creating a new {@link ServerSocket}.
     */
    public TestRouter(final int dbPort) throws IOException {
        this.dbPort = dbPort;
    }

    /**
     * Gets new DB properties with the JDBC URL patched to replace the default DB port with a new one.
     *
     * @param dbProperties The original DB properties.
     * @param defaultPort  The default DB port.
     * @param newPort      The port to use as replacement in the JDBC URL for the default DB port.
     * @return The patched DB properties.
     * @implNote This parses the JDBC URL assuming that the DB host is "localhost".
     */
    public static Properties getPatchedDbProperties(final Properties dbProperties, final int defaultPort, final int newPort) {
        final Properties testProps = new Properties();
        testProps.putAll(dbProperties);

        final String patchedJdbc = dbProperties.getProperty(JDBC)
                .replace("localhost:" + defaultPort, "localhost:" + newPort);
        testProps.setProperty(JDBC, patchedJdbc);

        return testProps;
    }

    /**
     * Gets the local port (that will act as the DB server port for the DB engine).
     *
     * @return the local port.
     */
    public int getLocalPort() {
        return serverSocket.getLocalPort();
    }

    /**
     * Gets the database port (the port where the DB server being tested is listening).
     *
     * @return the database port.
     */
    public int getDbPort() {
        return dbPort;
    }

    /**
     * Initializes connections to/from the DB server.
     *
     * @throws IOException if there is a problem establishing connections.
     */
    public void init() throws IOException {
        socketRouterToDb.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), dbPort));

        final Future<Socket> socketFuture = executor.submit(serverSocket::accept);

        executor.submit(() -> {
                    final Socket socketTestToRouter = socketFuture.get();
                    logger.info("Test-to-DB established");
                    while (isRunning) {
                        int read = socketTestToRouter.getInputStream().read();
                        socketRouterToDb.getOutputStream().write(read);
                    }
                    return null;
                }
        );

        executor.submit(() -> {
                    final Socket socketTestToDb = socketFuture.get();
                    logger.info("DB-to-Test established");
                    while (isRunning) {
                        int read = socketRouterToDb.getInputStream().read();
                        socketTestToDb.getOutputStream().write(read);
                    }
                    return null;
                }
        );
    }

    /**
     * Terminates the forwarding of data to/from the DB server, thus breaking the connections (without closing them).
     */
    public void breakConnections() {
        isRunning = false;
        logger.info("Connections broken");
    }

    @Override
    public void close() {
        closeSilently(socketRouterToDb);
        closeSilently(serverSocket);
        executor.shutdownNow();
    }

    /**
     * Closes an {@link AutoCloseable} and ignores any exceptions.
     *
     * @param autoCloseable The AutoCloseable.
     */
    private void closeSilently(final AutoCloseable autoCloseable) {
        try {
            autoCloseable.close();
        } catch (final Exception e) {
            // ignore
        }
    }
}
