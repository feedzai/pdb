/*
 * Copyright 2014 Feedzai
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

import com.feedzai.commons.sql.abstraction.ddl.DbEntity;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngine;
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineDriver;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.all;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.entry;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.select;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.table;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ISOLATION_LEVEL;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
@RunWith(Parameterized.class)
public class EngineIsolationTest {
    protected Properties properties;

    @Parameterized.Parameters
    public static Collection<DatabaseConfiguration> data() throws Exception {
        return DatabaseTestUtil.loadConfigurations();
    }

    @Parameterized.Parameter
    public DatabaseConfiguration config;

    @Before
    public void init() {
        this.properties = new Properties() {
            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "drop-create");
            }
        };
    }

    @Test
    public void readCommittedTest() throws DatabaseFactoryException {
        properties.setProperty(ISOLATION_LEVEL, "read_committed");
        DatabaseFactory.getConnection(properties);
    }

    @Test
    public void readUncommittedTest() throws DatabaseFactoryException {
        this.properties.setProperty(ISOLATION_LEVEL, "read_uncommitted");
        DatabaseFactory.getConnection(properties);
    }

    @Test
    public void repeatableReadTest() throws DatabaseFactoryException {
        this.properties.setProperty(ISOLATION_LEVEL, "repeatable_read");
        DatabaseFactory.getConnection(properties);
    }

    @Test
    public void serializableTest() throws DatabaseFactoryException {
        this.properties.setProperty(ISOLATION_LEVEL, "serializable");
        DatabaseFactory.getConnection(properties);
    }

    public void limitsTest() throws Exception {
        final PdbProperties pdbProps = new PdbProperties(this.properties, true);

        final Connection conn = DriverManager.getConnection(
                pdbProps.getJdbc(),
                pdbProps.getUsername(),
                pdbProps.getPassword()
        );

        conn.setAutoCommit(true);

        try (final Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE test (col1 VARCHAR, col2 VARCHAR)");
        }

        try (final PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO test VALUES(?, ?)")) {
            for (int i = 1; i <= 10; i++) {
                preparedStatement.setString(1, "c1val" + i);
                preparedStatement.setString(2, "c2val" + i);
                preparedStatement.executeQuery();
            }
        }

        //conn.setAutoCommit(false);

    }

    /**
     * Tests whether the current DB engine in the default isolation level (usually "read committed") will cause
     * deadlocks when there are concurrent transactions acquiring DB locks (writes) and Java locks in different order.
     * <p>
     * Besides testing current DB engines with default isolation level, this test will allow verification of new DB
     * engines, and with small modifications, different isolation levels.
     *
     * The following table describes the sequence of events of both transactions used in the test. A deadlock may occur
     * at T4 if the "SELECT *" query in Transaction 2 gets blocked by the previous persist operation in Transaction 1:
     * Transaction 2 gets blocked until Transaction 1 commits or rolls back, but Transaction 1 can only advance when it
     * is able to acquire the Java lock (which is only released after Transaction2 advances).
     * <table>
     *     <tr><th>time</th><th>transaction 1</th><th>transaction 2</th></tr>
     *     <tr><td>T0</td><td>begin</td></tr>
     *     <tr><td>T1</td><td>persist (DB write lock)</td></tr>
     *     <tr><td>T2</td><td/><td>begin</td></tr>
     *     <tr><td>T3</td><td/><td>acquire Java lock</td></tr>
     *     <tr><td>T4</td><td>try acquire Java lock</td><td>query select *</td></tr>
     *     <tr><td>T5</td><td></td><td>commit</td></tr>
     *     <tr><td>T6</td><td></td><td>release Java lock</td></tr>
     *     <tr><td>T7</td><td>acquire Java lock</td><td></td></tr>
     *     <tr><td>T8</td><td>commit</td></tr>
     *     <tr><td>T9</td><td>release Java lock</td></tr>
     * </table>
     * @throws Exception if something goes wrong in the test.
     * @since 2.4.7
     */
    @Test
    public void deadlockTransactionTest() throws Exception {
        final StampedLock lock = new StampedLock();
        final Phaser phaser = new Phaser(1);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);

        final DbEntity entity = dbEntity().name("TEST")
                .addColumn("COL1", INT, true)
                .addColumn("COL2", INT)
                .pkFields("COL1")
                .build();

        final DatabaseEngine engine = DatabaseFactory.getConnection(properties);
        engine.updateEntity(entity);
        final DatabaseEngine engine2 = DatabaseFactory.getConnection(properties);
        engine2.updateEntity(entity);

        engine.persist("TEST", entry().set("COL2", 1).build());

        // start Transaction 1
        final Future<?> tx1 = executorService.submit(() -> {
            engine.beginTransaction();
            try {
                final Long persist = engine.persist("TEST", entry().set("COL2", 2).build());
                assertNotNull("Persist in Transaction 1 should be successful", persist);

                // arrive phase 0 - begin phase 1 (start Transaction 2)
                phaser.arrive();
                // wait for phase 1 to complete - Transaction 2 should have began and acquired the Java lock
                phaser.awaitAdvance(1);

                lock.writeLockInterruptibly();

                engine.commit();
            } catch (final Exception ex) {
                throw new RuntimeException("Error occurred on Transaction 1", ex);
            } finally {
                if (engine.isTransactionActive()) {
                    engine.rollback();
                }
                lock.tryUnlockWrite();

                // arrive phase 2 - begin phase 3 (proceed to end of test)
                phaser.arrive();
            }
        });

        // wait for phase 0 to complete - Transaction 1 should have began and persisted an entry to DB (acquiring a DB write lock)
        phaser.awaitAdvance(0);

        // start Transaction 2
        final Future<?> tx2 = executorService.submit(() -> {
            try {
                engine2.beginTransaction();

                long tstamp = lock.readLockInterruptibly();
                if (isRealSerializableLevel(engine2)) {
                    tstamp = lock.tryConvertToOptimisticRead(tstamp);
                }

                // arrive phase 1 - begin phase 2 (transaction 1 can now try to acquire the lock,
                // only being able to do so after it is unlocked below)
                phaser.arrive();

                // possible deadlock occurs on the next line if the engine blocks reads after a write in another transaction
                engine2.query(select(all()).from(table("TEST")));

                engine2.commit();
            } catch (final Exception ex) {
                throw new RuntimeException("Error occurred on Transaction 2", ex);
            } finally {
                if (engine2.isTransactionActive()) {
                    engine2.rollback();
                }
                // at this point, if we're using only optimistic read on the lock, we would be validating the timestamp;
                // if invalid (i.e. a write occurred in the meantime) then we would retry the code inside the lock
                lock.tryUnlockRead();
            }
        });

        // wait for phase 1 to complete (no issues are expected up to this point;
        // only on the next test phase a deadlock might prevent a phase advance and timeout on the next wait point below)
        phaser.awaitAdvance(1);
        final int finalPhase;
        try {
            finalPhase = phaser.awaitAdvanceInterruptibly(2, 5, TimeUnit.SECONDS);
        } catch (final Exception ex) {
            fail("The transaction threads are deadlocked");
            throw ex;
        }
        assertEquals(
                "Both transactions should have completed successfully, causing the main test thread to arrive at phase 3 of the test",
                3, finalPhase
        );

        assertThatCode(() -> tx1.get(1, TimeUnit.SECONDS))
                .as("Code for Transaction 1 shouldn't have thrown any exceptions")
                .doesNotThrowAnyException();

        assertThatCode(() -> tx2.get(1, TimeUnit.SECONDS))
                .as("Code for Transaction 2 shouldn't have thrown any exceptions")
                .doesNotThrowAnyException();

        executorService.shutdownNow();
        engine.close();
        engine2.close();
    }

    /**
     * Checks whether
     *
     * @param engine
     * @return
     * @throws Exception if something goes wrong in the test.
     * @since 2.4.7
     */
    protected boolean isRealSerializableLevel(final DatabaseEngine engine) throws Exception {
        final PdbProperties pdbProps = new PdbProperties(this.properties, true);
        final DatabaseEngineDriver engineDriver = DatabaseEngineDriver.fromEngine(pdbProps.getEngine());

        switch (engine.getConnection().getTransactionIsolation()) {
            case Connection.TRANSACTION_SERIALIZABLE:
                if (engineDriver == DatabaseEngineDriver.SQLSERVER) {
                    return true;
                }
            default:
                return false;
        }
    }
}
