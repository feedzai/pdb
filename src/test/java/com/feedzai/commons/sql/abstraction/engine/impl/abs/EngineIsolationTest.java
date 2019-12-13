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
import com.feedzai.commons.sql.abstraction.engine.DatabaseEngineException;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactory;
import com.feedzai.commons.sql.abstraction.engine.DatabaseFactoryException;
import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseConfiguration;
import com.feedzai.commons.sql.abstraction.engine.testconfig.DatabaseTestUtil;
import com.feedzai.commons.sql.abstraction.entry.EntityEntry;
import org.assertj.core.api.ThrowableAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;

import static com.feedzai.commons.sql.abstraction.ddl.DbColumnType.INT;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.all;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.column;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.count;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.dbEntity;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.entry;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.eq;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.k;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.select;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.table;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.update;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ENGINE;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.ISOLATION_LEVEL;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.JDBC;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.PASSWORD;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.SCHEMA_POLICY;
import static com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties.USERNAME;
import static com.feedzai.commons.sql.abstraction.util.Constants.RETRYABLE_EXCEPTIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
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

    /**
     * The {@link DatabaseEngineDriver} corresponding to the current {@link #config test config}.
     */
    private DatabaseEngineDriver engineDriver;

    /**
     * A list of actions to perform when a test finishes.
     */
    private List<ThrowableAssert.ThrowingCallable> closeActions = new ArrayList<>();

    @Before
    public void init() {
        this.properties = new Properties() {
            {
                setProperty(JDBC, config.jdbc);
                setProperty(USERNAME, config.username);
                setProperty(PASSWORD, config.password);
                setProperty(ENGINE, config.engine);
                setProperty(SCHEMA_POLICY, "create");
            }
        };

        final PdbProperties pdbProps = new PdbProperties(this.properties, true);
        this.engineDriver = DatabaseEngineDriver.fromEngine(pdbProps.getEngine());
    }

    @After
    public void cleanup() {
        this.closeActions.forEach(ThrowableAssert::catchThrowable);
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

    /**
     * Tests whether the current DB engine in the default isolation level (usually "read committed") will cause
     * deadlocks when there are concurrent transactions acquiring DB locks (writes) and Java locks in different order.
     * <p>
     * Besides testing current DB engines with default isolation level, this test will allow verification of new DB
     * engines, and with small modifications, different isolation levels.
     * <p>
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
     *
     * @throws Exception if something goes wrong in the test.
     * @since 2.4.7
     */
    @Test
    public void deadlockTransactionTest() throws Exception {
        final StampedLock lock = new StampedLock();
        final Phaser phaser = new Phaser(1);

        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        this.closeActions.add(executorService::shutdownNow);

        final DbEntity entity = dbEntity().name("TEST")
                .addColumn("COL1", INT, true)
                .addColumn("COL2", INT)
                .pkFields("COL1")
                .build();

        final DatabaseEngine engine = DatabaseFactory.getConnection(properties);
        engine.dropEntity(entity);
        engine.updateEntity(entity);
        final DatabaseEngine engine2 = engine.duplicate(null, true);

        this.closeActions.add(engine::close);
        this.closeActions.add(engine2::close);

        // persist an initial entry
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

        final AtomicInteger countResult = new AtomicInteger();
        // start Transaction 2
        final Future<?> tx2 = executorService.submit(() -> {
            try {
                engine2.beginTransaction();

                long tstamp = lock.readLockInterruptibly();
                if (isRealSerializableLevel(engine2)) {
                    lock.tryConvertToOptimisticRead(tstamp);
                }

                // arrive phase 1 - begin phase 2 (transaction 1 can now try to acquire the lock,
                // only being able to do so after it is unlocked below)
                phaser.arrive();

                // possible deadlock occurs on the next line if the engine blocks reads after a write in another transaction
                final Integer result = engine2.query(select(count(all()).alias("testcount"))
                        .from(table("TEST")))
                        .get(0)
                        .get("testcount")
                        .toInt();
                countResult.set(result);

                engine2.commit();
            } catch (final Exception ex) {
                throw new RuntimeException("Error occurred on Transaction 2", ex);
            } finally {
                if (engine2.isTransactionActive()) {
                    engine2.rollback();
                }
                // at this point, if using only optimistic read, we would be validating the timestamp;
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

        /*
         check that the SELECT query returned a correct result (and indirectly, if the write completed)
          - if using a "real" SERIALIZABLE isolation level, the SELECT should wait for the write operation on the DB to
            complete (when using Java optimistic read lock), thus the result should reflect 2 rows
          - if not using a "real" SERIALIZABLE isolation level, the SELECT should complete before the write in the other
            transaction, thus seeing only the initial persisted row
         */
        assertThat(countResult)
                .as("The SELECT query should return  correct result (2 for \"real\" SERIALIZABLE isolation level, 1 otherwise)")
                .hasValue(isRealSerializableLevel(engine) ? 2 : 1);
    }

    /**
     * Returns whether the current engine is using a "real" SERIALIZABLE isolation level (the supported DB engines
     * oconfigured to be SERIALIZABLE are in fact normally using snapshot isolation).
     *
     * @param engine the {@link DatabaseEngine} to get isolation level information from.
     * @return whether the current engine is using a "real" SERIALIZABLE isolation level.
     * @throws Exception if something goes wrong in the verification.
     * @since 2.4.7
     */
    private boolean isRealSerializableLevel(final DatabaseEngine engine) throws Exception {
        if (engine.getConnection().getTransactionIsolation() == Connection.TRANSACTION_SERIALIZABLE) {
            return engineDriver == DatabaseEngineDriver.SQLSERVER || engineDriver == DatabaseEngineDriver.COCKROACHDB;
        }
        return false;
    }

    /**
     * This test causes deadlocks using concurrent transactions and verifies that those deadlocks are detected by the
     * database, PDB correctly signals such errors as retryable, and finally that when the failed transactions are
     * retried they are successful.
     * <p>
     * The actions to perform can be selects, updates or persists (each verification uses a pair of actions in which at
     * least one must perform a write on the DB).
     * Each transaction is ran on its own DB engine.
     * Additionally, the test can run with first transaction (transaction 0) finishing either before or after the second
     * action on the second transaction runs.
     * <p>
     * The following table describes the sequence of events of both transactions used in the test:
     * <table>
     *     <tr><th>time</th><th>transaction 0</th><th>transaction 1</th></tr>
     *     <tr><td>T0</td><td>BEGIN</td><td>BEGIN</td></tr>
     *     <tr><td>T1</td><td>action1 on TEST0</td><td>action1 on TEST1</td></tr>
     *     <tr><td>T2</td><td>action2 on TEST1</td></tr>
     *     <tr>** if transaction 0 waits for transaction 1</tr>
     *     <tr><td>T3</td><td/><td>action2 on TEST0</td></tr>
     *     <tr><td>T4</td><td>COMMIT</td></tr>
     *     <tr><td>T5</td><td></td><td>COMMIT (fails?)</td></tr>
     *     <tr>** if transaction 0 commits before action2 on transaction 1</tr>
     *     <tr><td>T3</td><td>COMMIT</td></tr>
     *     <tr><td>T4</td><td/><td>action2 on TEST0 (fails?)</td></tr>
     * </table>
     * The COMMIT/action2 may fail or not at the indicated timestamps, depending on the database.
     * Some database may block on some of the actions until the other interfering transaction either finishes (commit)
     * or fails (e.g. a deadlock is detected by the database) - hence the use of asynchronous actions.
     * <p>
     * Finally, the test will check that the failed actions return an exception that is "retryable".
     * After that, the failed transaction is rolled back and ran again from the beginning.
     * In the end, the test checks if the tables have the expected data.
     *
     * @throws Exception if a problem occurs in the test.
     * @since 2.5.1
     */
    @Test
    public void deadlockRecoveryTest() throws Exception {
        final DbEngineAction selectAction = (engine, tableIdx) -> engine.query(select(all()).from(table("TEST" + tableIdx)));

        final DbEngineAction updateAction = (engine, tableIdx) -> engine.executeUpdate(
                update(table("TEST" + tableIdx))
                        .set(eq(column("COL2"), k(tableIdx + 1)))
                        .where(eq(column("COL1"), k(1)))
        );

        final DbEngineAction persistAction = (engine, tableIdx) -> engine.persist(
                "TEST" + tableIdx, entry().set("COL2", tableIdx + 1).build()
        );

        Integer[][] expected = new Integer[][]{{1, 1}, {2, 2}};
        runDeadlockExceptionTest(false, updateAction, persistAction, expected);
        runDeadlockExceptionTest(true, updateAction, persistAction, expected);

        expected = new Integer[][]{{0, 1}, {0, 2}};

        runDeadlockExceptionTest(false, selectAction, persistAction, expected);
        runDeadlockExceptionTest(true, selectAction, persistAction, expected);

        runDeadlockExceptionTest(false, persistAction, selectAction, expected);
        runDeadlockExceptionTest(true, persistAction, selectAction, expected);
    }

    /**
     * This method runs the actions for {@link #deadlockRecoveryTest()}.
     *
     * @param waitAction2FromEngine2 Whether engine/transaction 0 waits for action 2 to execute on engine/transaction 1
     *                               before proceeding to commit.
     * @param action1                The first action to run after beginning transaction.
     * @param action2                The second action to run after beginning transaction.
     * @param expected               The expected values (first dimension corresponds to the table index, second
     *                               dimension contains the several expected values for that particular table).
     * @throws Exception if a problem occurs in the test.
     * @since 2.5.1
     */
    private void runDeadlockExceptionTest(final boolean waitAction2FromEngine2,
                                          final DbEngineAction action1,
                                          final DbEngineAction action2,
                                          final Integer[][] expected) throws Exception {
        final DatabaseEngine[] engines = prepareEngineForDeadlockRecoveryTest();
        final ExecutorService executorService = Executors.newCachedThreadPool();
        this.closeActions.add(executorService::shutdownNow);

        for (int i = 0; i < engines.length; i++) {
            engines[i].beginTransaction();
            action1.runFor(engines[i], i);
        }

        final CompletableFuture<Boolean> success0Future = CompletableFuture.supplyAsync(
                () -> runAction(engines[0], () -> action2.runFor(engines[0], 1)),
                executorService
        );

        final AtomicReference<CompletableFuture<Boolean>> success1FutureRef = new AtomicReference<>();
        if (waitAction2FromEngine2) {
            success1FutureRef.set(CompletableFuture.supplyAsync(
                    () -> runAction(engines[1], () -> action2.runFor(engines[1], 0)),
                    executorService
            ));

            catchThrowable(() -> success1FutureRef.get().get(5, TimeUnit.SECONDS));
        }

        final CompletableFuture<Boolean> success0Future1 = success0Future.thenApplyAsync(
                success0 -> success0 && runAction(engines[0], engines[0]::commit),
                executorService
        );

        catchThrowable(() -> success0Future1.get(5, TimeUnit.SECONDS));

        boolean success1;
        if (waitAction2FromEngine2) {
            success1 = success1FutureRef.get().get(5, TimeUnit.SECONDS);
        } else {
            success1 = runAction(engines[1], () -> action2.runFor(engines[1], 0));
        }

        if (success1) {
            success1 = runAction(engines[1], engines[1]::commit);
        }

        if (!success1) {
            repeatTransaction(engines, 1, action1, action2);
        } else if (!Boolean.TRUE.equals(success0Future1.get(5, TimeUnit.SECONDS))) {
            repeatTransaction(engines, 0, action1, action2);
        }

        assertEntityValues(engines[0], expected);

        for (final DatabaseEngine engine : engines) {
            catchThrowable(engine::close);
        }
    }

    /**
     * This test causes deadlocks using concurrent transactions and verifies that those deadlocks are detected by the
     * database, PDB correctly signals such errors as retryable, and finally that when the failed transactions are
     * retried they are successful.
     *
     * This test is similar to {@link #deadlockRecoveryTest()}, but both actions used in this test perform writes (they
     * are both UPDATEs) and always act on the same row (identified by COL1=1).
     * Each transaction is ran on its own DB engine.
     * By using only writes and directly interfering on row level, this test may cause slightly different outcomes
     * depending on the database (on PostgreSQL this results in a 40P01 error instead of 40001; on Oracle it results in
     * ORA-08177 instead of ORA-00060).
     * <p>
     * The following table describes the sequence of events of both transactions used in the test:
     * <table>
     *     <tr><th>time</th><th>transaction 0</th><th>transaction 1</th></tr>
     *     <tr><td>T0</td><td>BEGIN</td><td>BEGIN</td></tr>
     *     <tr><td>T1</td><td>update on TEST0</td><td>update on TEST1</td></tr>
     *     <tr><td>T2</td><td>update on TEST1</td></tr>
     *     <tr><td>T3</td><td>COMMIT</td></tr>
     *     <tr><td>T4</td><td/><td>update on TEST0</td></tr>
     *     <tr><td>T5</td><td></td><td>COMMIT</td></tr>
     * </table>
     * The actions on T2 to T5 are ran asynchronously and may occur in a different order than what's shown in the table.
     * Different databases may block and/or fail at different points.
     * Like in {@link #deadlockRecoveryTest()}, the test will check that the failed actions return an exception that
     * is "retryable" and will rollback and retry those.
     * In the end, the test checks if the tables have the expected data.
     *
     * @throws Exception if a problem occurs in the test.
     * @since 2.5.1
     */
    @Test
    public void directDeadlockRecoveryTest() throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        final DbEngineAction updateAction = (engine, tableIdx) -> engine.executeUpdate(
                update(table("TEST" + tableIdx))
                        .set(eq(column("COL2"), k(counter.incrementAndGet())))
                        .where(eq(column("COL1"), k(1)))
        );

        final DatabaseEngine[] engines = prepareEngineForDeadlockRecoveryTest();
        final ExecutorService executorService = Executors.newCachedThreadPool();
        this.closeActions.add(executorService::shutdownNow);

        final CompletableFuture<Boolean>[] successFutures = new CompletableFuture[2];
        for (int i = 0; i < engines.length; i++) {
            engines[i].beginTransaction();
            final int idx = i;
            successFutures[i] = CompletableFuture.supplyAsync(
                    () -> runAction(engines[idx], () -> updateAction.runFor(engines[idx], idx)),
                    executorService
            );
        }

        successFutures[0] = successFutures[0]
                .thenApplyAsync(
                        success0 -> success0 && runAction(engines[0], () -> updateAction.runFor(engines[0], 1)),
                        executorService
                )
                .thenApplyAsync(
                        success0 -> success0 && runAction(engines[0], engines[0]::commit),
                        executorService
                );

        catchThrowable(() -> successFutures[0].get(5, TimeUnit.SECONDS));

        successFutures[1] = successFutures[1]
                .thenApplyAsync(
                        success0 -> success0 && runAction(engines[1], () -> updateAction.runFor(engines[1], 0)),
                        executorService
                )
                .thenApplyAsync(
                        success0 -> success0 && runAction(engines[1], engines[1]::commit),
                        executorService
                );

        catchThrowable(() -> successFutures[0].get(5, TimeUnit.SECONDS));

        // retry failed transactions
        for (int i = 0; i < successFutures.length; i++) {
            if (!successFutures[i].get(5, TimeUnit.SECONDS)) {
                repeatTransaction(engines, i, updateAction, updateAction);
            }
        }

        final Integer[][] expected = successFutures[0].get() ? new Integer[][]{{6}, {5}} : new Integer[][]{{5}, {6}};
        assertEntityValues(engines[0], expected);
    }

    /**
     * Prepares 2 {@link DatabaseEngine}s for deadlock recovery tests.
     * <p>
     * This method sets the session isolation level to SERIALIZABLE so that the expected deadlocks can occur and creates
     * 2 tables (TEST0 and TEST1) with an initial entry persisted.
     *
     * @return an array of {@link DatabaseEngine}.
     * @throws Exception if a problem occurs in the preparation.
     * @since 2.5.1
     */
    protected DatabaseEngine[] prepareEngineForDeadlockRecoveryTest() throws Exception {
        this.properties.setProperty(ISOLATION_LEVEL, "serializable");

        final DbEntity.Builder entityBuilder = dbEntity()
                .addColumn("COL1", INT, true)
                .addColumn("COL2", INT)
                .pkFields("COL1");

        final DatabaseEngine[] engines = new DatabaseEngine[2];
        engines[0] = DatabaseFactory.getConnection(properties);
        final EntityEntry entry = entry().set("COL1", 1).set("COL2", 0).build();

        for (int i = 0; i < 2; i++) {
            final DbEntity entity0 = entityBuilder.name("TEST" + i).build();
            engines[0].dropEntity(entity0);
            engines[0].updateEntity(entity0);
            engines[0].persist(entity0.getName(), entry);
        }

        engines[1] = engines[0].duplicate(null, true);

        for (final DatabaseEngine engine : engines) {
            this.closeActions.add(engine::close);

            if (this.engineDriver.equals(DatabaseEngineDriver.MYSQL)) {
                // MySQL is too slow detecting deadlocks by default (default is 50 seconds)
                // for the purpose of the test, it can be reduced
                engine.executeUpdate("SET SESSION innodb_lock_wait_timeout = 1");
            }
        }

        return engines;
    }

    /**
     * Runs an action on a database (for use in deadlock recovery tests).
     * <p>
     * If the action fails, this method asserts that the resulting exception is retryable, and if so confirms that the
     * transaction is still active and rolls it back.
     *
     * @param engine The DB engine to use to run the action.
     * @param action The action to perform.
     * @return {@code true} if the action succeeded, {@code false} otherwise.
     * @since 2.5.1
     */
    private boolean runAction(final DatabaseEngine engine, final ThrowableAssert.ThrowingCallable action) {
        final Throwable throwable = catchThrowable(action);

        if (throwable == null) {
            return true;
        }

        assertThat(throwable)
                .as("If a DB action fails due to deadlock, it should indicate it is retryable")
                .isInstanceOfAny(RETRYABLE_EXCEPTIONS.toArray(new Class[0]));

        assertTrue("A transaction failed due to deadlock should still be active and needs to be rolled back",
                engine.isTransactionActive());
        engine.rollback();

        return false;
    }

    /**
     * Repeats a transaction that failed due to deadlock in a deadlock recovery test.
     *
     * @param engines   The engines setup by {@link #prepareEngineForDeadlockRecoveryTest()}.
     * @param engineIdx The index of the engine to be used to repeat the transaction.
     * @param action1   The first action to run after beginning transaction.
     * @param action2   The second action to run after beginning transaction.
     * @throws Exception if a problem occurs in the test.
     * @since 2.5.1
     */
    private void repeatTransaction(final DatabaseEngine[] engines,
                                   final int engineIdx,
                                   final DbEngineAction action1,
                                   final DbEngineAction action2) throws Exception {
        final DatabaseEngine engine = engines[engineIdx];

        engine.beginTransaction();
        action1.runFor(engine, engineIdx);
        action2.runFor(engine, 1 - engineIdx);
        engine.commit();
    }

    /**
     * Performs an assertion on the values present in the tables setup by {@link #prepareEngineForDeadlockRecoveryTest()}.
     *
     * @param dbEngine The DB engine to use to get the values in the tables.
     * @param expected The expected values (first dimension corresponds to the table index, second dimension contains
     *                 the several expected values for that particular table).
     * @throws DatabaseEngineException if a problem occurs running the query to get data for the assertion.
     * @since 2.5.1
     */
    private static void assertEntityValues(final DatabaseEngine dbEngine,
                                           final Integer[][] expected) throws DatabaseEngineException {
        for (int i = 0; i < 1; i++) {
            assertThat(dbEngine.query(select(column("COL2")).from(table("TEST" + i))))
                    .as("COL2 in 'TEST%d' should have the expected value", i)
                    .extracting(res -> res.get("COL2").toInt())
                    .containsExactly(expected[i]);
        }
    }

    /**
     * Represents an action to be performed on the database that can throw any exception.
     * This is meant to be used in the deadlock recovery tests, with the DB engines as setup by
     * {@link #prepareEngineForDeadlockRecoveryTest()}.
     *
     * @since 2.5.1
     */
    @FunctionalInterface
    private interface DbEngineAction {
        /**
         * Executes the action using the specified engine in the table referred to by the provided table index.
         */
        void runFor(DatabaseEngine engine, int tableIdx) throws Exception;
    }
}
