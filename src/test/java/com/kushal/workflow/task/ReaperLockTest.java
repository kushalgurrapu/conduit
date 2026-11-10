package com.kushal.workflow.task;

import com.kushal.workflow.support.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lock races between the reap and the other writers of a {@code RUNNING} row.
 *
 * <p>The reap locks with {@code FOR UPDATE SKIP LOCKED}. A row held by an
 * uncommitted heartbeat or finish is skipped instead of waited for, and it
 * does not use up {@code LIMIT}. Once that heartbeat or finish commits, the
 * re-check keeps the row out of the next reap. A finish or heartbeat that
 * arrives while a reap is uncommitted waits, which {@code pg_blocking_pids}
 * confirms, and then matches nothing. Cancel is guarded by {@code PENDING},
 * so during an uncommitted reap it sees {@code RUNNING} and does not wait.
 *
 * <p>Holders are raw connections or latched transactions, committed or
 * rolled back in {@code finally}. Every reaping transaction used as a holder
 * or probe runs {@code SET LOCAL statement_timeout = '2s'} first, so a reap
 * that waits fails instead of hanging. Mutation check: with
 * {@code SKIP LOCKED} removed from the reap statement, the
 * uncommitted-heartbeat tests fail on that timeout (SQLState 57014).
 *
 * <p>The table-lock test is the one exception. Its reap is called through
 * the proxy with no outer transaction and no statement timeout, because it
 * shows that {@code @Transactional(timeout = 5)} reaches the statement and
 * fires before the container's 10 s {@code lock_timeout}.
 */
@SpringBootTest
class ReaperLockTest extends PostgresIntegrationTest {

    private static final Instant FIXTURE_TIME = Instant.parse("2000-01-01T00:00:00Z");
    private static final String STATEMENT_TIMEOUT = "2s";
    private static final String WAITER_STATEMENT_TIMEOUT = "8s";
    private static final int WAIT_SECONDS = 7;

    @Autowired
    private TaskService taskService;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void uncommittedHeartbeatOnTheOldestRowIsSkippedThenItsCommittedLeaseKeepsTheRow() throws Exception {
        reapAroundUncommittedHeartbeat(true);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void uncommittedHeartbeatOnTheOldestRowIsSkippedThenAfterRollbackTheNextReapTakesIt() throws Exception {
        reapAroundUncommittedHeartbeat(false);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void uncommittedCompleteIsSkippedThenTheCommittedRowIsNotReaped() throws Exception {
        UUID workerA = UUID.randomUUID();
        Task claimed = claimExpired(workerA, FIXTURE_TIME);

        Connection holder = dataSource.getConnection();
        holder.setAutoCommit(false);
        boolean committed = false;
        try {
            holdUncommittedComplete(holder, claimed.id(), workerA, claimed.attempt());

            assertEquals(List.of(), reapWithStatementTimeout(10));
            assertEquals("RUNNING", statusOf(claimed.id()));

            holder.commit();
            committed = true;

            assertEquals(List.of(), reapWithStatementTimeout(10));
            assertEquals("COMPLETED", statusOf(claimed.id()));
        } finally {
            release(holder, committed);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void finishAndExtendWaitingOnAnUncommittedReapMatchNothingAfterItCommits() throws Exception {
        UUID workerA = UUID.randomUUID();
        Task toFinish = claimExpired(workerA, FIXTURE_TIME);
        Task toExtend = claimExpired(workerA, FIXTURE_TIME);

        HeldReap reap = holdReap(10);
        Call<TransitionResult> finish = null;
        Call<Boolean> extend = null;
        try {
            reap.awaitReaped();
            assertEquals(Set.of(toFinish.id(), toExtend.id()), ids(reap.batch().get()));

            finish = startInTransaction("finish-vs-reap", WAITER_STATEMENT_TIMEOUT,
                    () -> taskService.completeTask(toFinish.id(), workerA, toFinish.attempt(), "{\"late\":true}"));
            extend = startInTransaction("extend-vs-reap", WAITER_STATEMENT_TIMEOUT,
                    () -> taskService.extendLease(toExtend.id(), workerA, toExtend.attempt()));
            assertTrue(finish.pidReady().await(WAIT_SECONDS, TimeUnit.SECONDS), "finish did not start");
            assertTrue(extend.pidReady().await(WAIT_SECONDS, TimeUnit.SECONDS), "extend did not start");
            assertTrue(awaitBlocked(finish.pid().get(), reap.pid().get()),
                    "finish should wait until the uncommitted reap releases the row");
            assertTrue(awaitBlocked(extend.pid().get(), reap.pid().get()),
                    "extend should wait until the uncommitted reap releases the row");
            assertEquals("RUNNING", statusOf(toFinish.id()));
            assertEquals("RUNNING", statusOf(toExtend.id()));

            reap.commit();

            finish.awaitFinished();
            extend.awaitFinished();
            reap.awaitFinished();
            assertEquals(new TransitionResult.Rejected(TaskStatus.PENDING, null), finish.result().get());
            assertEquals(Boolean.FALSE, extend.result().get());
            for (Task claimed : List.of(toFinish, toExtend)) {
                Task pending = taskService.getTask(claimed.id());
                assertEquals(TaskStatus.PENDING, pending.status());
                assertEquals(List.of(new TaskAttempt(
                        claimed.id(), 1, workerA, claimed.claimedAt(), pending.updatedAt(),
                        AttemptOutcome.LEASE_EXPIRED, null)
                ), taskService.getAttempts(claimed.id()));
            }
        } finally {
            reap.rollbackUnlessCommitted();
            join(reap.thread());
            join(finish);
            join(extend);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void cancelDoesNotWaitOnAnUncommittedReap() throws Exception {
        UUID workerA = UUID.randomUUID();
        Task claimed = claimExpired(workerA, FIXTURE_TIME);

        HeldReap reap = holdReap(10);
        Call<CancelResult> cancel = null;
        try {
            reap.awaitReaped();

            cancel = startInTransaction("cancel-vs-reap", STATEMENT_TIMEOUT,
                    () -> taskService.cancelTask(claimed.id()));
            assertTrue(cancel.pidReady().await(WAIT_SECONDS, TimeUnit.SECONDS), "cancel did not start");
            boolean blocked = false;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
            while (cancel.finished().getCount() > 0 && System.nanoTime() < deadline) {
                blocked |= isBlocked(cancel.pid().get(), reap.pid().get());
                Thread.sleep(10);
            }
            cancel.awaitFinished();
            assertFalse(blocked, "cancel must not wait on the uncommitted reap");
            assertEquals(new CancelResult.NotCancellable(TaskStatus.RUNNING), cancel.result().get());

            reap.commit();
            reap.awaitFinished();

            CancelResult.Cancelled cancelled = assertInstanceOf(
                    CancelResult.Cancelled.class, taskService.cancelTask(claimed.id()));
            assertEquals(TaskStatus.CANCELLED, cancelled.task().status());
            assertEquals(List.of(AttemptOutcome.LEASE_EXPIRED),
                    taskService.getAttempts(claimed.id()).stream().map(TaskAttempt::outcome).toList());
        } finally {
            reap.rollbackUnlessCommitted();
            join(reap.thread());
            join(cancel);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void reapBlockedByATableLockFailsWithinTheTransactionTimeout() throws Exception {
        UUID workerA = UUID.randomUUID();
        Task claimed = claimExpired(workerA, FIXTURE_TIME);

        Connection holder = dataSource.getConnection();
        holder.setAutoCommit(false);
        try {
            try (Statement statement = holder.createStatement()) {
                statement.execute("LOCK TABLE tasks IN ACCESS EXCLUSIVE MODE");
            }

            long started = System.nanoTime();
            DataAccessException thrown = assertThrows(
                    DataAccessException.class, () -> taskService.reapExpiredLeases(10));
            Duration elapsed = Duration.ofNanos(System.nanoTime() - started);

            assertTrue(elapsed.compareTo(Duration.ofSeconds(4)) >= 0 && elapsed.compareTo(Duration.ofSeconds(9)) < 0,
                    "the 5 s transaction timeout should fire before the 10 s lock_timeout, took " + elapsed);
            SQLException cause = assertInstanceOf(SQLException.class, thrown.getMostSpecificCause());
            assertEquals("57014", cause.getSQLState(), "a query cancel, not lock_timeout (55P03)");

            holder.rollback();

            assertEquals(List.of(new ReapedTask(claimed.id(), 1, workerA, true)), taskService.reapExpiredLeases(10));
        } finally {
            release(holder, false);
        }
    }

    private void reapAroundUncommittedHeartbeat(boolean commitHeartbeat) throws Exception {
        UUID workerA = UUID.randomUUID();
        Task oldest = claimExpired(workerA, FIXTURE_TIME);
        Task middle = claimExpired(workerA, FIXTURE_TIME.plusSeconds(1));
        Task newest = claimExpired(workerA, FIXTURE_TIME.plusSeconds(2));

        Connection holder = dataSource.getConnection();
        holder.setAutoCommit(false);
        boolean committed = false;
        try {
            holdUncommittedHeartbeat(holder, oldest.id(), workerA, oldest.attempt());

            List<ReapedTask> reaped = reapWithStatementTimeout(2);

            assertEquals(Set.of(middle.id(), newest.id()), ids(reaped),
                    "the locked oldest row must not use up LIMIT 2");
            assertEquals("RUNNING", statusOf(oldest.id()));

            if (commitHeartbeat) {
                holder.commit();
                committed = true;

                assertEquals(List.of(), reapWithStatementTimeout(2));
                Task kept = taskService.getTask(oldest.id());
                assertEquals(TaskStatus.RUNNING, kept.status());
                assertEquals(workerA, kept.workerId());
                assertNotNull(kept.lastHeartbeatAt());
                assertTrue(kept.leaseExpiresAt().isAfter(kept.lastHeartbeatAt()));
            } else {
                holder.rollback();

                assertEquals(List.of(new ReapedTask(oldest.id(), 1, workerA, true)), reapWithStatementTimeout(2));
            }
        } finally {
            release(holder, committed);
        }
    }

    private List<ReapedTask> reapWithStatementTimeout(int batchSize) {
        return new TransactionTemplate(transactionManager).execute(status -> {
            jdbcClient.sql("SET LOCAL statement_timeout = '" + STATEMENT_TIMEOUT + "'").update();
            return taskService.reapExpiredLeases(batchSize);
        });
    }

    /**
     * Reaps on its own thread and keeps the transaction open until
     * {@link HeldReap#commit()} or {@link HeldReap#rollbackUnlessCommitted()}.
     */
    private HeldReap holdReap(int batchSize) {
        CountDownLatch reaped = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicBoolean commitRequested = new AtomicBoolean();
        AtomicInteger pid = new AtomicInteger();
        AtomicReference<List<ReapedTask>> batch = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
                    jdbcClient.sql("SET LOCAL statement_timeout = '" + STATEMENT_TIMEOUT + "'").update();
                    pid.set(jdbcClient.sql("SELECT pg_backend_pid()").query(Integer.class).single());
                    batch.set(taskService.reapExpiredLeases(batchSize));
                    reaped.countDown();
                    try {
                        if (!release.await(20, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("held reap was never released");
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("held reap was interrupted", interrupted);
                    }
                    if (!commitRequested.get()) {
                        status.setRollbackOnly();
                    }
                });
            } catch (Throwable thrown) {
                error.set(thrown);
            } finally {
                reaped.countDown();
                finished.countDown();
            }
        }, "held-reap");
        thread.start();
        return new HeldReap(thread, reaped, release, finished, commitRequested, pid, batch, error);
    }

    private <T> Call<T> startInTransaction(String name, String statementTimeout, Supplier<T> action) {
        CountDownLatch pidReady = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicInteger pid = new AtomicInteger();
        AtomicReference<T> result = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                result.set(new TransactionTemplate(transactionManager).execute(status -> {
                    jdbcClient.sql("SET LOCAL statement_timeout = '" + statementTimeout + "'").update();
                    pid.set(jdbcClient.sql("SELECT pg_backend_pid()").query(Integer.class).single());
                    pidReady.countDown();
                    return action.get();
                }));
            } catch (Throwable thrown) {
                error.set(thrown);
            } finally {
                pidReady.countDown();
                finished.countDown();
            }
        }, name);
        thread.start();
        return new Call<>(thread, pidReady, finished, pid, result, error);
    }

    private boolean awaitBlocked(int waiterPid, int holderPid) throws InterruptedException {
        assertNotEquals(holderPid, waiterPid, "the waiter must use a different database connection from the holder");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            if (isBlocked(waiterPid, holderPid)) {
                return true;
            }
            Thread.sleep(50);
        }
        return false;
    }

    private boolean isBlocked(int waiterPid, int holderPid) {
        return Boolean.TRUE.equals(jdbcClient.sql("""
                        SELECT CAST(:holder AS integer) = ANY (pg_blocking_pids(CAST(:waiter AS integer)))
                        """)
                .param("holder", holderPid)
                .param("waiter", waiterPid)
                .query(Boolean.class)
                .single());
    }

    private Task claimExpired(UUID workerId, Instant leaseExpiresAt) {
        taskService.createTask("NOOP", "{\"test\":\"reaper-lock\"}");
        Task claimed = taskService.claimTask(workerId);
        assertNotNull(claimed);
        assertEquals(1, claimed.attempt());
        jdbcClient.sql("UPDATE tasks SET lease_expires_at = :leaseExpiresAt WHERE id = :id")
                .param("leaseExpiresAt", OffsetDateTime.ofInstant(leaseExpiresAt, ZoneOffset.UTC),
                        Types.TIMESTAMP_WITH_TIMEZONE)
                .param("id", claimed.id())
                .update();
        return claimed;
    }

    /** The {@code extendLease} update, left uncommitted. */
    private void holdUncommittedHeartbeat(Connection connection, UUID taskId, UUID workerId, int attempt)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                UPDATE tasks
                SET lease_expires_at = NOW() + INTERVAL '30 seconds',
                    last_heartbeat_at = NOW()
                WHERE id = ?
                  AND status = 'RUNNING'
                  AND worker_id = ?
                  AND attempt = ?
                """)) {
            statement.setObject(1, taskId);
            statement.setObject(2, workerId);
            statement.setInt(3, attempt);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("uncommitted heartbeat did not update " + taskId);
            }
        }
    }

    /** The {@code completeTask} update, left uncommitted. */
    private void holdUncommittedComplete(Connection connection, UUID taskId, UUID workerId, int attempt)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                UPDATE tasks
                SET status = 'COMPLETED',
                    result = CAST('{}' AS jsonb),
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE id = ?
                  AND status = 'RUNNING'
                  AND worker_id = ?
                  AND attempt = ?
                """)) {
            statement.setObject(1, taskId);
            statement.setObject(2, workerId);
            statement.setInt(3, attempt);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("uncommitted complete did not update " + taskId);
            }
        }
    }

    private void release(Connection connection, boolean committed) throws SQLException {
        try {
            if (!committed && !connection.isClosed()) {
                connection.rollback();
            }
        } finally {
            connection.close();
        }
    }

    private static void join(Call<?> call) throws InterruptedException {
        if (call != null) {
            join(call.thread());
        }
    }

    private static void join(Thread thread) throws InterruptedException {
        thread.join(10_000);
    }

    private String statusOf(UUID id) {
        return jdbcClient.sql("SELECT status FROM tasks WHERE id = :id")
                .param("id", id)
                .query(String.class)
                .single();
    }

    private static Set<UUID> ids(List<ReapedTask> batch) {
        return batch.stream().map(ReapedTask::taskId).collect(Collectors.toSet());
    }

    private record HeldReap(
            Thread thread,
            CountDownLatch reaped,
            CountDownLatch release,
            CountDownLatch finished,
            AtomicBoolean commitRequested,
            AtomicInteger pid,
            AtomicReference<List<ReapedTask>> batch,
            AtomicReference<Throwable> error
    ) {
        void awaitReaped() throws InterruptedException {
            assertTrue(reaped.await(WAIT_SECONDS, TimeUnit.SECONDS), "held reap did not run");
            if (error.get() != null) {
                throw new AssertionError("held reap failed", error.get());
            }
        }

        void commit() {
            commitRequested.set(true);
            release.countDown();
        }

        void rollbackUnlessCommitted() {
            release.countDown();
        }

        void awaitFinished() throws InterruptedException {
            assertTrue(finished.await(WAIT_SECONDS, TimeUnit.SECONDS), "held reap did not finish");
            if (error.get() != null) {
                throw new AssertionError("held reap failed", error.get());
            }
        }
    }

    private record Call<T>(
            Thread thread,
            CountDownLatch pidReady,
            CountDownLatch finished,
            AtomicInteger pid,
            AtomicReference<T> result,
            AtomicReference<Throwable> error
    ) {
        void awaitFinished() throws InterruptedException {
            assertTrue(finished.await(WAIT_SECONDS, TimeUnit.SECONDS), thread.getName() + " did not finish");
            if (error.get() != null) {
                throw new AssertionError(thread.getName() + " failed", error.get());
            }
        }
    }
}
