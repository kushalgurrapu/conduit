package com.kushal.workflow.task;

import com.kushal.workflow.support.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Lock-hold races for cancel and for a second claim.
 *
 * <p>The holding connection has already updated the row to {@code RUNNING}
 * and has not committed, so it still owns the row lock. Cancel does not use
 * {@code SKIP LOCKED}, so it waits. {@code pg_blocking_pids} is how the test
 * knows the wait has started before the holder commits or rolls back.
 * PostgreSQL then re-checks {@code status = 'PENDING'}: a committed claim
 * makes cancel update zero rows, and a rolled-back claim leaves the row
 * pending so cancel applies.
 *
 * <p>The second claim uses the real {@code claimTask} statement. It returns
 * empty while the first claim is still uncommitted, and the committed worker
 * is still the holder. That is also the case that would show the outer
 * {@code AND status = 'PENDING'} check if {@code SKIP LOCKED} were removed:
 * the second claim would wait, the first would commit {@code RUNNING}, and
 * the re-check would still refuse the row. This test does not remove
 * {@code SKIP LOCKED}. It holds the lock until the second claim has already
 * returned, with a {@code statement_timeout} shorter than the wait, so a
 * blocking claim fails the test.
 *
 * <p>Timeouts bound every latch. The holder is rolled back and closed in
 * {@code finally}. {@code statement_timeout} is {@code SET LOCAL} on the
 * waiting transaction only.
 */
@SpringBootTest
class TransitionLockHoldTest extends PostgresIntegrationTest {

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
    void cancelWaitsForUncommittedClaimThenRejectsWhenItCommits() throws Exception {
        CancelResult result = raceCancel(true);
        CancelResult.NotCancellable rejected = assertInstanceOf(CancelResult.NotCancellable.class, result);
        assertEquals(TaskStatus.RUNNING, rejected.status());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void cancelWaitsForUncommittedClaimThenAppliesWhenItRollsBack() throws Exception {
        CancelResult result = raceCancel(false);
        CancelResult.Cancelled cancelled = assertInstanceOf(CancelResult.Cancelled.class, result);
        assertEquals(TaskStatus.CANCELLED, cancelled.task().status());
        assertNotNull(cancelled.task().finishedAt());
        assertEquals(cancelled.task().finishedAt(), cancelled.task().updatedAt());
        assertNull(cancelled.task().workerId());
        assertNull(cancelled.task().claimedAt());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void secondClaimSkipsUncommittedClaimAndLeavesWorkerA() throws Exception {
        UUID workerA = UUID.randomUUID();
        UUID workerB = UUID.randomUUID();
        Task created = taskService.createTask("NOOP", "{\"test\":\"held-claim\"}");

        Connection holder = dataSource.getConnection();
        holder.setAutoCommit(false);
        Thread claimThread = null;
        boolean committed = false;
        AtomicReference<Task> claimedByB = new AtomicReference<>();
        AtomicReference<Throwable> claimError = new AtomicReference<>();
        CountDownLatch claimFinished = new CountDownLatch(1);
        try {
            holdUncommittedClaim(holder, created.id(), workerA);
            claimThread = new Thread(() -> {
                try {
                    TransactionTemplate transaction = new TransactionTemplate(transactionManager);
                    claimedByB.set(transaction.execute(status -> {
                        jdbcClient.sql("SET LOCAL statement_timeout = '8s'").update();
                        return taskService.claimTask(workerB);
                    }));
                } catch (Throwable thrown) {
                    claimError.set(thrown);
                } finally {
                    claimFinished.countDown();
                }
            }, "second-claim");
            claimThread.start();

            assertTrue(claimFinished.await(5, TimeUnit.SECONDS),
                    "second claim should skip the locked row instead of waiting for the holder");
            if (claimError.get() != null) {
                throw new AssertionError("second claim failed", claimError.get());
            }
            assertNull(claimedByB.get());
            assertEquals("PENDING", statusOf(created.id()),
                    "the held claim is uncommitted, so other transactions still see PENDING");

            holder.commit();
            committed = true;

            assertEquals("RUNNING", statusOf(created.id()));
            assertEquals(workerA, workerOf(created.id()));
        } finally {
            release(holder, committed);
            if (claimThread != null) {
                claimThread.join(10_000);
            }
        }
    }

    private CancelResult raceCancel(boolean commitClaim) throws Exception {
        UUID workerA = UUID.randomUUID();
        Task created = taskService.createTask("NOOP", "{\"test\":\"cancel-vs-claim\"}");

        Connection holder = dataSource.getConnection();
        holder.setAutoCommit(false);
        CancelAttempt attempt = null;
        boolean committed = false;
        try {
            holdUncommittedClaim(holder, created.id(), workerA);
            int holderPid = backendPid(holder);
            attempt = startCancel(created.id());

            assertTrue(attempt.pidReady().await(WAIT_SECONDS, TimeUnit.SECONDS), "cancel did not start");
            assertNotEquals(holderPid, attempt.pid().get(),
                    "cancel must wait on a different database connection from the claim");
            assertTrue(awaitBlocked(attempt.pid().get(), holderPid),
                    "cancel should wait until the uncommitted claim releases the row");
            assertEquals("PENDING", statusOf(created.id()));

            if (commitClaim) {
                holder.commit();
                committed = true;
            } else {
                holder.rollback();
            }

            assertTrue(attempt.finished().await(WAIT_SECONDS, TimeUnit.SECONDS),
                    "cancel did not finish after the claim released the row");
            if (attempt.error().get() != null) {
                throw new AssertionError("cancel failed", attempt.error().get());
            }

            if (commitClaim) {
                assertEquals("RUNNING", statusOf(created.id()));
                assertEquals(workerA, workerOf(created.id()));
                assertNull(finishedAtOf(created.id()));
            } else {
                assertEquals("CANCELLED", statusOf(created.id()));
                assertNull(workerOf(created.id()));
            }
            return attempt.result().get();
        } finally {
            release(holder, committed);
            if (attempt != null) {
                attempt.thread().join(10_000);
            }
        }
    }

    private CancelAttempt startCancel(UUID taskId) {
        CountDownLatch pidReady = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicInteger pid = new AtomicInteger();
        AtomicReference<CancelResult> result = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            try {
                TransactionTemplate transaction = new TransactionTemplate(transactionManager);
                result.set(transaction.execute(status -> {
                    jdbcClient.sql("SET LOCAL statement_timeout = '8s'").update();
                    pid.set(jdbcClient.sql("SELECT pg_backend_pid()").query(Integer.class).single());
                    pidReady.countDown();
                    return taskService.cancelTask(taskId);
                }));
            } catch (Throwable thrown) {
                error.set(thrown);
            } finally {
                pidReady.countDown();
                finished.countDown();
            }
        }, "cancel-vs-claim");
        thread.start();
        return new CancelAttempt(thread, pidReady, finished, pid, result, error);
    }

    private boolean awaitBlocked(int waiterPid, int holderPid) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            Boolean blocked = jdbcClient.sql("""
                            SELECT CAST(:holder AS integer) = ANY (pg_blocking_pids(CAST(:waiter AS integer)))
                            """)
                    .param("holder", holderPid)
                    .param("waiter", waiterPid)
                    .query(Boolean.class)
                    .single();
            if (Boolean.TRUE.equals(blocked)) {
                return true;
            }
            Thread.sleep(50);
        }
        return false;
    }

    private void holdUncommittedClaim(Connection connection, UUID taskId, UUID workerId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                UPDATE tasks
                SET status = 'RUNNING',
                    claimed_at = NOW(),
                    worker_id = ?,
                    updated_at = NOW()
                WHERE id = ?
                  AND status = 'PENDING'
                """)) {
            statement.setObject(1, workerId);
            statement.setObject(2, taskId);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("uncommitted claim did not update " + taskId);
            }
        }
    }

    private int backendPid(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT pg_backend_pid()")) {
            result.next();
            return result.getInt(1);
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

    private String statusOf(UUID id) {
        return jdbcClient.sql("SELECT status FROM tasks WHERE id = :id")
                .param("id", id)
                .query(String.class)
                .single();
    }

    private UUID workerOf(UUID id) {
        Optional<UUID> worker = jdbcClient.sql("SELECT worker_id FROM tasks WHERE id = :id")
                .param("id", id)
                .query((rs, rowNum) -> Optional.ofNullable(rs.getObject("worker_id", UUID.class)))
                .single();
        return worker.orElse(null);
    }

    private Instant finishedAtOf(UUID id) {
        Optional<Instant> finishedAt = jdbcClient.sql("SELECT finished_at FROM tasks WHERE id = :id")
                .param("id", id)
                .query((rs, rowNum) -> {
                    OffsetDateTime value = rs.getObject("finished_at", OffsetDateTime.class);
                    return Optional.ofNullable(value == null ? null : value.toInstant());
                })
                .single();
        return finishedAt.orElse(null);
    }

    private record CancelAttempt(
            Thread thread,
            CountDownLatch pidReady,
            CountDownLatch finished,
            AtomicInteger pid,
            AtomicReference<CancelResult> result,
            AtomicReference<Throwable> error
    ) {
    }
}
