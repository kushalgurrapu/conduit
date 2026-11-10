package com.kushal.workflow.task;

import com.kushal.workflow.support.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The reap is one statement: an expired {@code RUNNING} row goes back to
 * {@code PENDING} with its owner, lease, and heartbeat cleared and its
 * {@code attempt} kept, and that attempt is closed as {@code LEASE_EXPIRED}.
 *
 * <p>Expiry is set by SQL. A lease at {@code NOW()} is not expired, and
 * {@code PENDING} and terminal rows are never touched, even with past
 * leases. An expired attempt that extends, completes, or fails before a
 * reap keeps the row.
 */
@SpringBootTest
class LeaseReapTest extends PostgresIntegrationTest {

    private static final Instant FIXTURE_TIME = Instant.parse("2000-01-01T00:00:00Z");
    private static final UUID OWNER = UUID.fromString("00000000-0000-0000-0000-00000000000a");

    /** The requeue procedure in ADR 0005, verbatim. */
    private static final String EXPIRE_THE_LEASE = """
            UPDATE tasks SET lease_expires_at = now()
            WHERE id = :id AND status = 'RUNNING' AND worker_id = :observedWorker AND attempt = :observedAttempt;
            """;

    @Autowired
    private TaskService taskService;

    @Autowired
    private LeaseProperties leaseProperties;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Test
    void expiredRunningRowIsRequeuedAndItsAttemptClosed() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);
        assertTrue(taskService.extendLease(claimed.id(), workerId, claimed.attempt()));
        setLease(claimed.id(), FIXTURE_TIME);
        backdateUpdatedAt(claimed.id());
        Task before = taskService.getTask(claimed.id());
        assertNotNull(before.lastHeartbeatAt());

        List<ReapedTask> reaped = taskService.reapExpiredLeases(10);

        assertEquals(List.of(new ReapedTask(claimed.id(), 1, workerId, true)), reaped);
        Task after = taskService.getTask(claimed.id());
        assertTrue(after.updatedAt().isAfter(before.updatedAt()));
        assertEquals(new Task(
                before.id(), before.taskType(), TaskStatus.PENDING, before.payload(), before.result(),
                before.error(), before.createdAt(), after.updatedAt(), null, before.finishedAt(),
                null, before.attempt(), null, null
        ), after);
        assertEquals(List.of(new TaskAttempt(
                claimed.id(), 1, workerId, claimed.claimedAt(), after.updatedAt(), AttemptOutcome.LEASE_EXPIRED, null)
        ), taskService.getAttempts(claimed.id()));
    }

    @Test
    void futureLeasesPendingRowsAndTerminalRowsAreNeverReaped() {
        UUID workerId = UUID.randomUUID();
        claim(workerId);
        Task completed = claim(workerId);
        Task failed = claim(workerId);
        assertInstanceOf(TransitionResult.Applied.class,
                taskService.completeTask(completed.id(), workerId, completed.attempt(), "{\"ok\":true}"));
        assertInstanceOf(TransitionResult.Applied.class,
                taskService.failTask(failed.id(), workerId, failed.attempt(), "boom"));
        Task cancelled = taskService.createTask("NOOP", "{\"test\":\"reap-cancelled\"}");
        assertInstanceOf(CancelResult.Cancelled.class, taskService.cancelTask(cancelled.id()));
        Task pending = taskService.createTask("NOOP", "{\"test\":\"reap-pending\"}");
        for (UUID id : List.of(completed.id(), failed.id(), cancelled.id(), pending.id())) {
            setLease(id, FIXTURE_TIME);
        }
        List<String> tasksBefore = allTasks();
        List<String> historyBefore = allHistory();

        assertEquals(List.of(), taskService.reapExpiredLeases(10));

        assertEquals(tasksBefore, allTasks());
        assertEquals(historyBefore, allHistory());
    }

    @Test
    void leaseAtNowIsNotExpired() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);

        List<ReapedTask> reaped = new TransactionTemplate(transactionManager).execute(status -> {
            jdbcClient.sql("UPDATE tasks SET lease_expires_at = NOW() WHERE id = :id")
                    .param("id", claimed.id())
                    .update();
            return taskService.reapExpiredLeases(10);
        });

        assertEquals(List.of(), reaped);
        Task after = taskService.getTask(claimed.id());
        assertEquals(TaskStatus.RUNNING, after.status());
        assertEquals(workerId, after.workerId());
        assertEquals(List.of(new TaskAttempt(
                claimed.id(), 1, workerId, claimed.claimedAt(), null, null, null)
        ), taskService.getAttempts(claimed.id()));
    }

    @Test
    void batchTakesTheOldestLeasesAndRepeatedCallsDrainTheRest() {
        UUID workerId = UUID.randomUUID();
        List<UUID> byLease = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            UUID id = claim(workerId).id();
            setLease(id, FIXTURE_TIME.plusSeconds(i));
            byLease.add(id);
        }

        List<ReapedTask> first = taskService.reapExpiredLeases(2);
        List<ReapedTask> second = taskService.reapExpiredLeases(2);
        List<ReapedTask> third = taskService.reapExpiredLeases(2);

        assertEquals(Set.of(byLease.get(0), byLease.get(1)), ids(first));
        assertEquals(Set.of(byLease.get(2), byLease.get(3)), ids(second));
        assertEquals(Set.of(byLease.get(4)), ids(third));
        for (List<ReapedTask> batch : List.of(first, second, third)) {
            assertOrderedById(batch);
        }
        assertEquals(List.of(), taskService.reapExpiredLeases(2));
    }

    @Test
    void emptyTableReapsNothing() {
        assertEquals(List.of(), taskService.reapExpiredLeases(1));
    }

    @Test
    void reapedTaskIsClaimedAgainAtTheNextAttempt() {
        UUID workerA = UUID.randomUUID();
        UUID workerB = UUID.randomUUID();
        Task claimed = claim(workerA);
        assertTrue(taskService.extendLease(claimed.id(), workerA, claimed.attempt()));
        setLease(claimed.id(), FIXTURE_TIME);
        assertEquals(1, taskService.reapExpiredLeases(10).size());
        Instant reapedAt = taskService.getTask(claimed.id()).updatedAt();

        Task again = taskService.claimTask(workerB);

        assertNotNull(again);
        assertEquals(claimed.id(), again.id());
        assertEquals(TaskStatus.RUNNING, again.status());
        assertEquals(workerB, again.workerId());
        assertEquals(2, again.attempt());
        assertNull(again.lastHeartbeatAt());
        assertEquals(again.claimedAt().plus(leaseProperties.getDuration()), again.leaseExpiresAt());
        assertEquals(List.of(
                new TaskAttempt(claimed.id(), 1, workerA, claimed.claimedAt(), reapedAt,
                        AttemptOutcome.LEASE_EXPIRED, null),
                new TaskAttempt(claimed.id(), 2, workerB, again.claimedAt(), null, null, null)
        ), taskService.getAttempts(claimed.id()));
    }

    @Test
    void legacyAttemptZeroRowIsReapedAndClaimedAtAttemptOne() {
        UUID id = insertRunning(0);
        insertOpenAttempt(id, 0);

        assertEquals(List.of(new ReapedTask(id, 0, OWNER, true)), taskService.reapExpiredLeases(10));
        Instant reapedAt = taskService.getTask(id).updatedAt();
        assertEquals(List.of(new TaskAttempt(
                id, 0, OWNER, FIXTURE_TIME, reapedAt, AttemptOutcome.LEASE_EXPIRED, null)
        ), taskService.getAttempts(id));

        UUID workerId = UUID.randomUUID();
        Task claimed = taskService.claimTask(workerId);

        assertNotNull(claimed);
        assertEquals(id, claimed.id());
        assertEquals(1, claimed.attempt());
        assertEquals(List.of(0, 1), taskService.getAttempts(id).stream().map(TaskAttempt::attempt).toList());
    }

    @Test
    void closedAttemptIsNotOverwrittenAndTheTaskIsStillRequeued() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);
        jdbcClient.sql("""
                        UPDATE task_attempts
                        SET ended_at = :endedAt, outcome = 'FAILED', error = 'by hand'
                        WHERE task_id = :id AND attempt = 1
                        """)
                .param("endedAt", offset(FIXTURE_TIME), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("id", claimed.id())
                .update();
        setLease(claimed.id(), FIXTURE_TIME);
        List<String> historyBefore = allHistory();

        assertEquals(List.of(new ReapedTask(claimed.id(), 1, workerId, false)), taskService.reapExpiredLeases(10));

        Task after = taskService.getTask(claimed.id());
        assertEquals(TaskStatus.PENDING, after.status());
        assertNull(after.workerId());
        assertEquals(1, after.attempt());
        assertEquals(historyBefore, allHistory());
    }

    @Test
    void missingAttemptRowIsCreatedClosed() {
        UUID id = insertRunning(2);
        assertEquals(List.of(), taskService.getAttempts(id));

        assertEquals(List.of(new ReapedTask(id, 2, OWNER, true)), taskService.reapExpiredLeases(10));

        Task after = taskService.getTask(id);
        assertEquals(TaskStatus.PENDING, after.status());
        assertEquals(2, after.attempt());
        assertEquals(List.of(new TaskAttempt(
                id, 2, OWNER, FIXTURE_TIME, after.updatedAt(), AttemptOutcome.LEASE_EXPIRED, null)
        ), taskService.getAttempts(id));
    }

    @Test
    void expiredAttemptThatExtendsIsNotReaped() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);
        setLease(claimed.id(), FIXTURE_TIME);

        assertTrue(taskService.extendLease(claimed.id(), workerId, claimed.attempt()));

        assertEquals(List.of(), taskService.reapExpiredLeases(10));
        Task after = taskService.getTask(claimed.id());
        assertEquals(TaskStatus.RUNNING, after.status());
        assertEquals(workerId, after.workerId());
        assertNull(taskService.getAttempts(claimed.id()).getFirst().outcome());
    }

    @Test
    void expiredAttemptThatCompletesIsNotReaped() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);
        setLease(claimed.id(), FIXTURE_TIME);

        TransitionResult.Applied applied = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.completeTask(claimed.id(), workerId, claimed.attempt(), "{\"late\":true}"));

        assertEquals(List.of(), taskService.reapExpiredLeases(10));
        assertEquals(applied.task(), taskService.getTask(claimed.id()));
        assertEquals(AttemptOutcome.COMPLETED, taskService.getAttempts(claimed.id()).getFirst().outcome());
    }

    @Test
    void expiredAttemptThatFailsIsNotReaped() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);
        setLease(claimed.id(), FIXTURE_TIME);

        TransitionResult.Applied applied = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.failTask(claimed.id(), workerId, claimed.attempt(), "late"));

        assertEquals(List.of(), taskService.reapExpiredLeases(10));
        assertEquals(applied.task(), taskService.getTask(claimed.id()));
        assertEquals(AttemptOutcome.FAILED, taskService.getAttempts(claimed.id()).getFirst().outcome());
    }

    @Test
    void expireTheLeaseProcedureLetsTheNextReapRequeue() {
        UUID workerA = UUID.randomUUID();
        Task claimed = claim(workerA);

        assertEquals(1, expireTheLease(claimed.id(), workerA, claimed.attempt()));

        assertEquals(List.of(new ReapedTask(claimed.id(), 1, workerA, true)), taskService.reapExpiredLeases(10));
        Task pending = taskService.getTask(claimed.id());
        assertEquals(TaskStatus.PENDING, pending.status());
        assertEquals(1, pending.attempt());
        assertEquals(List.of(new TaskAttempt(
                claimed.id(), 1, workerA, claimed.claimedAt(), pending.updatedAt(), AttemptOutcome.LEASE_EXPIRED, null)
        ), taskService.getAttempts(claimed.id()));

        Task again = taskService.claimTask(UUID.randomUUID());
        assertNotNull(again);
        assertEquals(claimed.id(), again.id());
        assertEquals(2, again.attempt());
    }

    @Test
    void expireTheLeaseProcedureWithTheWrongWorkerOrAttemptChangesNothing() {
        UUID workerA = UUID.randomUUID();
        Task claimed = claim(workerA);
        Task before = taskService.getTask(claimed.id());
        List<String> historyBefore = allHistory();

        assertEquals(0, expireTheLease(claimed.id(), UUID.randomUUID(), claimed.attempt()));
        assertEquals(0, expireTheLease(claimed.id(), workerA, claimed.attempt() + 1));

        assertEquals(before, taskService.getTask(claimed.id()));
        assertEquals(List.of(), taskService.reapExpiredLeases(10));
        assertEquals(before, taskService.getTask(claimed.id()));
        assertEquals(historyBefore, allHistory());
    }

    @Test
    void batchSizeBelowOneIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> taskService.reapExpiredLeases(0));
    }

    private Task claim(UUID workerId) {
        taskService.createTask("NOOP", "{\"test\":\"reap\"}");
        Task claimed = taskService.claimTask(workerId);
        assertNotNull(claimed);
        assertEquals(1, claimed.attempt());
        return claimed;
    }

    private int expireTheLease(UUID id, UUID observedWorker, int observedAttempt) {
        return jdbcClient.sql(EXPIRE_THE_LEASE)
                .param("id", id)
                .param("observedWorker", observedWorker)
                .param("observedAttempt", observedAttempt)
                .update();
    }

    /** A {@code RUNNING} row with no history row, claimed by {@link #OWNER} with an expired lease. */
    private UUID insertRunning(int attempt) {
        UUID id = UUID.randomUUID();
        jdbcClient.sql("""
                        INSERT INTO tasks (
                            id, task_type, status, payload, created_at, updated_at,
                            claimed_at, worker_id, attempt, lease_expires_at
                        ) VALUES (
                            :id, 'NOOP', 'RUNNING', CAST('{}' AS jsonb), :at, :at, :at, :workerId, :attempt, :at
                        )
                        """)
                .param("id", id)
                .param("at", offset(FIXTURE_TIME), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("workerId", OWNER)
                .param("attempt", attempt)
                .update();
        return id;
    }

    /** The open row V6 backfills for a {@code RUNNING} task. */
    private void insertOpenAttempt(UUID taskId, int attempt) {
        jdbcClient.sql("""
                        INSERT INTO task_attempts (task_id, attempt, worker_id, started_at)
                        SELECT id, attempt, worker_id, claimed_at
                        FROM tasks
                        WHERE id = :taskId AND attempt = :attempt
                        """)
                .param("taskId", taskId)
                .param("attempt", attempt)
                .update();
    }

    private void setLease(UUID id, Instant leaseExpiresAt) {
        jdbcClient.sql("UPDATE tasks SET lease_expires_at = :leaseExpiresAt WHERE id = :id")
                .param("leaseExpiresAt", offset(leaseExpiresAt), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("id", id)
                .update();
    }

    private void backdateUpdatedAt(UUID id) {
        jdbcClient.sql("UPDATE tasks SET updated_at = :updatedAt WHERE id = :id")
                .param("updatedAt", offset(FIXTURE_TIME), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("id", id)
                .update();
    }

    private List<String> allTasks() {
        return jdbcClient.sql("SELECT t::text FROM tasks t ORDER BY t.id")
                .query(String.class)
                .list();
    }

    private List<String> allHistory() {
        return jdbcClient.sql("""
                        SELECT a::text
                        FROM task_attempts a
                        ORDER BY a.task_id, a.attempt
                        """)
                .query(String.class)
                .list();
    }

    private static Set<UUID> ids(List<ReapedTask> batch) {
        return batch.stream().map(ReapedTask::taskId).collect(Collectors.toSet());
    }

    /** PostgreSQL orders {@code uuid} by its bytes, which is the order of the lowercase text form. */
    private static void assertOrderedById(List<ReapedTask> batch) {
        List<UUID> ids = batch.stream().map(ReapedTask::taskId).toList();
        assertEquals(ids.stream().sorted(Comparator.comparing(UUID::toString)).toList(), ids);
    }

    private static OffsetDateTime offset(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
