package com.kushal.workflow.task;

import com.kushal.workflow.support.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves lease extension is one guarded update, and that complete and fail
 * accept only the attempt the caller still holds.
 *
 * <p>A past {@code lease_expires_at} still extends while the row is
 * {@code RUNNING} for that worker and attempt. Expiry is not a new owner.
 * The engine's {@code RUNNING} to {@code PENDING} path is the lease reaper;
 * the requeue in this class is test SQL, a fixture for a later claim.
 */
@SpringBootTest
class LeaseExtensionTest extends PostgresIntegrationTest {

    private static final Instant FIXTURE_TIME = Instant.parse("2000-01-01T00:00:00Z");

    @Autowired
    private TaskService taskService;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private LeaseProperties leaseProperties;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    void extendMovesTheLeaseAndRecordsTheHeartbeat() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);
        backdateUpdatedAt(claimed.id());
        Task before = taskService.getTask(claimed.id());
        assertNull(before.lastHeartbeatAt());
        assertEquals(FIXTURE_TIME, before.updatedAt());

        assertTrue(taskService.extendLease(before.id(), workerId, before.attempt()));

        Task after = taskService.getTask(before.id());
        assertEquals(TaskStatus.RUNNING, after.status());
        assertEquals(workerId, after.workerId());
        assertEquals(before.attempt(), after.attempt());
        assertEquals(before.updatedAt(), after.updatedAt());
        assertEquals(before.claimedAt(), after.claimedAt());
        assertNull(after.finishedAt());
        assertNull(after.result());
        assertNull(after.error());
        assertNotNull(after.lastHeartbeatAt());
        assertTrue(after.leaseExpiresAt().isAfter(before.leaseExpiresAt()));
        assertExtendedBy(before.id(), leaseProperties.leaseMicros());
    }

    @Test
    void extendHonorsASubSecondDuration() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);
        long leaseMicros = Duration.ofMillis(500).toNanos() / 1_000L;
        assertEquals(500_000L, leaseMicros);

        assertTrue(taskRepository.extendLease(
                claimed.id(), workerId, claimed.attempt(), leaseMicros));

        assertExtendedBy(claimed.id(), leaseMicros);
        Boolean shorterThanOneSecond = jdbcClient.sql("""
                SELECT lease_expires_at < last_heartbeat_at + INTERVAL '1 second'
                FROM tasks
                WHERE id = :id
                """)
                .param("id", claimed.id())
                .query(Boolean.class)
                .single();
        assertEquals(Boolean.TRUE, shorterThanOneSecond);
    }

    @Test
    void extendReturnsFalseWhenTheGuardMisses() {
        UUID workerId = UUID.randomUUID();
        Task running = claim(workerId);
        Task toFail = claim(workerId);
        Task pending = taskService.createTask("NOOP", "{\"test\":\"extend-pending\"}");
        backdateUpdatedAt(running.id());
        Task before = taskService.getTask(running.id());

        assertFalse(taskService.extendLease(UUID.randomUUID(), workerId, before.attempt()));
        assertFalse(taskService.extendLease(before.id(), UUID.randomUUID(), before.attempt()));
        assertFalse(taskService.extendLease(before.id(), workerId, before.attempt() + 1));
        assertEquals(before, taskService.getTask(before.id()));

        assertFalse(taskService.extendLease(pending.id(), workerId, pending.attempt()));
        assertEquals(pending, taskService.getTask(pending.id()));

        CancelResult.Cancelled cancelled = assertInstanceOf(
                CancelResult.Cancelled.class, taskService.cancelTask(pending.id()));
        assertFalse(taskService.extendLease(pending.id(), workerId, cancelled.task().attempt()));
        assertEquals(cancelled.task(), taskService.getTask(pending.id()));

        TransitionResult.Applied completed = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.completeTask(before.id(), workerId, before.attempt(), "{\"done\":true}"));
        assertFalse(taskService.extendLease(before.id(), workerId, before.attempt()));
        assertEquals(completed.task(), taskService.getTask(before.id()));

        TransitionResult.Applied failed = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.failTask(toFail.id(), workerId, toFail.attempt(), "boom"));
        assertFalse(taskService.extendLease(toFail.id(), workerId, toFail.attempt()));
        assertEquals(failed.task(), taskService.getTask(toFail.id()));
    }

    @Test
    void extendRejectsANullIdOrWorkerId() {
        Task created = taskService.createTask("NOOP", "{\"test\":\"extend-null\"}");

        assertThrows(IllegalArgumentException.class,
                () -> taskService.extendLease(null, UUID.randomUUID(), 0));
        assertThrows(IllegalArgumentException.class,
                () -> taskService.extendLease(created.id(), null, 0));

        assertEquals(created, taskService.getTask(created.id()));
    }

    @Test
    void pastLeaseStillExtendsWhileTheGuardMatches() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);
        jdbcClient.sql("""
                UPDATE tasks
                SET lease_expires_at = :leaseExpiresAt,
                    updated_at = :updatedAt
                WHERE id = :id
                """)
                .param("leaseExpiresAt", offset(FIXTURE_TIME), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("updatedAt", offset(FIXTURE_TIME), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("id", claimed.id())
                .update();
        Boolean alreadyPast = jdbcClient.sql("""
                SELECT lease_expires_at < NOW()
                FROM tasks
                WHERE id = :id
                """)
                .param("id", claimed.id())
                .query(Boolean.class)
                .single();
        assertEquals(Boolean.TRUE, alreadyPast);

        Task before = taskService.getTask(claimed.id());
        assertTrue(taskService.extendLease(before.id(), workerId, before.attempt()));

        Task after = taskService.getTask(before.id());
        assertEquals(TaskStatus.RUNNING, after.status());
        assertEquals(workerId, after.workerId());
        assertEquals(before.attempt(), after.attempt());
        assertEquals(before.updatedAt(), after.updatedAt());
        assertTrue(after.leaseExpiresAt().isAfter(before.leaseExpiresAt()));
        assertNotNull(after.lastHeartbeatAt());
        assertExtendedBy(before.id(), leaseProperties.leaseMicros());
    }

    @Test
    void wrongAttemptLeavesARunningTaskUntouched() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);
        backdateUpdatedAt(claimed.id());
        Task before = taskService.getTask(claimed.id());
        int staleAttempt = before.attempt() + 1;

        assertFalse(taskService.extendLease(before.id(), workerId, staleAttempt));
        TransitionResult.Rejected completed = assertInstanceOf(
                TransitionResult.Rejected.class,
                taskService.completeTask(before.id(), workerId, staleAttempt, "{\"x\":1}"));
        TransitionResult.Rejected failed = assertInstanceOf(
                TransitionResult.Rejected.class,
                taskService.failTask(before.id(), workerId, staleAttempt, "nope"));

        assertEquals(TaskStatus.RUNNING, completed.currentStatus());
        assertEquals(workerId, completed.currentWorker());
        assertEquals(TaskStatus.RUNNING, failed.currentStatus());
        assertEquals(workerId, failed.currentWorker());
        assertEquals(before, taskService.getTask(before.id()));
    }

    @Test
    void differentAttemptIsNotAlreadyApplied() {
        UUID workerId = UUID.randomUUID();
        Task completed = claim(workerId);
        TransitionResult.Applied first = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.completeTask(completed.id(), workerId, completed.attempt(), "{\"n\":1}"));
        Task stored = taskService.getTask(completed.id());
        assertEquals(first.task(), stored);

        TransitionResult.Rejected otherComplete = assertInstanceOf(
                TransitionResult.Rejected.class,
                taskService.completeTask(
                        completed.id(), workerId, completed.attempt() + 1, "{\"n\":2}"));
        assertEquals(TaskStatus.COMPLETED, otherComplete.currentStatus());
        assertEquals(workerId, otherComplete.currentWorker());
        assertEquals(stored, taskService.getTask(completed.id()));

        Task failed = claim(workerId);
        TransitionResult.Applied appliedFail = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.failTask(failed.id(), workerId, failed.attempt(), "first"));
        Task failedRow = taskService.getTask(failed.id());
        assertEquals(appliedFail.task(), failedRow);

        TransitionResult.AlreadyApplied again = assertInstanceOf(
                TransitionResult.AlreadyApplied.class,
                taskService.failTask(failed.id(), workerId, failed.attempt(), "second"));
        assertEquals(failed.attempt(), again.task().attempt());
        assertEquals("first", again.task().error());

        TransitionResult.Rejected otherFail = assertInstanceOf(
                TransitionResult.Rejected.class,
                taskService.failTask(failed.id(), workerId, failed.attempt() + 1, "third"));
        assertEquals(TaskStatus.FAILED, otherFail.currentStatus());
        assertEquals(workerId, otherFail.currentWorker());
        assertEquals(failedRow, taskService.getTask(failed.id()));
    }

    @Test
    void appliedFinishLeavesTheAttemptAndLease() {
        UUID workerId = UUID.randomUUID();
        Task toComplete = claim(workerId);
        Task toFail = claim(workerId);

        TransitionResult.Applied completed = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.completeTask(
                        toComplete.id(), workerId, toComplete.attempt(), "{\"ok\":true}"));
        assertEquals(toComplete.attempt(), completed.task().attempt());
        assertEquals(toComplete.leaseExpiresAt(), completed.task().leaseExpiresAt());
        assertNull(completed.task().lastHeartbeatAt());
        assertEquals(TaskStatus.COMPLETED, completed.task().status());

        TransitionResult.Applied failed = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.failTask(toFail.id(), workerId, toFail.attempt(), "boom"));
        assertEquals(toFail.attempt(), failed.task().attempt());
        assertEquals(toFail.leaseExpiresAt(), failed.task().leaseExpiresAt());
        assertNull(failed.task().lastHeartbeatAt());
        assertEquals(TaskStatus.FAILED, failed.task().status());
    }

    /**
     * The engine's {@code RUNNING} to {@code PENDING} path is the lease
     * reaper. The update below is a test fixture, not that path: it clears
     * the owner fields and leaves {@code attempt} alone, then the same
     * worker claims again.
     */
    @Test
    void sameWorkerRequeueFencesThePreviousAttempt() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);
        assertTrue(taskService.extendLease(claimed.id(), workerId, claimed.attempt()));
        Task heartbeated = taskService.getTask(claimed.id());
        assertNotNull(heartbeated.lastHeartbeatAt());

        Optional<UUID> requeued = jdbcClient.sql("""
                UPDATE tasks
                SET status = 'PENDING', worker_id = NULL, claimed_at = NULL, updated_at = NOW()
                WHERE id = :id AND status = 'RUNNING' AND worker_id = :workerId
                RETURNING id
                """)
                .param("id", claimed.id())
                .param("workerId", workerId)
                .query(UUID.class)
                .optional();
        assertEquals(Optional.of(claimed.id()), requeued);
        Task pending = taskService.getTask(claimed.id());
        assertEquals(TaskStatus.PENDING, pending.status());
        assertEquals(heartbeated.attempt(), pending.attempt());
        assertEquals(heartbeated.leaseExpiresAt(), pending.leaseExpiresAt());
        assertEquals(heartbeated.lastHeartbeatAt(), pending.lastHeartbeatAt());

        Task again = taskService.claimTask(workerId);
        assertNotNull(again);
        assertEquals(claimed.id(), again.id());
        assertEquals(workerId, again.workerId());
        assertEquals(heartbeated.attempt() + 1, again.attempt());
        assertNull(again.lastHeartbeatAt());
        assertExtendedByClaim(again.id(), again.attempt());

        Task owned = taskService.getTask(again.id());
        assertFalse(taskService.extendLease(again.id(), workerId, claimed.attempt()));
        assertEquals(owned, taskService.getTask(again.id()));

        TransitionResult.Rejected rejected = assertInstanceOf(
                TransitionResult.Rejected.class,
                taskService.completeTask(again.id(), workerId, claimed.attempt(), "{\"by\":\"old\"}"));
        assertEquals(TaskStatus.RUNNING, rejected.currentStatus());
        assertEquals(workerId, rejected.currentWorker());
        assertEquals(owned, taskService.getTask(again.id()));

        TransitionResult.Applied applied = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.completeTask(again.id(), workerId, again.attempt(), "{\"by\":\"new\"}"));
        assertEquals(TaskStatus.COMPLETED, applied.task().status());
        assertEquals(again.attempt(), applied.task().attempt());
        assertTrue(resultEquals(again.id(), "{\"by\":\"new\"}"));
        assertFalse(resultEquals(again.id(), "{\"by\":\"old\"}"));
    }

    @Test
    void onlyClaimChangesAttempt() {
        Task created = taskService.createTask("NOOP", "{\"test\":\"attempt-stable\"}");
        assertEquals(0, created.attempt());

        CancelResult.Cancelled cancelled = assertInstanceOf(
                CancelResult.Cancelled.class, taskService.cancelTask(created.id()));
        assertEquals(0, cancelled.task().attempt());

        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);
        assertEquals(1, claimed.attempt());
        assertTrue(taskService.extendLease(claimed.id(), workerId, claimed.attempt()));
        assertEquals(1, taskService.getTask(claimed.id()).attempt());

        TransitionResult.Applied completed = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.completeTask(claimed.id(), workerId, claimed.attempt(), "null"));
        assertEquals(1, completed.task().attempt());

        Task toFail = claim(workerId);
        TransitionResult.Applied failed = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.failTask(toFail.id(), workerId, toFail.attempt(), "nope"));
        assertEquals(1, failed.task().attempt());
    }

    private Task claim(UUID workerId) {
        taskService.createTask("NOOP", "{\"test\":\"lease\"}");
        Task claimed = taskService.claimTask(workerId);
        assertNotNull(claimed);
        assertEquals(workerId, claimed.workerId());
        assertEquals(TaskStatus.RUNNING, claimed.status());
        assertEquals(1, claimed.attempt());
        assertNull(claimed.lastHeartbeatAt());
        return claimed;
    }

    private void backdateUpdatedAt(UUID id) {
        jdbcClient.sql("""
                UPDATE tasks
                SET updated_at = :updatedAt
                WHERE id = :id
                """)
                .param("updatedAt", offset(FIXTURE_TIME), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("id", id)
                .update();
    }

    private void assertExtendedBy(UUID id, long leaseMicros) {
        Boolean matches = jdbcClient.sql("""
                SELECT lease_expires_at = last_heartbeat_at + (:leaseMicros * INTERVAL '1 microsecond')
                   AND lease_expires_at <> last_heartbeat_at
                FROM tasks
                WHERE id = :id
                """)
                .param("leaseMicros", leaseMicros)
                .param("id", id)
                .query(Boolean.class)
                .single();
        assertEquals(Boolean.TRUE, matches);
    }

    private void assertExtendedByClaim(UUID id, int attempt) {
        Boolean matches = jdbcClient.sql("""
                SELECT lease_expires_at = claimed_at + (:leaseMicros * INTERVAL '1 microsecond')
                   AND last_heartbeat_at IS NULL
                   AND attempt = :attempt
                FROM tasks
                WHERE id = :id
                """)
                .param("leaseMicros", leaseProperties.leaseMicros())
                .param("attempt", attempt)
                .param("id", id)
                .query(Boolean.class)
                .single();
        assertEquals(Boolean.TRUE, matches);
    }

    private boolean resultEquals(UUID id, String json) {
        return Boolean.TRUE.equals(jdbcClient.sql("""
                SELECT result = CAST(:json AS jsonb)
                FROM tasks
                WHERE id = :id
                """)
                .param("json", json)
                .param("id", id)
                .query(Boolean.class)
                .single());
    }

    private static OffsetDateTime offset(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
