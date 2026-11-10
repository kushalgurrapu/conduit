package com.kushal.workflow.task;

import com.kushal.workflow.support.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@code task_attempts} is written in the same transaction as the
 * {@code tasks} change it records: a claim opens a row, and only an applied
 * complete or fail closes one. Rows written by hand stand in for history the
 * engine did not write (a conflict, a legacy {@code RUNNING} row, an attempt
 * already closed). None of them blocks or rolls back the {@code tasks} change.
 *
 * <p>History is compared as the text of each whole row, so "unchanged" means
 * every column, byte for byte.
 */
@SpringBootTest
class TaskAttemptHistoryTest extends PostgresIntegrationTest {

    private static final Instant FIXTURE_TIME = Instant.parse("2000-01-01T00:00:00Z");
    private static final UUID OWNER = UUID.fromString("00000000-0000-0000-0000-00000000000a");

    @Autowired
    private TaskService taskService;

    @Autowired
    private TaskAttemptRepository taskAttemptRepository;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Test
    void claimOpensAttemptOneAtClaimedAt() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);

        assertEquals(List.of(new TaskAttempt(
                claimed.id(), 1, workerId, claimed.claimedAt(), null, null, null)
        ), taskService.getAttempts(claimed.id()));
    }

    @Test
    void completeClosesTheAttemptAtFinishedAt() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);

        TransitionResult.Applied applied = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.completeTask(claimed.id(), workerId, claimed.attempt(), "{\"ok\":true}"));

        assertEquals(List.of(new TaskAttempt(
                claimed.id(), 1, workerId, claimed.claimedAt(),
                applied.task().finishedAt(), AttemptOutcome.COMPLETED, null)
        ), taskService.getAttempts(claimed.id()));
    }

    @Test
    void failClosesTheAttemptWithTheStoredError() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);
        String error = "\u0000" + "a".repeat(4005);

        TransitionResult.Applied applied = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.failTask(claimed.id(), workerId, claimed.attempt(), error));

        assertEquals("a".repeat(4000), applied.task().error());
        assertEquals(List.of(new TaskAttempt(
                claimed.id(), 1, workerId, claimed.claimedAt(),
                applied.task().finishedAt(), AttemptOutcome.FAILED, "a".repeat(4000))
        ), taskService.getAttempts(claimed.id()));
        Boolean sameError = jdbcClient.sql("""
                        SELECT a.error = t.error
                        FROM task_attempts a
                        JOIN tasks t ON t.id = a.task_id
                        WHERE a.task_id = :id
                        """)
                .param("id", claimed.id())
                .query(Boolean.class)
                .single();
        assertEquals(Boolean.TRUE, sameError);
    }

    @Test
    void alreadyAppliedRejectedAndCancelLeaveHistoryUnchanged() {
        UUID workerId = UUID.randomUUID();
        Task completed = claim(workerId);
        Task failed = claim(workerId);
        Task running = claim(workerId);
        Task pending = taskService.createTask("NOOP", "{\"test\":\"history-pending\"}");
        assertInstanceOf(TransitionResult.Applied.class,
                taskService.completeTask(completed.id(), workerId, completed.attempt(), "{\"n\":1}"));
        assertInstanceOf(TransitionResult.Applied.class,
                taskService.failTask(failed.id(), workerId, failed.attempt(), "first"));
        List<String> before = allHistory();
        assertEquals(3, before.size());

        assertInstanceOf(TransitionResult.AlreadyApplied.class,
                taskService.completeTask(completed.id(), workerId, completed.attempt(), "{\"n\":2}"));
        assertInstanceOf(TransitionResult.AlreadyApplied.class,
                taskService.failTask(failed.id(), workerId, failed.attempt(), "second"));
        assertInstanceOf(TransitionResult.Rejected.class,
                taskService.failTask(completed.id(), workerId, completed.attempt(), "wrong status"));
        assertInstanceOf(TransitionResult.Rejected.class,
                taskService.completeTask(completed.id(), UUID.randomUUID(), completed.attempt(), "{}"));
        assertInstanceOf(TransitionResult.Rejected.class,
                taskService.completeTask(running.id(), UUID.randomUUID(), running.attempt(), "{}"));
        assertInstanceOf(TransitionResult.Rejected.class,
                taskService.failTask(running.id(), workerId, running.attempt() + 1, "stale"));
        assertInstanceOf(TransitionResult.Rejected.class,
                taskService.completeTask(pending.id(), workerId, 0, "{}"));
        assertInstanceOf(CancelResult.NotCancellable.class, taskService.cancelTask(running.id()));
        assertInstanceOf(CancelResult.Cancelled.class, taskService.cancelTask(pending.id()));
        assertInstanceOf(CancelResult.AlreadyCancelled.class, taskService.cancelTask(pending.id()));

        assertEquals(before, allHistory());
    }

    @Test
    void rolledBackClaimLeavesNoHistory() {
        Task created = taskService.createTask("NOOP", "{\"test\":\"rollback-claim\"}");
        UUID workerId = UUID.randomUUID();

        Task claimed = new TransactionTemplate(transactionManager).execute(status -> {
            Task inside = taskService.claimTask(workerId);
            assertNotNull(inside);
            assertEquals(1, taskService.getAttempts(inside.id()).size());
            status.setRollbackOnly();
            return inside;
        });

        assertEquals(created.id(), claimed.id());
        assertEquals(List.of(), allHistory());
        Task after = taskService.getTask(created.id());
        assertEquals(TaskStatus.PENDING, after.status());
        assertEquals(0, after.attempt());
    }

    @Test
    void rolledBackCompleteLeavesTheAttemptOpen() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);
        List<String> before = allHistory();

        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            assertInstanceOf(TransitionResult.Applied.class,
                    taskService.completeTask(claimed.id(), workerId, claimed.attempt(), "{\"ok\":true}"));
            assertEquals(AttemptOutcome.COMPLETED, taskService.getAttempts(claimed.id()).getFirst().outcome());
            status.setRollbackOnly();
        });

        assertEquals(before, allHistory());
        assertEquals(TaskStatus.RUNNING, taskService.getTask(claimed.id()).status());
    }

    @ParameterizedTest(name = "closed={0}")
    @ValueSource(booleans = {false, true})
    void existingRowForTheAttemptDoesNotBlockTheClaim(boolean closed) {
        Task created = taskService.createTask("NOOP", "{\"test\":\"conflict\"}");
        insertAttempt(created.id(), 1, OWNER, closed);
        List<String> before = allHistory();
        UUID workerId = UUID.randomUUID();

        Task claimed = taskService.claimTask(workerId);

        assertNotNull(claimed);
        assertEquals(created.id(), claimed.id());
        assertEquals(TaskStatus.RUNNING, claimed.status());
        assertEquals(workerId, claimed.workerId());
        assertEquals(1, claimed.attempt());
        assertEquals(claimed, taskService.getTask(created.id()));
        assertEquals(before, allHistory());
        assertFalse(taskAttemptRepository.recordStart(claimed.id(), workerId, claimed.attempt()));
        assertEquals(before, allHistory());
    }

    @Test
    void conflictingTaskDoesNotBlockTheNextClaim() {
        Task first = taskService.createTask("NOOP", "{\"test\":\"conflict-first\"}");
        Task second = taskService.createTask("NOOP", "{\"test\":\"conflict-second\"}");
        setCreatedAt(first.id(), FIXTURE_TIME);
        setCreatedAt(second.id(), FIXTURE_TIME.plusSeconds(1));
        insertAttempt(first.id(), 1, OWNER, false);
        List<String> firstHistory = history(first.id());
        UUID workerX = UUID.randomUUID();
        UUID workerY = UUID.randomUUID();

        Task claimedX = taskService.claimTask(workerX);
        Task claimedY = taskService.claimTask(workerY);

        assertNotNull(claimedX);
        assertNotNull(claimedY);
        assertEquals(first.id(), claimedX.id());
        assertEquals(second.id(), claimedY.id());
        assertEquals(firstHistory, history(first.id()));
        assertEquals(List.of(new TaskAttempt(
                second.id(), 1, workerY, claimedY.claimedAt(), null, null, null)
        ), taskService.getAttempts(second.id()));
    }

    @Test
    void legacyRunningRowWithoutHistoryGetsAClosedRowOnComplete() {
        UUID id = insertLegacyRunning();
        assertEquals(List.of(), taskService.getAttempts(id));

        TransitionResult.Applied applied = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.completeTask(id, OWNER, 0, "{\"legacy\":true}"));

        assertEquals(List.of(new TaskAttempt(
                id, 0, OWNER, FIXTURE_TIME, applied.task().finishedAt(), AttemptOutcome.COMPLETED, null)
        ), taskService.getAttempts(id));
    }

    @Test
    void closedAttemptIsNotOverwrittenByComplete() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim(workerId);
        jdbcClient.sql("""
                        UPDATE task_attempts
                        SET ended_at = :endedAt, outcome = 'LEASE_EXPIRED'
                        WHERE task_id = :id AND attempt = 1
                        """)
                .param("endedAt", offset(FIXTURE_TIME), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("id", claimed.id())
                .update();
        List<String> before = allHistory();

        TransitionResult.Applied applied = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.completeTask(claimed.id(), workerId, claimed.attempt(), "{\"ok\":true}"));

        assertEquals(TaskStatus.COMPLETED, applied.task().status());
        assertEquals(TaskStatus.COMPLETED, taskService.getTask(claimed.id()).status());
        assertEquals(before, allHistory());
        assertFalse(taskAttemptRepository.recordFinish(claimed.id(), claimed.attempt(), AttemptOutcome.COMPLETED));
        assertEquals(before, allHistory());
    }

    @Test
    void deletingTasksDeletesTheirHistory() {
        UUID workerId = UUID.randomUUID();
        Task completed = claim(workerId);
        claim(workerId);
        assertInstanceOf(TransitionResult.Applied.class,
                taskService.completeTask(completed.id(), workerId, completed.attempt(), "{}"));
        assertEquals(2, allHistory().size());

        jdbcClient.sql("DELETE FROM tasks").update();

        assertEquals(List.of(), allHistory());
    }

    @Test
    void getAttemptsIsOrderedEmptyBeforeAClaimAndNotFoundForAMissingTask() {
        Task created = taskService.createTask("NOOP", "{\"test\":\"attempts\"}");
        assertEquals(List.of(), taskService.getAttempts(created.id()));

        insertAttempt(created.id(), 2, OWNER, true);
        insertAttempt(created.id(), 1, OWNER, true);
        jdbcClient.sql("UPDATE tasks SET attempt = 2 WHERE id = :id")
                .param("id", created.id())
                .update();
        UUID workerId = UUID.randomUUID();
        Task claimed = taskService.claimTask(workerId);
        assertNotNull(claimed);
        assertEquals(3, claimed.attempt());

        List<TaskAttempt> attempts = taskService.getAttempts(created.id());
        assertEquals(List.of(1, 2, 3), attempts.stream().map(TaskAttempt::attempt).toList());
        assertEquals(new TaskAttempt(created.id(), 3, workerId, claimed.claimedAt(), null, null, null),
                attempts.get(2));

        UUID missing = UUID.randomUUID();
        TaskNotFoundException notFound = assertThrows(
                TaskNotFoundException.class, () -> taskService.getAttempts(missing));
        assertEquals(missing, notFound.taskId());
        assertThrows(IllegalArgumentException.class, () -> taskService.getAttempts(null));
    }

    private Task claim(UUID workerId) {
        taskService.createTask("NOOP", "{\"test\":\"history\"}");
        Task claimed = taskService.claimTask(workerId);
        assertNotNull(claimed);
        assertEquals(1, claimed.attempt());
        return claimed;
    }

    /** A {@code RUNNING} row claimed before V6 counted history: attempt 0 and no attempt row. */
    private UUID insertLegacyRunning() {
        UUID id = UUID.randomUUID();
        jdbcClient.sql("""
                        INSERT INTO tasks (
                            id, task_type, status, payload, created_at, updated_at,
                            claimed_at, worker_id, lease_expires_at
                        ) VALUES (
                            :id, 'NOOP', 'RUNNING', CAST('{}' AS jsonb), :at, :at, :at, :workerId, :at
                        )
                        """)
                .param("id", id)
                .param("at", offset(FIXTURE_TIME), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("workerId", OWNER)
                .update();
        return id;
    }

    private void insertAttempt(UUID taskId, int attempt, UUID workerId, boolean closed) {
        jdbcClient.sql("""
                        INSERT INTO task_attempts (task_id, attempt, worker_id, started_at, ended_at, outcome, error)
                        VALUES (
                            :taskId, :attempt, :workerId, :startedAt,
                            CAST(:endedAt AS timestamptz), CAST(:outcome AS text), CAST(:error AS text)
                        )
                        """)
                .param("taskId", taskId)
                .param("attempt", attempt)
                .param("workerId", workerId)
                .param("startedAt", offset(FIXTURE_TIME), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("endedAt", closed ? offset(FIXTURE_TIME.plusSeconds(1)) : null, Types.TIMESTAMP_WITH_TIMEZONE)
                .param("outcome", closed ? "LEASE_EXPIRED" : null, Types.VARCHAR)
                .param("error", closed ? "by hand" : null, Types.VARCHAR)
                .update();
    }

    private void setCreatedAt(UUID id, Instant createdAt) {
        jdbcClient.sql("UPDATE tasks SET created_at = :createdAt WHERE id = :id")
                .param("createdAt", offset(createdAt), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("id", id)
                .update();
    }

    private List<String> history(UUID taskId) {
        return jdbcClient.sql("""
                        SELECT a::text
                        FROM task_attempts a
                        WHERE a.task_id = :id
                        ORDER BY a.attempt
                        """)
                .param("id", taskId)
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

    private static OffsetDateTime offset(Instant instant) {
        return instant == null ? null : OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
