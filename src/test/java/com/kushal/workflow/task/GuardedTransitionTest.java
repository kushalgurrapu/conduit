package com.kushal.workflow.task;

import com.kushal.workflow.support.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Proves that complete, fail, and cancel are each one update, and that the
 * {@code WHERE} clause decides whether the row changes. A rejected write
 * leaves every column as it was, including the backdated {@code updated_at}.
 */
@SpringBootTest
class GuardedTransitionTest extends PostgresIntegrationTest {

    private static final Instant FIXTURE_TIME = Instant.parse("2000-01-01T00:00:00Z");
    private static final UUID OWNER = UUID.fromString("00000000-0000-0000-0000-00000000000a");
    private static final String COMPLETE_RESULT = "{\"applied\":true}";
    private static final String FAIL_ERROR = "boom";

    @Autowired
    private TaskService taskService;

    @Autowired
    private JdbcClient jdbcClient;

    @ParameterizedTest(name = "{0} then {1} -> {2}")
    @MethodSource("transitions")
    void whereClauseIsTheStateMachine(TaskStatus starting, Operation operation, Kind expected) {
        StoredTask before = insert(starting);
        assertEquals(FIXTURE_TIME, before.updatedAt());

        switch (operation) {
            case COMPLETE -> assertFinish(
                    expected,
                    operation,
                    taskService.completeTask(before.id(), OWNER, COMPLETE_RESULT),
                    before);
            case FAIL -> assertFinish(
                    expected,
                    operation,
                    taskService.failTask(before.id(), OWNER, FAIL_ERROR),
                    before);
            case CANCEL -> assertCancel(expected, taskService.cancelTask(before.id()), before);
        }
    }

    static Stream<Arguments> transitions() {
        return Stream.of(
                arguments(TaskStatus.PENDING, Operation.COMPLETE, Kind.REJECTED),
                arguments(TaskStatus.PENDING, Operation.FAIL, Kind.REJECTED),
                arguments(TaskStatus.PENDING, Operation.CANCEL, Kind.CANCELLED),
                arguments(TaskStatus.RUNNING, Operation.COMPLETE, Kind.APPLIED),
                arguments(TaskStatus.RUNNING, Operation.FAIL, Kind.APPLIED),
                arguments(TaskStatus.RUNNING, Operation.CANCEL, Kind.NOT_CANCELLABLE),
                arguments(TaskStatus.COMPLETED, Operation.COMPLETE, Kind.ALREADY_APPLIED),
                arguments(TaskStatus.COMPLETED, Operation.FAIL, Kind.REJECTED),
                arguments(TaskStatus.COMPLETED, Operation.CANCEL, Kind.NOT_CANCELLABLE),
                arguments(TaskStatus.FAILED, Operation.COMPLETE, Kind.REJECTED),
                arguments(TaskStatus.FAILED, Operation.FAIL, Kind.ALREADY_APPLIED),
                arguments(TaskStatus.FAILED, Operation.CANCEL, Kind.NOT_CANCELLABLE),
                arguments(TaskStatus.CANCELLED, Operation.COMPLETE, Kind.REJECTED),
                arguments(TaskStatus.CANCELLED, Operation.FAIL, Kind.REJECTED),
                arguments(TaskStatus.CANCELLED, Operation.CANCEL, Kind.ALREADY_CANCELLED)
        );
    }

    @Test
    void secondCompleteWithADifferentResultIsAlreadyApplied() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim("{\"test\":\"double-complete\"}", workerId);

        TransitionResult.Applied first = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.completeTask(claimed.id(), workerId, "{\"n\":1}"));
        StoredTask afterFirst = read(claimed.id());

        TransitionResult.AlreadyApplied second = assertInstanceOf(
                TransitionResult.AlreadyApplied.class,
                taskService.completeTask(claimed.id(), workerId, "{\"n\":2}"));

        assertEquals(afterFirst, read(claimed.id()));
        assertEquals(first.task().result(), second.task().result());
        assertEquals(first.task().updatedAt(), second.task().updatedAt());
        assertEquals(first.task().finishedAt(), second.task().finishedAt());
        assertTrue(resultEquals(claimed.id(), "{\"n\":1}"));
        assertFalse(resultEquals(claimed.id(), "{\"n\":2}"));
    }

    @Test
    void completeByADifferentWorkerIsRejected() {
        UUID owner = UUID.randomUUID();
        Task claimed = claim("{\"test\":\"other-worker\"}", owner);
        StoredTask before = read(claimed.id());

        TransitionResult.Rejected rejected = assertInstanceOf(
                TransitionResult.Rejected.class,
                taskService.completeTask(claimed.id(), UUID.randomUUID(), "{\"x\":1}"));

        assertEquals(TaskStatus.RUNNING, rejected.currentStatus());
        assertEquals(owner, rejected.currentWorker());
        assertEquals(before, read(claimed.id()));
    }

    @Test
    void ownerPinnedRequeueRejectsTheOldWorkerAndLetsTheNewOneFinish() {
        UUID workerA = UUID.randomUUID();
        UUID workerB = UUID.randomUUID();
        Task claimed = claim("{\"test\":\"requeue\"}", workerA);

        Optional<UUID> requeued = jdbcClient.sql("""
                        UPDATE tasks
                        SET status = 'PENDING', worker_id = NULL, claimed_at = NULL, updated_at = now()
                        WHERE id = :id AND status = 'RUNNING' AND worker_id = :observedWorkerId
                        RETURNING id
                        """)
                .param("id", claimed.id())
                .param("observedWorkerId", workerA)
                .query(UUID.class)
                .optional();
        assertEquals(Optional.of(claimed.id()), requeued);

        Task claimedByB = taskService.claimTask(workerB);
        assertNotNull(claimedByB);
        assertEquals(claimed.id(), claimedByB.id());
        assertEquals(workerB, claimedByB.workerId());

        StoredTask ownedByB = read(claimed.id());
        TransitionResult.Rejected rejected = assertInstanceOf(
                TransitionResult.Rejected.class,
                taskService.completeTask(claimed.id(), workerA, "{\"by\":\"A\"}"));
        assertEquals(TaskStatus.RUNNING, rejected.currentStatus());
        assertEquals(workerB, rejected.currentWorker());
        assertEquals(ownedByB, read(claimed.id()));

        TransitionResult.Applied applied = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.completeTask(claimed.id(), workerB, "{\"by\":\"B\"}"));
        assertEquals(TaskStatus.COMPLETED, applied.task().status());
        assertTrue(resultEquals(claimed.id(), "{\"by\":\"B\"}"));
        assertTrue(applied.task().updatedAt().isAfter(ownedByB.updatedAt()));
        assertEquals(applied.task().updatedAt(), applied.task().finishedAt());
    }

    @Test
    void missingTaskIsRejectedOnTheWorkerPathAndNotFoundOnCancel() {
        UUID missing = UUID.randomUUID();

        TransitionResult.Rejected completed = assertInstanceOf(
                TransitionResult.Rejected.class,
                taskService.completeTask(missing, UUID.randomUUID(), "{\"x\":1}"));
        assertNull(completed.currentStatus());
        assertNull(completed.currentWorker());

        TransitionResult.Rejected failed = assertInstanceOf(
                TransitionResult.Rejected.class,
                taskService.failTask(missing, UUID.randomUUID(), "gone"));
        assertNull(failed.currentStatus());
        assertNull(failed.currentWorker());

        TaskNotFoundException notFound = assertThrows(
                TaskNotFoundException.class, () -> taskService.cancelTask(missing));
        assertEquals(missing, notFound.taskId());
    }

    @Test
    void completeAndFailRejectANullWorkerAndCancelRejectsANullId() {
        Task created = taskService.createTask("NOOP", "{\"test\":\"null-guard\"}");

        assertThrows(IllegalArgumentException.class,
                () -> taskService.completeTask(created.id(), null, "{}"));
        assertThrows(IllegalArgumentException.class,
                () -> taskService.failTask(created.id(), null, "x"));
        assertThrows(IllegalArgumentException.class,
                () -> taskService.completeTask(null, UUID.randomUUID(), "{}"));
        assertThrows(IllegalArgumentException.class,
                () -> taskService.cancelTask(null));

        assertEquals("PENDING", read(created.id()).status());
    }

    @Test
    void failStripsNullBytesThenTruncatesInSql() {
        UUID workerId = UUID.randomUUID();
        Task claimed = claim("{\"test\":\"truncate\"}", workerId);
        String error = "\u0000" + "a".repeat(4005);

        TransitionResult.Applied applied = assertInstanceOf(
                TransitionResult.Applied.class,
                taskService.failTask(claimed.id(), workerId, error));

        assertEquals("a".repeat(4000), applied.task().error());
        assertFalse(applied.task().error().indexOf('\0') >= 0);
        assertEquals(applied.task().error(), read(claimed.id()).error());
    }

    private void assertFinish(Kind expected, Operation operation, TransitionResult result, StoredTask before) {
        StoredTask after = read(before.id());
        switch (expected) {
            case APPLIED -> assertApplied(operation, result, before, after);
            case ALREADY_APPLIED -> assertAlreadyApplied(result, before, after);
            case REJECTED -> assertRejected(result, before, after);
            default -> fail("complete/fail cannot expect " + expected);
        }
    }

    private void assertCancel(Kind expected, CancelResult result, StoredTask before) {
        StoredTask after = read(before.id());
        switch (expected) {
            case CANCELLED -> {
                CancelResult.Cancelled cancelled = assertInstanceOf(CancelResult.Cancelled.class, result);
                assertEquals(TaskStatus.CANCELLED, cancelled.task().status());
                assertMovedTerminalTimestamps(before, after, cancelled.task());
                assertEquals("CANCELLED", after.status());
                assertNull(after.workerId());
                assertNull(after.claimedAt());
                assertUntouchedPayload(before, after);
            }
            case ALREADY_CANCELLED -> {
                CancelResult.AlreadyCancelled already = assertInstanceOf(CancelResult.AlreadyCancelled.class, result);
                assertEquals(before, after);
                assertEquals(before.finishedAt(), already.task().finishedAt());
                assertEquals(TaskStatus.CANCELLED, already.task().status());
            }
            case NOT_CANCELLABLE -> {
                CancelResult.NotCancellable rejected = assertInstanceOf(CancelResult.NotCancellable.class, result);
                assertEquals(TaskStatus.valueOf(before.status()), rejected.status());
                assertEquals(before, after);
            }
            default -> fail("cancel cannot expect " + expected);
        }
    }

    private void assertApplied(Operation operation, TransitionResult result, StoredTask before, StoredTask after) {
        TransitionResult.Applied applied = assertInstanceOf(TransitionResult.Applied.class, result);
        assertMovedTerminalTimestamps(before, after, applied.task());
        assertUntouchedPayload(before, after);
        assertEquals(before.claimedAt(), after.claimedAt());
        assertEquals(before.workerId(), after.workerId());
        if (operation == Operation.COMPLETE) {
            assertEquals("COMPLETED", after.status());
            assertEquals(TaskStatus.COMPLETED, applied.task().status());
            assertTrue(resultEquals(before.id(), COMPLETE_RESULT));
            assertNull(after.error());
        } else {
            assertEquals("FAILED", after.status());
            assertEquals(TaskStatus.FAILED, applied.task().status());
            assertEquals(FAIL_ERROR, after.error());
            assertEquals(FAIL_ERROR, applied.task().error());
            assertNull(after.result());
        }
    }

    private void assertAlreadyApplied(TransitionResult result, StoredTask before, StoredTask after) {
        TransitionResult.AlreadyApplied already = assertInstanceOf(TransitionResult.AlreadyApplied.class, result);
        assertEquals(before, after);
        assertEquals(before.result(), already.task().result());
        assertEquals(before.error(), already.task().error());
        assertEquals(before.updatedAt(), already.task().updatedAt());
        assertEquals(before.finishedAt(), already.task().finishedAt());
        assertEquals(TaskStatus.valueOf(before.status()), already.task().status());
    }

    private void assertRejected(TransitionResult result, StoredTask before, StoredTask after) {
        TransitionResult.Rejected rejected = assertInstanceOf(TransitionResult.Rejected.class, result);
        assertEquals(TaskStatus.valueOf(before.status()), rejected.currentStatus());
        assertEquals(before.workerId(), rejected.currentWorker());
        assertEquals(before, after);
    }

    private static void assertMovedTerminalTimestamps(StoredTask before, StoredTask after, Task returned) {
        assertTrue(after.updatedAt().isAfter(before.updatedAt()));
        assertTrue(after.finishedAt().isAfter(FIXTURE_TIME));
        assertEquals(after.updatedAt(), after.finishedAt());
        assertEquals(after.updatedAt(), returned.updatedAt());
        assertEquals(after.finishedAt(), returned.finishedAt());
        assertEquals(after.status(), returned.status().name());
    }

    private static void assertUntouchedPayload(StoredTask before, StoredTask after) {
        assertEquals(before.id(), after.id());
        assertEquals(before.taskType(), after.taskType());
        assertEquals(before.payload(), after.payload());
        assertEquals(before.createdAt(), after.createdAt());
    }

    private Task claim(String payload, UUID workerId) {
        taskService.createTask("NOOP", payload);
        Task claimed = taskService.claimTask(workerId);
        assertNotNull(claimed);
        return claimed;
    }

    private StoredTask insert(TaskStatus status) {
        UUID id = UUID.randomUUID();
        UUID workerId = status == TaskStatus.PENDING || status == TaskStatus.CANCELLED ? null : OWNER;
        Instant claimedAt = workerId == null ? null : FIXTURE_TIME;
        Instant finishedAt = status.isTerminal() ? FIXTURE_TIME : null;
        String result = status == TaskStatus.COMPLETED ? "{\"from\":\"fixture\"}" : null;
        String error = status == TaskStatus.FAILED ? "fixture-error" : null;

        jdbcClient.sql("""
                        INSERT INTO tasks (
                            id, task_type, status, payload, result, error,
                            created_at, updated_at, claimed_at, finished_at, worker_id
                        ) VALUES (
                            :id, 'NOOP', :status, CAST(:payload AS jsonb),
                            CAST(:result AS jsonb), :error,
                            CAST(:createdAt AS timestamptz), CAST(:updatedAt AS timestamptz),
                            CAST(:claimedAt AS timestamptz), CAST(:finishedAt AS timestamptz),
                            CAST(:workerId AS uuid)
                        )
                        """)
                .param("id", id)
                .param("status", status.name())
                .param("payload", "{\"test\":\"transition\"}")
                .param("result", result, Types.VARCHAR)
                .param("error", error, Types.VARCHAR)
                .param("createdAt", offset(FIXTURE_TIME), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("updatedAt", offset(FIXTURE_TIME), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("claimedAt", offset(claimedAt), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("finishedAt", offset(finishedAt), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("workerId", workerId, Types.OTHER)
                .update();
        return read(id);
    }

    private StoredTask read(UUID id) {
        return jdbcClient.sql("""
                        SELECT id, task_type, status, payload::text AS payload, result::text AS result,
                               error, created_at, updated_at, claimed_at, finished_at, worker_id
                        FROM tasks
                        WHERE id = :id
                        """)
                .param("id", id)
                .query((rs, rowNum) -> new StoredTask(
                        rs.getObject("id", UUID.class),
                        rs.getString("task_type"),
                        rs.getString("status"),
                        rs.getString("payload"),
                        rs.getString("result"),
                        rs.getString("error"),
                        instant(rs, "created_at"),
                        instant(rs, "updated_at"),
                        instant(rs, "claimed_at"),
                        instant(rs, "finished_at"),
                        rs.getObject("worker_id", UUID.class)))
                .single();
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
        return instant == null ? null : OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private enum Operation {
        COMPLETE, FAIL, CANCEL
    }

    private enum Kind {
        APPLIED, ALREADY_APPLIED, REJECTED, CANCELLED, ALREADY_CANCELLED, NOT_CANCELLABLE
    }

    private record StoredTask(
            UUID id,
            String taskType,
            String status,
            String payload,
            String result,
            String error,
            Instant createdAt,
            Instant updatedAt,
            Instant claimedAt,
            Instant finishedAt,
            UUID workerId
    ) {
    }
}
