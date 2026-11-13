package com.kushal.workflow.worker;

import com.kushal.workflow.task.LeaseProperties;
import com.kushal.workflow.task.Task;
import com.kushal.workflow.task.TaskAttempt;
import com.kushal.workflow.task.TaskService;
import com.kushal.workflow.task.TaskStatus;
import com.kushal.workflow.task.TransitionResult;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessResourceFailureException;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The recovery tests' recording service and context: results are kept per
 * task, worker, and attempt; the scripted complete failure fires once and
 * writes nothing; and a claim here writes a one-second lease.
 */
class RecordingTaskServiceTest extends RecoveryContextTest {

    @Autowired
    private TaskService taskService;

    @Autowired
    private LeaseProperties leaseProperties;

    @Test
    void finishesAreRecordedPerTaskWorkerAndAttemptInOrder() {
        RecordingTaskService recording = new RecordingTaskService(taskService);
        UUID workerA = UUID.randomUUID();
        UUID workerB = UUID.randomUUID();
        Task first = claim(recording, workerA);
        Task second = claim(recording, workerA);

        TransitionResult applied = recording.completeTask(first.id(), workerA, 1, "{\"n\":1}");
        TransitionResult again = recording.completeTask(first.id(), workerA, 1, "{\"n\":2}");
        TransitionResult rejected = recording.completeTask(first.id(), workerB, 1, "{\"n\":3}");
        TransitionResult failed = recording.failTask(second.id(), workerA, 1, "boom");

        assertInstanceOf(TransitionResult.Applied.class, applied);
        assertInstanceOf(TransitionResult.AlreadyApplied.class, again);
        assertEquals(new TransitionResult.Rejected(TaskStatus.COMPLETED, workerA), rejected);
        assertInstanceOf(TransitionResult.Applied.class, failed);
        assertEquals(List.of(applied, again), recording.finishResults(first.id(), workerA, 1));
        assertEquals(List.of(rejected), recording.finishResults(first.id(), workerB, 1));
        assertEquals(List.of(failed), recording.finishResults(second.id(), workerA, 1));
        assertEquals(List.of(), recording.finishResults(first.id(), workerA, 2));
        assertEquals(List.of(
                new RecordingTaskService.Finish(
                        new RecordingTaskService.Attempt(first.id(), workerA, 1),
                        RecordingTaskService.FinishKind.COMPLETE, applied),
                new RecordingTaskService.Finish(
                        new RecordingTaskService.Attempt(first.id(), workerA, 1),
                        RecordingTaskService.FinishKind.COMPLETE, again),
                new RecordingTaskService.Finish(
                        new RecordingTaskService.Attempt(first.id(), workerB, 1),
                        RecordingTaskService.FinishKind.COMPLETE, rejected),
                new RecordingTaskService.Finish(
                        new RecordingTaskService.Attempt(second.id(), workerA, 1),
                        RecordingTaskService.FinishKind.FAIL, failed)
        ), recording.finishes());
        assertEquals(recording.finishes().subList(0, 3), recording.finishes(first.id()));
        assertEquals(List.of(
                new RecordingTaskService.Attempt(first.id(), workerA, 1),
                new RecordingTaskService.Attempt(second.id(), workerA, 1)
        ), recording.claims());
    }

    @Test
    void extendResultsAreRecordedPerAttempt() {
        RecordingTaskService recording = new RecordingTaskService(taskService);
        UUID workerA = UUID.randomUUID();
        UUID workerB = UUID.randomUUID();
        Task claimed = claim(recording, workerA);

        assertTrue(recording.extendLease(claimed.id(), workerA, 1));
        assertFalse(recording.extendLease(claimed.id(), workerB, 1));
        assertFalse(recording.extendLease(claimed.id(), workerA, 2));
        assertTrue(recording.extendLease(claimed.id(), workerA, 1));

        assertEquals(List.of(true, true), recording.extendResults(claimed.id(), workerA, 1));
        assertEquals(List.of(false), recording.extendResults(claimed.id(), workerB, 1));
        assertEquals(List.of(false), recording.extendResults(claimed.id(), workerA, 2));
        assertEquals(List.of(), recording.finishes());
    }

    @Test
    void scriptedCompleteFailureFiresOnceForThatWorkerAndWritesNothing() {
        RecordingTaskService recording = new RecordingTaskService(taskService);
        UUID workerA = UUID.randomUUID();
        UUID workerB = UUID.randomUUID();
        Task a = claim(recording, workerA);
        Task b = claim(recording, workerB);
        recording.throwOnceFromComplete(workerA);

        TransitionResult other = recording.completeTask(b.id(), workerB, 1, "{}");
        assertThrows(DataAccessResourceFailureException.class,
                () -> recording.completeTask(a.id(), workerA, 1, "{\"lost\":true}"));

        assertInstanceOf(TransitionResult.Applied.class, other);
        Task afterThrow = taskService.getTask(a.id());
        assertEquals(TaskStatus.RUNNING, afterThrow.status());
        assertEquals(workerA, afterThrow.workerId());
        assertEquals(1, afterThrow.attempt());
        assertNull(afterThrow.result());
        List<TaskAttempt> attempts = taskService.getAttempts(a.id());
        assertEquals(1, attempts.size());
        assertNull(attempts.getFirst().outcome());
        assertEquals(List.of(), recording.finishResults(a.id(), workerA, 1));
        assertEquals(List.of(new RecordingTaskService.Attempt(a.id(), workerA, 1)), recording.scriptedThrows());

        TransitionResult next = assertDoesNotThrow(() -> recording.completeTask(a.id(), workerA, 1, "{}"));

        assertInstanceOf(TransitionResult.Applied.class, next);
        assertEquals(List.of(next), recording.finishResults(a.id(), workerA, 1));
        assertEquals(1, recording.scriptedThrows().size());
    }

    @Test
    void claimInThisContextWritesAOneSecondLease() {
        assertEquals(Duration.ofSeconds(1), leaseProperties.getDuration());
        assertEquals(Duration.ofMillis(300), leaseProperties.getHeartbeatInterval());
        RecordingTaskService recording = new RecordingTaskService(taskService);

        Task claimed = claim(recording, UUID.randomUUID());

        assertEquals(claimed.claimedAt().plus(Duration.ofSeconds(1)), claimed.leaseExpiresAt());
    }

    @Test
    void everyPublicTaskServiceMethodIsOverridden() {
        List<String> missing = Arrays.stream(TaskService.class.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()) && !Modifier.isStatic(method.getModifiers()))
                .filter(method -> !declaredBy(RecordingTaskService.class, method))
                .map(Method::getName)
                .toList();

        assertEquals(List.of(), missing);
    }

    private static boolean declaredBy(Class<?> type, Method method) {
        try {
            return type.getDeclaredMethod(method.getName(), method.getParameterTypes()).getDeclaringClass() == type;
        } catch (NoSuchMethodException ex) {
            return false;
        }
    }

    private static Task claim(RecordingTaskService recording, UUID workerId) {
        Task created = recording.createTask("NOOP", "{\"test\":\"recording\"}");
        Task claimed = recording.claimTask(workerId);
        assertNotNull(claimed);
        assertEquals(created.id(), claimed.id());
        assertEquals(1, claimed.attempt());
        return claimed;
    }
}
