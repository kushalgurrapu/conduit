package com.kushal.workflow.worker;

import com.kushal.workflow.task.AttemptOutcome;
import com.kushal.workflow.task.CancelResult;
import com.kushal.workflow.task.LeaseProperties;
import com.kushal.workflow.task.Task;
import com.kushal.workflow.task.TaskService;
import com.kushal.workflow.task.TaskStatus;
import com.kushal.workflow.task.TransitionResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Recovery and the attempt fence against a later claim. An attempt whose
 * lease stops moving is reaped and completed by a later attempt, and the
 * old attempt's extend and finish then match nothing: each finish gets the
 * exact {@link TransitionResult.Rejected} for the row's state and leaves
 * the row and its history byte-for-byte unchanged.
 *
 * <p>Every executor is built here, with its own {@link ManualScheduler},
 * so a heartbeat ticks only when a test fires it. Leases expire naturally
 * on the database clock, and the reaper runs only through
 * {@link LeaseReaper#reapOnce()}.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class RecoveryIntegrationTest extends RecoveryContextTest {

    private static final long WAIT_SECONDS = 20;

    @Autowired
    private TaskService taskService;

    @Autowired
    private LeaseProperties leaseProperties;

    @Autowired
    private JsonMapper jsonMapper;

    @Autowired
    private JdbcClient jdbcClient;

    private final UUID workerA = UUID.randomUUID();
    private final UUID workerB = UUID.randomUUID();
    private final RecHandler handler = new RecHandler();
    private final List<Worker> workers = new CopyOnWriteArrayList<>();
    private final ManualScheduler reaperScheduler = new ManualScheduler();
    private RecordingTaskService recording;
    private LeaseReaper reaper;

    @BeforeEach
    void buildRecordingServiceAndReaper() {
        recording = new RecordingTaskService(taskService);
        reaper = new LeaseReaper(taskService, new ReaperProperties(), reaperScheduler);
    }

    @AfterEach
    void stopEveryThread() throws InterruptedException {
        handler.releaseAll();
        for (Worker worker : workers) {
            worker.thread.interrupt();
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        List<String> alive = new ArrayList<>();
        for (Worker worker : workers) {
            long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            worker.thread.join(Math.max(1, remainingMillis));
            if (worker.thread.isAlive()) {
                alive.add(worker.thread.getName());
            }
        }
        for (Worker worker : workers) {
            worker.heartbeat.stop();
            worker.scheduler.shutdownNow();
        }
        reaperScheduler.shutdownNow();
        assertEquals(List.of(), alive, "threads still alive");
    }

    @Test
    void crashedAttemptIsRequeuedAndCompletedByAnotherWorker() throws Exception {
        UUID id = createTask();
        Script a = handler.script(workerA, 1, Outcome.SUCCEED);
        Worker zombie = crashAndReap(id, a);
        assertEquals(List.of(), recording.extendResults(id, workerA, 1));

        runToTheEnd("B", workerB, 2, handler.script(workerB, 2, Outcome.SUCCEED));

        assertCompleted(id, workerB, 2);
        assertEquals(List.of(
                new History(1, workerA, AttemptOutcome.LEASE_EXPIRED),
                new History(2, workerB, AttemptOutcome.COMPLETED)
        ), history(id));

        releaseZombie(zombie, a, id);

        assertEquals(List.of(new TransitionResult.Rejected(TaskStatus.COMPLETED, workerB)),
                recording.finishResults(id, workerA, 1));
        assertEquals(Boolean.FALSE, a.leaseLostAtReturn.get());
        assertEquals(List.of(
                new RecordingTaskService.Attempt(id, workerA, 1),
                new RecordingTaskService.Attempt(id, workerB, 2)
        ), recording.claims());
    }

    @Test
    void interruptedAttemptIsRequeuedAndCompleted() throws Exception {
        UUID id = createTask();
        Script a = handler.script(workerA, 1, Outcome.SUCCEED);
        Worker first = start("A", workerA, 1);
        await(a.started, "A started");

        first.thread.interrupt();
        first.joinCleanly();

        assertRow(id, TaskStatus.RUNNING, 1, workerA);
        assertTrue(first.scheduler.onlyTick().isCancelled());
        assertNull(a.leaseLostAtReturn.get(), "the handler returned instead of throwing");
        assertEquals(List.of(), recording.finishes(id));
        assertEquals(List.of(new History(1, workerA, null)), history(id));

        reapExpired(id);
        runToTheEnd("B", workerB, 2, handler.script(workerB, 2, Outcome.SUCCEED));

        assertCompleted(id, workerB, 2);
        assertEquals(List.of(
                new History(1, workerA, AttemptOutcome.LEASE_EXPIRED),
                new History(2, workerB, AttemptOutcome.COMPLETED)
        ), history(id));
    }

    @Test
    void finishThatThrowsIsRequeuedAndCompleted() throws Exception {
        UUID id = createTask();
        recording.throwOnceFromComplete(workerA);
        Script a = handler.script(workerA, 1, Outcome.SUCCEED);
        Worker first = start("A", workerA, 1);
        await(a.started, "A started");

        a.release.countDown();
        first.joinCleanly();

        assertEquals(List.of(new RecordingTaskService.Attempt(id, workerA, 1)), recording.scriptedThrows());
        assertEquals(List.of(), recording.finishes(id));
        Task stuck = assertRow(id, TaskStatus.RUNNING, 1, workerA);
        assertNull(stuck.result());
        assertNull(stuck.finishedAt());
        assertTrue(first.scheduler.onlyTick().isCancelled());
        assertEquals(List.of(new History(1, workerA, null)), history(id));

        reapExpired(id);
        runToTheEnd("B", workerB, 2, handler.script(workerB, 2, Outcome.SUCCEED));

        assertCompleted(id, workerB, 2);
        assertEquals(List.of(
                new History(1, workerA, AttemptOutcome.LEASE_EXPIRED),
                new History(2, workerB, AttemptOutcome.COMPLETED)
        ), history(id));
    }

    @Test
    void zombieCompleteAfterAnotherWorkerCompleted() throws Exception {
        UUID id = createTask();
        Script a = handler.script(workerA, 1, Outcome.SUCCEED);
        Worker zombie = crashAndReap(id, a);
        runToTheEnd("B", workerB, 2, handler.script(workerB, 2, Outcome.SUCCEED));
        assertCompleted(id, workerB, 2);

        fireZombieTick(zombie, id);
        releaseZombie(zombie, a, id);

        assertEquals(Boolean.TRUE, a.leaseLostAtReturn.get());
        assertEquals(List.of(new TransitionResult.Rejected(TaskStatus.COMPLETED, workerB)),
                recording.finishResults(id, workerA, 1));
        assertEquals(result(2, workerB), storedResult(id));
        assertEquals(List.of(
                new History(1, workerA, AttemptOutcome.LEASE_EXPIRED),
                new History(2, workerB, AttemptOutcome.COMPLETED)
        ), history(id));
    }

    @Test
    void zombieFailAfterAnotherWorkerCompleted() throws Exception {
        UUID id = createTask();
        Script a = handler.script(workerA, 1, Outcome.FAIL);
        Worker zombie = crashAndReap(id, a);
        runToTheEnd("B", workerB, 2, handler.script(workerB, 2, Outcome.SUCCEED));
        assertCompleted(id, workerB, 2);

        fireZombieTick(zombie, id);
        releaseZombie(zombie, a, id);

        assertEquals(Boolean.TRUE, a.leaseLostAtReturn.get());
        assertEquals(new RecordingTaskService.Finish(
                new RecordingTaskService.Attempt(id, workerA, 1),
                RecordingTaskService.FinishKind.FAIL,
                new TransitionResult.Rejected(TaskStatus.COMPLETED, workerB)
        ), recording.finishes(id).getLast());
        assertEquals(List.of(new TransitionResult.Rejected(TaskStatus.COMPLETED, workerB)),
                recording.finishResults(id, workerA, 1));
        Task after = assertRow(id, TaskStatus.COMPLETED, 2, workerB);
        assertNull(after.error());
        assertEquals(result(2, workerB), storedResult(id));
    }

    @Test
    void zombieAfterAnotherWorkerFailed() throws Exception {
        UUID id = createTask();
        Script a = handler.script(workerA, 1, Outcome.SUCCEED);
        Worker zombie = crashAndReap(id, a);
        runToTheEnd("B", workerB, 2, handler.script(workerB, 2, Outcome.FAIL));

        Task failed = assertRow(id, TaskStatus.FAILED, 2, workerB);
        assertEquals(reason(2, workerB), failed.error());
        assertNull(failed.result());
        assertApplied(id, workerB, 2);

        fireZombieTick(zombie, id);
        releaseZombie(zombie, a, id);

        assertEquals(Boolean.TRUE, a.leaseLostAtReturn.get());
        assertEquals(List.of(new TransitionResult.Rejected(TaskStatus.FAILED, workerB)),
                recording.finishResults(id, workerA, 1));
        assertEquals(List.of(
                new History(1, workerA, AttemptOutcome.LEASE_EXPIRED),
                new History(2, workerB, AttemptOutcome.FAILED)
        ), history(id));
    }

    @Test
    void zombieExtendIsRejectedWhileTheNewOwnerRuns() throws Exception {
        UUID id = createTask();
        Script a = handler.script(workerA, 1, Outcome.SUCCEED);
        Worker zombie = crashAndReap(id, a);
        Script b = handler.script(workerB, 2, Outcome.SUCCEED);
        Worker owner = start("B", workerB, 2);
        await(b.started, "B started");
        Task held = assertRow(id, TaskStatus.RUNNING, 2, workerB);
        assertNull(held.lastHeartbeatAt());
        Snapshot before = snapshot(id);

        fireZombieTick(zombie, id);

        assertEquals(before, snapshot(id));
        assertEquals(List.of(), recording.extendResults(id, workerB, 2));

        b.release.countDown();
        owner.joinCleanly();
        assertCompleted(id, workerB, 2);
        releaseZombie(zombie, a, id);

        assertEquals(Boolean.TRUE, a.leaseLostAtReturn.get());
        assertEquals(List.of(new TransitionResult.Rejected(TaskStatus.COMPLETED, workerB)),
                recording.finishResults(id, workerA, 1));
    }

    @Test
    void zombieFinishWhileTheNewOwnerRunsIsRejected() throws Exception {
        UUID id = createTask();
        Script a = handler.script(workerA, 1, Outcome.SUCCEED);
        Worker zombie = crashAndReap(id, a);
        Script b = handler.script(workerB, 2, Outcome.SUCCEED);
        Worker owner = start("B", workerB, 2);
        await(b.started, "B started");

        releaseZombie(zombie, a, id);

        assertEquals(Boolean.FALSE, a.leaseLostAtReturn.get());
        assertEquals(List.of(new TransitionResult.Rejected(TaskStatus.RUNNING, workerB)),
                recording.finishResults(id, workerA, 1));
        assertRow(id, TaskStatus.RUNNING, 2, workerB);
        assertEquals(List.of(
                new History(1, workerA, AttemptOutcome.LEASE_EXPIRED),
                new History(2, workerB, null)
        ), history(id));

        b.release.countDown();
        owner.joinCleanly();

        assertCompleted(id, workerB, 2);
        assertEquals(List.of(
                new History(1, workerA, AttemptOutcome.LEASE_EXPIRED),
                new History(2, workerB, AttemptOutcome.COMPLETED)
        ), history(id));
    }

    @Test
    void zombieBeforeAnyReclaimSeesPending() throws Exception {
        UUID id = createTask();
        Script a = handler.script(workerA, 1, Outcome.SUCCEED);
        Worker zombie = crashAndReap(id, a);

        fireZombieTick(zombie, id);
        releaseZombie(zombie, a, id);

        assertEquals(Boolean.TRUE, a.leaseLostAtReturn.get());
        assertEquals(List.of(new TransitionResult.Rejected(TaskStatus.PENDING, null)),
                recording.finishResults(id, workerA, 1));
        assertRow(id, TaskStatus.PENDING, 1, null);
        assertEquals(List.of(new History(1, workerA, AttemptOutcome.LEASE_EXPIRED)), history(id));

        runToTheEnd("B", workerB, 2, handler.script(workerB, 2, Outcome.SUCCEED));

        assertCompleted(id, workerB, 2);
        assertEquals(List.of(
                new History(1, workerA, AttemptOutcome.LEASE_EXPIRED),
                new History(2, workerB, AttemptOutcome.COMPLETED)
        ), history(id));
    }

    @Test
    void zombieAfterReapAndCancelIsRejected() throws Exception {
        UUID id = createTask();
        Script a = handler.script(workerA, 1, Outcome.SUCCEED);
        Worker zombie = crashAndReap(id, a);

        assertInstanceOf(CancelResult.Cancelled.class, taskService.cancelTask(id));
        fireZombieTick(zombie, id);
        releaseZombie(zombie, a, id);

        assertEquals(Boolean.TRUE, a.leaseLostAtReturn.get());
        assertEquals(List.of(new TransitionResult.Rejected(TaskStatus.CANCELLED, null)),
                recording.finishResults(id, workerA, 1));
        Task cancelled = assertRow(id, TaskStatus.CANCELLED, 1, null);
        assertNull(cancelled.result());
        assertEquals(List.of(new History(1, workerA, AttemptOutcome.LEASE_EXPIRED)), history(id));
        assertEquals(List.of(new RecordingTaskService.Attempt(id, workerA, 1)), recording.claims());
    }

    @Test
    void sameWorkerIdReclaimStillRejectsTheOldAttempt() throws Exception {
        UUID id = createTask();
        Script a1 = handler.script(workerA, 1, Outcome.SUCCEED);
        Worker zombie = crashAndReap(id, a1);
        Script a2 = handler.script(workerA, 2, Outcome.SUCCEED);
        Worker second = start("A", workerA, 2);
        await(a2.started, "A's second executor started");
        assertRow(id, TaskStatus.RUNNING, 2, workerA);

        fireZombieTick(zombie, id);
        assertEquals(List.of(), recording.extendResults(id, workerA, 2));
        Snapshot held = snapshot(id);
        TransitionResult whileHeld = recording.completeTask(
                id, workerA, 1, jsonMapper.writeValueAsString(result(1, workerA)));

        assertEquals(new TransitionResult.Rejected(TaskStatus.RUNNING, workerA), whileHeld);
        assertEquals(held, snapshot(id));

        a2.release.countDown();
        second.joinCleanly();
        assertCompleted(id, workerA, 2);
        releaseZombie(zombie, a1, id);

        assertEquals(Boolean.TRUE, a1.leaseLostAtReturn.get());
        assertEquals(List.of(
                new TransitionResult.Rejected(TaskStatus.RUNNING, workerA),
                new TransitionResult.Rejected(TaskStatus.COMPLETED, workerA)
        ), recording.finishResults(id, workerA, 1));
        assertEquals(List.of(
                new History(1, workerA, AttemptOutcome.LEASE_EXPIRED),
                new History(2, workerA, AttemptOutcome.COMPLETED)
        ), history(id));
        assertEquals(List.of(
                new RecordingTaskService.Attempt(id, workerA, 1),
                new RecordingTaskService.Attempt(id, workerA, 2)
        ), recording.claims());
    }

    @Test
    void expiredButUnreapedAttemptStillCompletes() throws Exception {
        UUID id = createTask();
        Script a = handler.script(workerA, 1, Outcome.SUCCEED);
        Worker first = start("A", workerA, 1);
        await(a.started, "A started");
        assertRow(id, TaskStatus.RUNNING, 1, workerA);
        awaitLeaseExpired(id);

        a.release.countDown();
        first.joinCleanly();

        assertEquals(Boolean.FALSE, a.leaseLostAtReturn.get());
        assertCompleted(id, workerA, 1);
        awaitLeaseExpired(id);
        assertEquals(List.of(new History(1, workerA, AttemptOutcome.COMPLETED)), history(id));
        Snapshot before = snapshot(id);

        assertEquals(0, reaper.reapOnce());

        assertEquals(before, snapshot(id));
    }

    private UUID createTask() {
        return taskService.createTask("REC", "{}").id();
    }

    /** A claims attempt 1 and blocks without heartbeats; its lease expires and one reap requeues it. */
    private Worker crashAndReap(UUID id, Script a) throws InterruptedException {
        Worker worker = start("A", workerA, 1);
        await(a.started, "A started");
        assertRow(id, TaskStatus.RUNNING, 1, workerA);
        assertEquals(List.of(new History(1, workerA, null)), history(id));
        reapExpired(id);
        return worker;
    }

    /** Waits for attempt 1 of {@link #workerA} to expire, then reaps it. */
    private void reapExpired(UUID id) throws InterruptedException {
        awaitLeaseExpired(id);

        assertEquals(1, reapAfterExpiry());

        Task pending = assertRow(id, TaskStatus.PENDING, 1, null);
        assertNull(pending.claimedAt());
        assertNull(pending.leaseExpiresAt());
        assertNull(pending.lastHeartbeatAt());
        assertEquals(List.of(new History(1, workerA, AttemptOutcome.LEASE_EXPIRED)), history(id));
    }

    private void runToTheEnd(String label, UUID workerId, int attempt, Script script) throws InterruptedException {
        Worker worker = start(label, workerId, attempt);
        await(script.started, label + " started");
        script.release.countDown();
        worker.joinCleanly();
    }

    /** Fires the zombie's tick while its handler is blocked, so the watch is still open. */
    private void fireZombieTick(Worker zombie, UUID id) {
        ManualScheduler.Tick tick = zombie.scheduler.onlyTick();
        assertFalse(tick.isCancelled(), "the zombie's watch is already closed");

        tick.fire();

        assertEquals(List.of(false), recording.extendResults(id, zombie.workerId, 1));
        assertTrue(tick.isCancelled());
    }

    /** Releases the zombie's handler and asserts its finish changed neither the row nor its history. */
    private void releaseZombie(Worker zombie, Script script, UUID id) throws InterruptedException {
        Snapshot before = snapshot(id);

        script.release.countDown();
        zombie.joinCleanly();

        assertEquals(before, snapshot(id));
    }

    private Worker start(String label, UUID workerId, int attempt) {
        ManualScheduler scheduler = new ManualScheduler();
        LeaseHeartbeat heartbeat = new LeaseHeartbeat(recording, leaseProperties.getHeartbeatInterval(), scheduler);
        TaskExecutor executor = new TaskExecutor(recording, Map.of("REC", handler), jsonMapper, heartbeat);
        Worker worker = new Worker("recovery-" + label + "-attempt-" + attempt, executor, workerId, scheduler, heartbeat);
        workers.add(worker);
        worker.thread.start();
        return worker;
    }

    private Task assertRow(UUID id, TaskStatus status, int attempt, UUID workerId) {
        Task task = taskService.getTask(id);
        assertEquals(status, task.status(), "status");
        assertEquals(attempt, task.attempt(), "attempt");
        assertEquals(workerId, task.workerId(), "worker");
        return task;
    }

    private void assertCompleted(UUID id, UUID workerId, int attempt) {
        Task completed = assertRow(id, TaskStatus.COMPLETED, attempt, workerId);
        assertNull(completed.error());
        assertEquals(result(attempt, workerId), storedResult(id));
        assertApplied(id, workerId, attempt);
    }

    private void assertApplied(UUID id, UUID workerId, int attempt) {
        List<TransitionResult> results = recording.finishResults(id, workerId, attempt);
        assertEquals(1, results.size(), "finishes for attempt " + attempt);
        assertInstanceOf(TransitionResult.Applied.class, results.getFirst());
    }

    private JsonNode storedResult(UUID id) {
        return jsonMapper.readTree(taskService.getTask(id).result());
    }

    private JsonNode result(int attempt, UUID workerId) {
        return jsonMapper.createObjectNode()
                .put("attempt", attempt)
                .put("worker", workerId.toString());
    }

    private static String reason(int attempt, UUID workerId) {
        return "attempt " + attempt + " of worker " + workerId + " failed";
    }

    /** Polls in autocommit, so the database clock decides when the lease has passed. */
    private void awaitLeaseExpired(UUID id) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!leaseExpired(id)) {
            if (System.nanoTime() >= deadline) {
                fail("lease of task " + id + " did not expire");
            }
            Thread.sleep(50);
        }
    }

    /** Retries a zero: the reap reads {@code NOW()} in a later transaction, and the database clock can step backwards. */
    private int reapAfterExpiry() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        int reaped = reaper.reapOnce();
        while (reaped == 0 && System.nanoTime() < deadline) {
            Thread.sleep(50);
            reaped = reaper.reapOnce();
        }
        return reaped;
    }

    private boolean leaseExpired(UUID id) {
        return Boolean.TRUE.equals(jdbcClient.sql("""
                        SELECT lease_expires_at < clock_timestamp()
                        FROM tasks
                        WHERE id = :id
                        """)
                .param("id", id)
                .query(Boolean.class)
                .single());
    }

    private List<History> history(UUID id) {
        return jdbcClient.sql("""
                        SELECT attempt, worker_id, outcome
                        FROM task_attempts
                        WHERE task_id = :id
                        ORDER BY attempt
                        """)
                .param("id", id)
                .query((rs, rowNum) -> {
                    String outcome = rs.getString("outcome");
                    return new History(
                            rs.getInt("attempt"),
                            rs.getObject("worker_id", UUID.class),
                            outcome == null ? null : AttemptOutcome.valueOf(outcome));
                })
                .list();
    }

    private Snapshot snapshot(UUID id) {
        String task = jdbcClient.sql("SELECT row_to_json(t)::text FROM tasks t WHERE t.id = :id")
                .param("id", id)
                .query(String.class)
                .single();
        List<String> attempts = jdbcClient.sql("""
                        SELECT row_to_json(a)::text
                        FROM task_attempts a
                        WHERE a.task_id = :id
                        ORDER BY a.attempt
                        """)
                .param("id", id)
                .query(String.class)
                .list();
        return new Snapshot(task, attempts);
    }

    private static void await(CountDownLatch latch, String what) throws InterruptedException {
        assertTrue(latch.await(WAIT_SECONDS, TimeUnit.SECONDS), what);
    }

    /** {@code outcome} is null while the attempt is open. */
    private record History(int attempt, UUID workerId, AttemptOutcome outcome) {
    }

    private record Snapshot(String task, List<String> attempts) {
    }

    private record Run(UUID workerId, int attempt) {
    }

    private enum Outcome {
        SUCCEED,
        FAIL
    }

    /** One attempt's script. {@code leaseLostAtReturn} stays null unless the handler returned. */
    private static final class Script {

        private final Outcome outcome;
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicReference<Boolean> leaseLostAtReturn = new AtomicReference<>();

        private Script(Outcome outcome) {
            this.outcome = outcome;
        }
    }

    /**
     * Runs the script for the context's worker and attempt. It blocks until
     * released and throws {@link InterruptedException} on an interrupt.
     */
    private final class RecHandler implements TaskHandler {

        private final Map<Run, Script> scripts = new ConcurrentHashMap<>();

        Script script(UUID workerId, int attempt, Outcome outcome) {
            Script script = new Script(outcome);
            scripts.put(new Run(workerId, attempt), script);
            return script;
        }

        void releaseAll() {
            scripts.values().forEach(script -> script.release.countDown());
        }

        @Override
        public String type() {
            return "REC";
        }

        @Override
        public TaskOutcome execute(TaskContext ctx) throws InterruptedException {
            Script script = scripts.get(new Run(ctx.workerId(), ctx.attempt()));
            if (script == null) {
                throw new IllegalStateException("no script for " + ctx);
            }
            script.started.countDown();
            if (!script.release.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("handler was not released: " + ctx);
            }
            script.leaseLostAtReturn.set(ctx.leaseLost());
            return switch (script.outcome) {
                case SUCCEED -> new TaskOutcome.Succeeded(result(ctx.attempt(), ctx.workerId()));
                case FAIL -> new TaskOutcome.Failed(reason(ctx.attempt(), ctx.workerId()));
            };
        }
    }

    /** One hand-built executor's {@link TaskExecutor#runOnce} on its own named thread. */
    private static final class Worker {

        private final UUID workerId;
        private final ManualScheduler scheduler;
        private final LeaseHeartbeat heartbeat;
        private final Thread thread;
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final AtomicReference<Throwable> thrown = new AtomicReference<>();

        private Worker(String name, TaskExecutor executor, UUID workerId, ManualScheduler scheduler,
                       LeaseHeartbeat heartbeat) {
            this.workerId = workerId;
            this.scheduler = scheduler;
            this.heartbeat = heartbeat;
            this.thread = new Thread(() -> {
                try {
                    claimed.set(executor.runOnce(workerId));
                } catch (Throwable t) {
                    thrown.set(t);
                }
            }, name);
            this.thread.setDaemon(true);
        }

        void joinCleanly() throws InterruptedException {
            thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
            assertFalse(thread.isAlive(), thread.getName() + " did not return");
            if (thrown.get() != null) {
                throw new AssertionError(thread.getName() + " threw", thrown.get());
            }
            assertTrue(claimed.get(), thread.getName() + " claimed nothing");
        }
    }
}
