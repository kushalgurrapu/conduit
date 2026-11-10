package com.kushal.workflow.worker;

import com.kushal.workflow.task.LeaseProperties;
import com.kushal.workflow.task.Task;
import com.kushal.workflow.task.TaskService;
import com.kushal.workflow.task.TaskStatus;
import com.kushal.workflow.task.TransitionResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.context.LifecycleProperties;
import org.springframework.dao.DataAccessResourceFailureException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The heartbeat tick and the executor's watch, without a database. Ticks
 * run only when a test fires them, so every interleaving here is chosen,
 * not timed.
 */
class LeaseHeartbeatTest {

    private static final Duration INTERVAL = Duration.ofMillis(1234);

    private final ManualScheduler scheduler = new ManualScheduler();
    private final ScriptedTaskService taskService = new ScriptedTaskService(scheduler);
    private final LeaseHeartbeat heartbeat = new LeaseHeartbeat(taskService, INTERVAL, scheduler);
    private final UUID taskId = UUID.randomUUID();
    private final UUID workerId = UUID.randomUUID();

    @AfterEach
    void shutDown() {
        Thread.interrupted();
        scheduler.shutdownNow();
    }

    @Test
    void delayAndPeriodAreTheIntervalAndNothingRunsUntilFired() {
        TaskContext context = context(4);

        heartbeat.start(context);

        ManualScheduler.Tick tick = scheduler.onlyTick();
        assertEquals(INTERVAL.toNanos(), tick.initialDelay(TimeUnit.NANOSECONDS));
        assertEquals(INTERVAL.toNanos(), tick.delay(TimeUnit.NANOSECONDS));
        assertFalse(tick.isCancelled());
        assertTrue(taskService.extendCalls.isEmpty());
        assertFalse(context.leaseLost());
    }

    @Test
    void oneFireExtendsOnceForThisWorkerAndAttempt() {
        TaskContext context = context(4);
        heartbeat.start(context);

        scheduler.onlyTick().fire();

        assertEquals(List.of(new Extend(taskId, workerId, 4)), taskService.extendCalls);
        assertFalse(context.leaseLost());
    }

    @Test
    void falseWhileOpenMarksTheLeaseLost() {
        TaskContext context = context(1);
        heartbeat.start(context);
        taskService.script(() -> false);

        scheduler.onlyTick().fire();

        assertEquals(1, taskService.extendCalls.size());
        assertTrue(context.leaseLost());
    }

    @Test
    void closeWatchWhenLeaseLost() {
        TaskContext context = context(1);
        heartbeat.start(context);
        taskService.script(() -> false);

        ManualScheduler.Tick tick = scheduler.onlyTick();
        tick.fire();
        tick.fire();

        assertEquals(1, taskService.extendCalls.size());
        assertTrue(context.leaseLost());
        assertTrue(tick.isCancelled());
        assertEquals(Boolean.FALSE, tick.mayInterruptIfRunning());
    }

    @Test
    void dataAccessErrorIsNotLeaseLossAndTheNextFireStillExtends() {
        TaskContext context = context(1);
        heartbeat.start(context);
        taskService.script(() -> {
            throw new DataAccessResourceFailureException("database down");
        });

        ManualScheduler.Tick tick = scheduler.onlyTick();
        tick.fire();
        assertFalse(context.leaseLost());

        tick.fire();
        assertEquals(2, taskService.extendCalls.size());
        assertFalse(context.leaseLost());
        assertFalse(tick.isCancelled());
    }

    @Test
    void otherRuntimeExceptionIsNotLeaseLossAndTheNextFireStillExtends() {
        TaskContext context = context(1);
        heartbeat.start(context);
        taskService.script(() -> {
            throw new IllegalStateException("unexpected");
        });

        ManualScheduler.Tick tick = scheduler.onlyTick();
        tick.fire();
        assertFalse(context.leaseLost());

        tick.fire();
        assertEquals(2, taskService.extendCalls.size());
        assertFalse(context.leaseLost());
    }

    @Test
    void errorFromTheExtendPropagatesAndIsNotLeaseLoss() {
        TaskContext context = context(1);
        heartbeat.start(context);
        taskService.script(() -> {
            throw new AssertionError("fatal");
        });

        assertThrows(AssertionError.class, () -> scheduler.onlyTick().fire());
        assertFalse(context.leaseLost());
    }

    @Test
    void closedWatchDoesNotExtendAndCancelsWithoutInterrupt() {
        TaskContext context = context(1);
        LeaseHeartbeat.Watch watch = heartbeat.start(context);

        watch.close();
        watch.close();

        ManualScheduler.Tick tick = scheduler.onlyTick();
        assertTrue(tick.isCancelled());
        assertEquals(Boolean.FALSE, tick.mayInterruptIfRunning());
        tick.fire();
        assertTrue(taskService.extendCalls.isEmpty());
        assertFalse(context.leaseLost());
    }

    @Test
    void falseAfterTheWatchClosesDoesNotMarkTheLeaseLost() {
        TaskContext context = context(1);
        AtomicReference<LeaseHeartbeat.Watch> watch = new AtomicReference<>();
        taskService.script(() -> {
            watch.get().close();
            return false;
        });
        watch.set(heartbeat.start(context));

        scheduler.onlyTick().fire();

        assertEquals(1, taskService.extendCalls.size());
        assertFalse(context.leaseLost());
    }

    @Test
    void stoppedHeartbeatReturnsAClosedWatch() {
        heartbeat.start();
        assertTrue(heartbeat.isRunning());
        heartbeat.stop();
        assertFalse(heartbeat.isRunning());
        assertTrue(scheduler.isShutdown());

        LeaseHeartbeat.Watch watch = heartbeat.start(context(1));

        assertFalse(watch.isOpen());
        assertTrue(scheduler.ticks().isEmpty());
        watch.close();
    }

    @Test
    void startsBeforeAndStopsAfterTheWorkerPool() {
        WorkerPool pool = new WorkerPool(null, new WorkerProperties(), null, new LifecycleProperties());

        assertTrue(heartbeat.getPhase() < pool.getPhase());
        assertTrue(heartbeat.isAutoStartup());
    }

    @Test
    void executorContextCarriesTheClaimedAttempt() {
        AtomicReference<TaskContext> seen = new AtomicReference<>();
        TaskExecutor executor = executor(new ScriptedHandler("WORK", ctx -> {
            seen.set(ctx);
            return new TaskOutcome.Succeeded(null);
        }));
        taskService.toClaim(claimed("WORK", 7));

        assertTrue(executor.runOnce(workerId));

        assertEquals(7, seen.get().attempt());
        assertEquals(taskId, seen.get().taskId());
        assertEquals(workerId, seen.get().workerId());
        assertEquals(List.of(new Finish("complete", 7, true)), taskService.finishes);
    }

    @Test
    void interruptFromExecuteClosesTheWatchAndSkipsFinish() {
        TaskExecutor executor = executor(new ScriptedHandler("WORK", ctx -> {
            throw new InterruptedException("shutdown");
        }));
        taskService.toClaim(claimed("WORK", 2));

        assertTrue(executor.runOnce(workerId));
        assertTrue(Thread.interrupted(), "interrupt flag restored");

        ManualScheduler.Tick tick = scheduler.onlyTick();
        assertTrue(tick.isCancelled());
        assertEquals(Boolean.FALSE, tick.mayInterruptIfRunning());
        tick.fire();
        assertTrue(taskService.extendCalls.isEmpty());
        assertTrue(taskService.finishes.isEmpty());
    }

    @Test
    void errorFromExecuteClosesTheWatchAndSkipsFinish() {
        TaskExecutor executor = executor(new ScriptedHandler("WORK", ctx -> {
            throw new AssertionError("handler error");
        }));
        taskService.toClaim(claimed("WORK", 2));

        AssertionError thrown = assertThrows(AssertionError.class, () -> executor.runOnce(workerId));

        assertEquals("handler error", thrown.getMessage());
        ManualScheduler.Tick tick = scheduler.onlyTick();
        assertTrue(tick.isCancelled());
        assertEquals(Boolean.FALSE, tick.mayInterruptIfRunning());
        tick.fire();
        assertTrue(taskService.extendCalls.isEmpty());
        assertTrue(taskService.finishes.isEmpty());
    }

    @Test
    void returnedOutcomeIsFinishedOnceEvenWhenTheLeaseWasLost() {
        taskService.script(() -> false);
        TaskExecutor executor = executor(new ScriptedHandler("WORK", ctx -> {
            scheduler.onlyTick().fire();
            assertTrue(ctx.leaseLost());
            return new TaskOutcome.Succeeded(null);
        }));
        taskService.toClaim(claimed("WORK", 3));

        assertTrue(executor.runOnce(workerId));

        assertEquals(1, taskService.extendCalls.size());
        assertEquals(List.of(new Finish("complete", 3, true)), taskService.finishes);
    }

    @Test
    void thrownExceptionClosesTheWatchBeforeTheFailWrite() {
        TaskExecutor executor = executor(new ScriptedHandler("WORK", ctx -> {
            throw new IllegalStateException("boom");
        }));
        taskService.toClaim(claimed("WORK", 5));

        assertTrue(executor.runOnce(workerId));

        assertEquals(List.of(new Finish("fail", 5, true)), taskService.finishes);
        scheduler.onlyTick().fire();
        assertTrue(taskService.extendCalls.isEmpty());
    }

    @Test
    void missingHandlerStartsNoWatch() {
        TaskExecutor executor = executor(new ScriptedHandler("WORK", ctx -> new TaskOutcome.Succeeded(null)));
        taskService.toClaim(claimed("OTHER", 1));

        assertTrue(executor.runOnce(workerId));

        assertTrue(scheduler.ticks().isEmpty());
        assertEquals(List.of(new Finish("fail", 1, true)), taskService.finishes);
    }

    private TaskExecutor executor(ScriptedHandler handler) {
        return new TaskExecutor(taskService, Map.of(handler.type(), handler), JsonMapper.shared(), heartbeat);
    }

    private TaskContext context(int attempt) {
        return new TaskContext(taskId, "WORK", null, workerId, attempt);
    }

    private Task claimed(String type, int attempt) {
        Instant now = Instant.now();
        return new Task(
                taskId, type, TaskStatus.RUNNING, "{}", null, null,
                now, now, now, null, workerId, attempt, now.plusSeconds(30), null);
    }

    private record Extend(UUID taskId, UUID workerId, int attempt) {
    }

    /** {@code watchClosed} is whether every scheduled tick was cancelled when the write ran. */
    private record Finish(String kind, int attempt, boolean watchClosed) {
    }

    @FunctionalInterface
    private interface ExtendStep {
        boolean extend();
    }

    @FunctionalInterface
    private interface HandlerStep {
        TaskOutcome execute(TaskContext ctx) throws Exception;
    }

    /**
     * Records calls instead of reaching a database. Extends return
     * {@code true} unless a test scripts something else.
     */
    private static final class ScriptedTaskService extends TaskService {

        private final ManualScheduler scheduler;
        private final List<Extend> extendCalls = new CopyOnWriteArrayList<>();
        private final List<Finish> finishes = new CopyOnWriteArrayList<>();
        private final Queue<Task> claims = new ArrayDeque<>();
        private volatile ExtendStep step = () -> true;

        private ScriptedTaskService(ManualScheduler scheduler) {
            super(null, new LeaseProperties());
            this.scheduler = scheduler;
        }

        void script(ExtendStep step) {
            this.step = step;
        }

        void toClaim(Task task) {
            claims.add(task);
        }

        @Override
        public Task claimTask(UUID workerId) {
            return claims.poll();
        }

        @Override
        public boolean extendLease(UUID taskId, UUID workerId, int attempt) {
            extendCalls.add(new Extend(taskId, workerId, attempt));
            return step.extend();
        }

        @Override
        public TransitionResult completeTask(UUID taskId, UUID workerId, int attempt, String result) {
            finishes.add(new Finish("complete", attempt, watchClosed()));
            return new TransitionResult.Rejected(TaskStatus.RUNNING, workerId);
        }

        @Override
        public TransitionResult failTask(UUID taskId, UUID workerId, int attempt, String error) {
            finishes.add(new Finish("fail", attempt, watchClosed()));
            return new TransitionResult.Rejected(TaskStatus.RUNNING, workerId);
        }

        private boolean watchClosed() {
            return scheduler.ticks().stream().allMatch(ManualScheduler.Tick::isCancelled);
        }
    }

    private static final class ScriptedHandler implements TaskHandler {

        private final String type;
        private final HandlerStep action;

        private ScriptedHandler(String type, HandlerStep action) {
            this.type = type;
            this.action = action;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public TaskOutcome execute(TaskContext ctx) throws Exception {
            return action.execute(ctx);
        }
    }
}
