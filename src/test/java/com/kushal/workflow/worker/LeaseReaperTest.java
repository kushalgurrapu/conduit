package com.kushal.workflow.worker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import com.kushal.workflow.task.LeaseProperties;
import com.kushal.workflow.task.ReapedTask;
import com.kushal.workflow.task.TaskService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.context.LifecycleProperties;
import org.springframework.dao.DataAccessResourceFailureException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The reaper's tick, loop, and lifecycle, without a database. Ticks run
 * only when a test fires them, on the test thread, so a rethrown error
 * reaches the test.
 */
class LeaseReaperTest {

    private static final Duration INTERVAL = Duration.ofMillis(1234);
    private static final String STOPPED_UNTIL_RESTART = "lease reaping has stopped in this process until restart";

    private final ManualScheduler scheduler = new ManualScheduler();
    private final ScriptedTaskService taskService = new ScriptedTaskService();
    private final LeaseReaper reaper = new LeaseReaper(taskService, properties(true, 100), scheduler);
    private final Logger reaperLogger = (Logger) LoggerFactory.getLogger(LeaseReaper.class);
    private final Level originalLevel = reaperLogger.getLevel();
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();

    @BeforeEach
    void captureLogs() {
        logs.start();
        reaperLogger.addAppender(logs);
    }

    @AfterEach
    void shutDown() {
        reaperLogger.detachAppender(logs);
        reaperLogger.setLevel(originalLevel);
        Thread.interrupted();
        scheduler.shutdownNow();
    }

    @Test
    void delayAndPeriodAreTheIntervalAndNothingRunsUntilFired() {
        reaper.start();

        assertTrue(reaper.isRunning());
        ManualScheduler.Tick tick = scheduler.onlyTick();
        assertEquals(INTERVAL.toNanos(), tick.initialDelay(TimeUnit.NANOSECONDS));
        assertEquals(INTERVAL.toNanos(), tick.delay(TimeUnit.NANOSECONDS));
        assertTrue(taskService.batchSizes.isEmpty());
    }

    @Test
    void fullBatchesRepeatUntilAShortOne() {
        taskService.thenBatches(100, 100, 3);

        assertEquals(203, reaper.reapOnce());

        assertEquals(List.of(100, 100, 100), taskService.batchSizes);
    }

    @Test
    void oneFireIsOneReapOnce() {
        reaper.start();
        taskService.thenBatches(100, 0, 100);

        scheduler.onlyTick().fire();

        assertEquals(2, taskService.batchSizes.size());
    }

    @Test
    void exceptionIsLoggedAndTheNextFireReapsAgain() {
        reaper.start();
        taskService.then(() -> {
            throw new DataAccessResourceFailureException("database down");
        });

        ManualScheduler.Tick tick = scheduler.onlyTick();
        tick.fire();
        tick.fire();

        assertEquals(2, taskService.batchSizes.size());
        assertFalse(tick.isCancelled());
        assertTrue(reaper.isRunning());
        assertEquals(1, events(Level.WARN).size());
    }

    @Test
    void errorThatIsNotAVirtualMachineErrorIsLoggedAndTheNextFireReapsAgain() {
        reaper.start();
        AssertionError error = new AssertionError("reap error");
        taskService.then(() -> {
            throw error;
        });

        ManualScheduler.Tick tick = scheduler.onlyTick();
        tick.fire();
        tick.fire();

        assertEquals(2, taskService.batchSizes.size());
        assertTrue(reaper.isRunning());
        List<ILoggingEvent> errors = events(Level.ERROR);
        assertEquals(1, errors.size());
        assertFalse(errors.getFirst().getFormattedMessage().contains(STOPPED_UNTIL_RESTART));
        assertSame(error, thrownBy(errors.getFirst()));
    }

    @Test
    void virtualMachineErrorIsLoggedThenRethrown() {
        reaper.start();
        OutOfMemoryError error = new OutOfMemoryError("test");
        taskService.then(() -> {
            throw error;
        });

        OutOfMemoryError thrown = assertThrows(OutOfMemoryError.class, () -> scheduler.onlyTick().fire());

        assertSame(error, thrown);
        List<ILoggingEvent> errors = events(Level.ERROR);
        assertEquals(1, errors.size());
        assertTrue(errors.getFirst().getFormattedMessage().contains(STOPPED_UNTIL_RESTART));
        assertSame(error, thrownBy(errors.getFirst()));
    }

    @Test
    void disabledReaperSchedulesNothing() {
        LeaseReaper disabled = new LeaseReaper(taskService, properties(false, 100), scheduler);

        disabled.start();

        assertFalse(disabled.isRunning());
        assertTrue(scheduler.ticks().isEmpty());
    }

    @Test
    void publicConstructorValidatesEvenWhenDisabled() {
        ReaperProperties badBatch = properties(false, 0);
        ReaperProperties badInterval = properties(false, 100);
        badInterval.setInterval(Duration.ofMillis(99));

        assertThrows(IllegalStateException.class, () -> new LeaseReaper(taskService, badBatch));
        assertThrows(IllegalStateException.class, () -> new LeaseReaper(taskService, badInterval));
    }

    @Test
    void stopDuringABatchEndsTheLoopAfterThatBatch() {
        taskService.then(() -> {
            reaper.stop();
            return batch(100);
        });
        taskService.thenBatches(100);

        assertEquals(100, reaper.reapOnce());

        assertEquals(1, taskService.batchSizes.size());
        assertFalse(reaper.isRunning());
    }

    @Test
    void interruptEndsTheLoopAfterTheCurrentBatch() {
        taskService.then(() -> {
            Thread.currentThread().interrupt();
            return batch(100);
        });
        taskService.thenBatches(100);

        assertEquals(100, reaper.reapOnce());
        assertEquals(1, taskService.batchSizes.size());

        assertEquals(0, reaper.reapOnce());
        assertEquals(1, taskService.batchSizes.size());
        assertTrue(Thread.interrupted());
    }

    @Test
    void reapOnceRunsOnABeanThatWasNeverStarted() {
        taskService.thenBatches(3);

        assertEquals(3, reaper.reapOnce());

        assertFalse(reaper.isRunning());
        assertTrue(scheduler.ticks().isEmpty());
    }

    @Test
    void startTwiceSchedulesOnce() {
        reaper.start();
        reaper.start();

        assertEquals(1, scheduler.ticks().size());
        assertTrue(reaper.isRunning());
    }

    @Test
    void startAfterStopDoesNotThrowAndLeavesItStopped() {
        reaper.start();
        reaper.stop();
        assertFalse(reaper.isRunning());
        assertTrue(scheduler.isShutdown());

        reaper.start();

        assertFalse(reaper.isRunning());
        assertEquals(1, scheduler.ticks().size());
        assertEquals(1, events(Level.WARN).size());
    }

    @Test
    void startAfterAStopThatCameFirstLeavesItStopped() {
        reaper.stop();

        reaper.start();

        assertFalse(reaper.isRunning());
        assertTrue(scheduler.ticks().isEmpty());
    }

    @Test
    void reaperIsPauseable() {
        assertTrue(reaper.isPauseable());
    }

    @Test
    void startsAfterTheHeartbeatAndBeforeTheWorkerPool() {
        LeaseHeartbeat heartbeat = new LeaseHeartbeat(taskService, INTERVAL, scheduler);
        WorkerPool pool = new WorkerPool(null, new WorkerProperties(), null, new LifecycleProperties());

        assertEquals(0, reaper.getPhase());
        assertTrue(heartbeat.getPhase() < reaper.getPhase());
        assertTrue(reaper.getPhase() < pool.getPhase());
        assertTrue(reaper.isAutoStartup());
    }

    @Test
    void springUsesThePublicConstructor() throws Exception {
        Constructor<LeaseReaper> production = LeaseReaper.class.getConstructor(TaskService.class, ReaperProperties.class);
        Constructor<LeaseReaper> forTests = LeaseReaper.class.getDeclaredConstructor(
                TaskService.class, ReaperProperties.class, ScheduledExecutorService.class);

        assertTrue(production.isAnnotationPresent(Autowired.class));
        assertFalse(forTests.isAnnotationPresent(Autowired.class));
        assertFalse(Modifier.isPublic(forTests.getModifiers()));
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void publicConstructorRunsOneDaemonThreadNamedLeaseReaper() throws Exception {
        ReaperProperties properties = properties(true, 100);
        properties.setInterval(Duration.ofHours(1));
        Set<Thread> before = reaperThreads();
        LeaseReaper real = new LeaseReaper(taskService, properties);
        List<Thread> started = new ArrayList<>();
        try {
            real.start();
            started.addAll(reaperThreads());
            started.removeAll(before);

            assertEquals(1, started.size(), "new lease-reaper threads");
            assertTrue(started.getFirst().isDaemon());
        } finally {
            real.stop();
        }
        for (Thread thread : started) {
            thread.join(TimeUnit.SECONDS.toMillis(5));
            assertFalse(thread.isAlive(), "lease-reaper thread still alive after stop");
        }
        assertTrue(taskService.batchSizes.isEmpty());
    }

    @Test
    void logsOneInfoPerNonEmptyBatchOneDebugPerTaskAndAWarnPerUnrecordedAttempt() {
        reaperLogger.setLevel(Level.DEBUG);
        LeaseReaper small = new LeaseReaper(taskService, properties(true, 2), scheduler);
        taskService.thenBatches(2);
        taskService.then(() -> List.of(new ReapedTask(UUID.randomUUID(), 3, UUID.randomUUID(), false)));

        assertEquals(3, small.reapOnce());

        assertEquals(2, events(Level.INFO).size());
        assertEquals(3, events(Level.DEBUG).size());
        assertEquals(1, events(Level.WARN).size());
    }

    @Test
    void emptyReapLogsNothing() {
        assertEquals(0, reaper.reapOnce());

        assertEquals(1, taskService.batchSizes.size());
        assertTrue(logs.list.isEmpty());
    }

    private List<ILoggingEvent> events(Level level) {
        return logs.list.stream().filter(event -> event.getLevel() == level).toList();
    }

    private static Throwable thrownBy(ILoggingEvent event) {
        return ((ThrowableProxy) event.getThrowableProxy()).getThrowable();
    }

    private static Set<Thread> reaperThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().equals("lease-reaper") && thread.isAlive())
                .collect(Collectors.toSet());
    }

    private static ReaperProperties properties(boolean enabled, int batchSize) {
        ReaperProperties properties = new ReaperProperties();
        properties.setEnabled(enabled);
        properties.setInterval(INTERVAL);
        properties.setBatchSize(batchSize);
        return properties;
    }

    private static List<ReapedTask> batch(int size) {
        List<ReapedTask> batch = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            batch.add(new ReapedTask(UUID.randomUUID(), 1, UUID.randomUUID(), true));
        }
        return batch;
    }

    @FunctionalInterface
    private interface ReapStep {
        List<ReapedTask> reap();
    }

    /** Returns scripted batches instead of reaching a database. Unscripted calls return no rows. */
    private static final class ScriptedTaskService extends TaskService {

        private final List<Integer> batchSizes = new CopyOnWriteArrayList<>();
        private final Queue<ReapStep> steps = new ConcurrentLinkedQueue<>();

        private ScriptedTaskService() {
            super(null, null, new LeaseProperties());
        }

        void then(ReapStep step) {
            steps.add(step);
        }

        void thenBatches(int... sizes) {
            for (int size : sizes) {
                steps.add(() -> batch(size));
            }
        }

        @Override
        public List<ReapedTask> reapExpiredLeases(int batchSize) {
            batchSizes.add(batchSize);
            ReapStep step = steps.poll();
            return step == null ? List.of() : step.reap();
        }
    }
}
