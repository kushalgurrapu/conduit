package com.kushal.workflow.worker;

import com.kushal.workflow.support.PostgresIntegrationTest;
import com.kushal.workflow.task.Task;
import com.kushal.workflow.task.TaskRepository;
import com.kushal.workflow.task.TaskService;
import com.kushal.workflow.task.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.context.LifecycleProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The shared application pool stays stopped
 * ({@code engine.worker.enabled=false}). Each test builds its own pool
 * and {@link WorkerPool#stop()} is called afterwards.
 */
@SpringBootTest
@Timeout(value = 45, unit = TimeUnit.SECONDS)
class WorkerPoolTest extends PostgresIntegrationTest {

    private static final long WAIT_SECONDS = 10;

    private static final int DRAIN_CONCURRENCY = 4;

    @Autowired
    private WorkerPool applicationPool;

    @Autowired
    private TaskExecutor taskExecutor;

    @Autowired
    private TaskService taskService;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private CountHandler countHandler;

    @Autowired
    private LatchHandler latchHandler;

    private WorkerPool pool;

    @BeforeEach
    void resetHandlers() {
        assertFalse(applicationPool.isRunning(), "the shared worker pool must stay stopped");
        countHandler.reset();
        latchHandler.reset();
        taskExecutor.setAfterClaim(null);
    }

    @AfterEach
    void stopPool() {
        taskExecutor.setAfterClaim(null);
        latchHandler.release();
        if (pool != null) {
            pool.stop();
            pool = null;
        }
    }

    @Test
    void fourLoopsDrainFiftyTasksOnce() throws Exception {
        List<UUID> counted = new ArrayList<>();
        for (int i = 0; i < 47; i++) {
            counted.add(taskService.createTask("COUNT", "{}").id());
        }
        UUID noop = taskService.createTask("NOOP", "{}").id();
        UUID echo = taskService.createTask("ECHO", "{\"message\":\"hi\"}").id();
        UUID fail = taskService.createTask("FAIL", "{\"reason\":\"nope\"}").id();
        List<UUID> all = new ArrayList<>(counted);
        all.add(noop);
        all.add(echo);
        all.add(fail);

        pool = newPool(DRAIN_CONCURRENCY, Duration.ofMillis(50), Duration.ofSeconds(10), Duration.ofMillis(50));
        pool.start();
        awaitTrue(() -> all.stream().allMatch(id -> require(id).status().isTerminal()), "tasks did not finish");

        assertEquals(47, countHandler.totalExecutions());
        for (UUID id : counted) {
            assertEquals(1, countHandler.executions(id), id.toString());
            assertEquals(TaskStatus.COMPLETED, require(id).status());
        }
        assertEquals(TaskStatus.COMPLETED, require(noop).status());
        assertTrue(jsonNullResult(noop));
        Task echoed = require(echo);
        assertEquals(TaskStatus.COMPLETED, echoed.status());
        assertTrue(resultEqualsPayload(echo));
        Task failed = require(fail);
        assertEquals(TaskStatus.FAILED, failed.status());
        assertEquals("nope", failed.error());
    }

    @Test
    void inFlightHandlersDoNotExceedConcurrency() throws Exception {
        int concurrency = DRAIN_CONCURRENCY;
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < concurrency * 3; i++) {
            ids.add(taskService.createTask("LATCH", "{}").id());
        }
        pool = newPool(concurrency, Duration.ofHours(1), Duration.ofSeconds(10), Duration.ofSeconds(1));
        pool.start();
        try {
            awaitTrue(() -> latchHandler.started() >= concurrency, "handlers did not fill the pool");
            Thread.sleep(200);
            assertEquals(concurrency, latchHandler.started(), "a free loop claimed another task");
            assertEquals(concurrency, latchHandler.inFlight());
            assertTrue(latchHandler.highWater() <= concurrency);
            assertFalse(latchHandler.sawVirtual());
            assertEquals(concurrency, latchHandler.threadNames().size());
            for (String name : latchHandler.threadNames()) {
                assertTrue(name.matches(".+-\\d+-loop\\d+"), name);
            }
        } finally {
            latchHandler.release();
        }

        awaitTrue(() -> ids.stream().allMatch(id -> require(id).status().isTerminal()), "latched tasks did not finish");
        assertTrue(latchHandler.highWater() <= concurrency);
        for (UUID id : ids) {
            assertEquals(TaskStatus.COMPLETED, require(id).status());
        }
    }

    @Test
    void databaseErrorsDoNotKillTheLoop() throws Exception {
        FlakyExecutor flaky = new FlakyExecutor(5);
        WorkerProperties properties = properties(
                1, Duration.ofMillis(20), Duration.ofSeconds(2), Duration.ofMillis(20));
        pool = new WorkerPool(flaky, properties, taskRepository, new LifecycleProperties());
        pool.start();
        awaitTrue(() -> flaky.calls() >= 8, "the loop stopped after database errors");
        assertTrue(pool.isRunning());
    }

    @Test
    void inFlightTaskFinishesDuringShutdownAndNothingNewIsClaimed() throws Exception {
        Task first = taskService.createTask("LATCH", "{}");
        pool = newPool(1, Duration.ofHours(1), Duration.ofSeconds(10), Duration.ofSeconds(1));
        pool.start();
        awaitTrue(() -> latchHandler.started() == 1, "the in-flight handler did not start");
        Task second = taskService.createTask("NOOP", "{}");

        Thread stopper = new Thread(pool::stop, "pool-stop");
        stopper.start();
        try {
            awaitTrue(pool::isStopping, "stop did not begin");
            latchHandler.release();
            stopper.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
            assertFalse(stopper.isAlive(), "stop did not return");
        } finally {
            latchHandler.release();
        }

        Task finished = require(first.id());
        assertEquals(TaskStatus.COMPLETED, finished.status());
        assertNotNull(finished.workerId());
        Task left = require(second.id());
        assertEquals(TaskStatus.PENDING, left.status());
        assertNull(left.workerId());
        assertNull(left.claimedAt());
    }

    @Test
    void taskStillRunningAtShutdownTimeoutStaysRunning() throws Exception {
        Task created = taskService.createTask("LATCH", "{}");
        pool = newPool(1, Duration.ofHours(1), Duration.ofMillis(400), Duration.ofSeconds(1));
        pool.start();
        awaitTrue(() -> latchHandler.started() == 1, "the handler did not start");
        Task running = require(created.id());
        assertEquals(TaskStatus.RUNNING, running.status());
        UUID workerId = running.workerId();

        pool.stop();
        awaitTrue(() -> latchHandler.inFlight() == 0, "the handler did not leave after the interrupt");

        Task after = require(created.id());
        assertEquals(TaskStatus.RUNNING, after.status());
        assertEquals(workerId, after.workerId());
        assertNull(after.finishedAt());
        assertNull(after.error());
        assertEquals(List.of(created.id()), taskRepository.findRunningIdsForWorkers(List.of(workerId)));
    }

    @Test
    void stopAfterClaimStillRunsTheHandler() throws Exception {
        Task created = taskService.createTask("NOOP", "{}");
        CountDownLatch claimed = new CountDownLatch(1);
        pool = newPool(1, Duration.ofHours(1), Duration.ofSeconds(10), Duration.ofSeconds(1));
        taskExecutor.setAfterClaim(() -> {
            claimed.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
            while (!pool.isStopping()) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("pool did not start stopping");
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                    throw new IllegalStateException("interrupted before the handler", ex);
                }
            }
        });
        pool.start();
        assertTrue(claimed.await(WAIT_SECONDS, TimeUnit.SECONDS), "the task was not claimed");
        Task running = require(created.id());
        assertEquals(TaskStatus.RUNNING, running.status());

        pool.stop();

        Task finished = require(created.id());
        assertEquals(TaskStatus.COMPLETED, finished.status());
        assertEquals(running.workerId(), finished.workerId());
        assertNotNull(finished.finishedAt());
    }

    @Test
    void crashedClaimStaysRunningWhileALaterTaskCompletes() throws Exception {
        UUID crashedWorker = UUID.randomUUID();
        Task stuck = taskService.createTask("NOOP", "{}");
        Task claimed = taskService.claimTask(crashedWorker);
        assertEquals(stuck.id(), claimed.id());
        assertEquals(TaskStatus.RUNNING, claimed.status());
        Task sentinel = taskService.createTask("NOOP", "{}");

        pool = newPool(2, Duration.ofMillis(20), Duration.ofSeconds(5), Duration.ofMillis(20));
        pool.start();
        awaitTrue(
                () -> require(sentinel.id()).status() == TaskStatus.COMPLETED,
                "the sentinel was not completed");

        Task abandoned = require(stuck.id());
        assertEquals(TaskStatus.RUNNING, abandoned.status());
        assertEquals(crashedWorker, abandoned.workerId());
        assertNull(abandoned.finishedAt());
        assertNull(abandoned.error());
        assertNotEquals(crashedWorker, require(sentinel.id()).workerId());
    }

    private WorkerPool newPool(
            int concurrency, Duration pollInterval, Duration shutdownTimeout, Duration errorBackoff) {
        return new WorkerPool(
                taskExecutor,
                properties(concurrency, pollInterval, shutdownTimeout, errorBackoff),
                taskRepository,
                new LifecycleProperties());
    }

    private static WorkerProperties properties(
            int concurrency, Duration pollInterval, Duration shutdownTimeout, Duration errorBackoff) {
        WorkerProperties properties = new WorkerProperties();
        properties.setEnabled(true);
        properties.setConcurrency(concurrency);
        properties.setPollInterval(pollInterval);
        properties.setShutdownTimeout(shutdownTimeout);
        properties.setErrorBackoff(errorBackoff);
        return properties;
    }

    private void awaitTrue(BooleanSupplier condition, String message) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                fail(message);
            }
            Thread.sleep(10);
        }
    }

    private Task require(UUID id) {
        return taskRepository.findById(id).orElseThrow();
    }

    private boolean resultEqualsPayload(UUID id) {
        return jdbcClient.sql("SELECT result = payload FROM tasks WHERE id = :id")
                .param("id", id)
                .query(Boolean.class)
                .single();
    }

    private boolean jsonNullResult(UUID id) {
        return jdbcClient.sql("SELECT result = 'null'::jsonb FROM tasks WHERE id = :id")
                .param("id", id)
                .query(Boolean.class)
                .single();
    }

    @TestConfiguration
    static class PoolHandlerConfig {

        @Bean
        CountHandler countHandler() {
            return new CountHandler();
        }

        @Bean
        LatchHandler latchHandler() {
            return new LatchHandler();
        }
    }

    static final class CountHandler implements TaskHandler {

        private final ConcurrentHashMap<UUID, AtomicInteger> executions = new ConcurrentHashMap<>();

        void reset() {
            executions.clear();
        }

        int executions(UUID id) {
            AtomicInteger count = executions.get(id);
            return count == null ? 0 : count.get();
        }

        int totalExecutions() {
            return executions.values().stream().mapToInt(AtomicInteger::get).sum();
        }

        @Override
        public String type() {
            return "COUNT";
        }

        @Override
        public TaskOutcome execute(TaskContext ctx) {
            executions.computeIfAbsent(ctx.taskId(), id -> new AtomicInteger()).incrementAndGet();
            return new TaskOutcome.Succeeded(null);
        }
    }

    static final class LatchHandler implements TaskHandler {

        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger highWater = new AtomicInteger();
        private final AtomicInteger started = new AtomicInteger();
        private final AtomicBoolean sawVirtual = new AtomicBoolean();
        private final ConcurrentHashMap<String, Boolean> names = new ConcurrentHashMap<>();
        private volatile CountDownLatch gate = new CountDownLatch(1);

        void reset() {
            inFlight.set(0);
            highWater.set(0);
            started.set(0);
            sawVirtual.set(false);
            names.clear();
            gate = new CountDownLatch(1);
        }

        void release() {
            gate.countDown();
        }

        int started() {
            return started.get();
        }

        int inFlight() {
            return inFlight.get();
        }

        int highWater() {
            return highWater.get();
        }

        boolean sawVirtual() {
            return sawVirtual.get();
        }

        Set<String> threadNames() {
            return names.keySet();
        }

        @Override
        public String type() {
            return "LATCH";
        }

        @Override
        public TaskOutcome execute(TaskContext ctx) throws InterruptedException {
            if (Thread.currentThread().isVirtual()) {
                sawVirtual.set(true);
            }
            names.put(Thread.currentThread().getName(), Boolean.TRUE);
            int now = inFlight.incrementAndGet();
            highWater.accumulateAndGet(now, Math::max);
            started.incrementAndGet();
            try {
                gate.await();
                return new TaskOutcome.Succeeded(null);
            } finally {
                inFlight.decrementAndGet();
            }
        }
    }

    /**
     * Throws {@link DataAccessResourceFailureException} for the first few
     * calls, then reports an empty queue. Not a Spring bean.
     */
    static final class FlakyExecutor extends TaskExecutor {

        private final AtomicInteger calls = new AtomicInteger();
        private final int failures;

        private FlakyExecutor(int failures) {
            super(null, Map.of(), null);
            this.failures = failures;
        }

        int calls() {
            return calls.get();
        }

        @Override
        public boolean runOnce(UUID workerId) {
            int n = calls.incrementAndGet();
            if (n <= failures) {
                throw new DataAccessResourceFailureException("down");
            }
            return false;
        }
    }
}
