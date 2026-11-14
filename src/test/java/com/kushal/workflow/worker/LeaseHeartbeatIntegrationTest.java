package com.kushal.workflow.worker;

import com.kushal.workflow.support.PostgresIntegrationTest;
import com.kushal.workflow.task.LeaseProperties;
import com.kushal.workflow.task.Task;
import com.kushal.workflow.task.TaskRepository;
import com.kushal.workflow.task.TaskService;
import com.kushal.workflow.task.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Heartbeats against PostgreSQL with a one-second lease. The short lease
 * is this class's own context; the shared context keeps the 30s default
 * so {@code handlerHoldsNoConnection} samples before the first tick.
 *
 * <p>Every sample is an autocommit statement that reads
 * {@code clock_timestamp()}, so the database clock decides when a lease
 * has passed. No test requires every tick to land.
 */
@SpringBootTest
@TestPropertySource(properties = {
        "engine.lease.duration=1s",
        "engine.lease.heartbeat-interval=300ms"
})
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class LeaseHeartbeatIntegrationTest extends PostgresIntegrationTest {

    private static final long WAIT_SECONDS = 20;

    @Autowired
    private TaskExecutor executor;

    @Autowired
    private TaskService taskService;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private LeaseProperties leaseProperties;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private JsonMapper jsonMapper;

    @Autowired
    @Qualifier("taskHandlers")
    private Map<String, TaskHandler> taskHandlers;

    @Autowired
    private HoldHandler hold;

    @AfterEach
    void releaseHandler() {
        hold.release();
    }

    @Test
    void heartbeatsKeepATaskRunningForThreeLeaseDurations() throws Exception {
        assertEquals(1_000_000L, leaseProperties.leaseMicros());
        hold.reset(false);
        UUID workerId = UUID.randomUUID();
        Task created = taskService.createTask("HOLD", "{}");

        Worker worker = Worker.start(executor, workerId);
        try {
            assertTrue(hold.started.await(WAIT_SECONDS, TimeUnit.SECONDS), "handler did not start");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
            while (true) {
                Sample sample = sample(created.id(), 3);
                assertEquals(TaskStatus.RUNNING, sample.status());
                assertEquals(workerId, sample.workerId());
                assertEquals(1, sample.attempt());
                if (sample.durationsPassed() && sample.leaseMoved()) {
                    break;
                }
                if (System.nanoTime() >= deadline) {
                    fail("lease did not move past three durations: " + sample);
                }
                Thread.sleep(50);
            }
        } finally {
            hold.release();
            worker.join();
        }

        worker.assertClaimedWithoutThrowing();
        Task finished = require(created.id());
        assertEquals(TaskStatus.COMPLETED, finished.status());
        assertEquals(workerId, finished.workerId());
        assertEquals(1, finished.attempt());
        assertNotNull(finished.lastHeartbeatAt());
        assertEquals(1, hold.executions.get());
        assertEquals(Boolean.FALSE, hold.leaseLostAtReturn.get());
    }

    /** The task stays {@code RUNNING} after expiry only because the reaper is off in this context. */
    @Test
    void withoutHeartbeatsTheLeaseExpiresAndTheTaskStaysRunning() throws Exception {
        hold.reset(false);
        ManualScheduler neverFires = new ManualScheduler();
        TaskExecutor silent = new TaskExecutor(
                taskService,
                taskHandlers,
                jsonMapper,
                new LeaseHeartbeat(taskService, leaseProperties.getHeartbeatInterval(), neverFires));
        UUID workerId = UUID.randomUUID();
        Task created = taskService.createTask("HOLD", "{}");

        Worker worker = Worker.start(silent, workerId);
        try {
            assertTrue(hold.started.await(WAIT_SECONDS, TimeUnit.SECONDS), "handler did not start");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
            while (true) {
                Sample sample = sample(created.id(), 0);
                assertEquals(TaskStatus.RUNNING, sample.status());
                assertEquals(workerId, sample.workerId());
                if (sample.leaseExpired()) {
                    break;
                }
                if (System.nanoTime() >= deadline) {
                    fail("lease did not expire: " + sample);
                }
                Thread.sleep(50);
            }

            Task expired = require(created.id());
            assertEquals(TaskStatus.RUNNING, expired.status());
            assertEquals(workerId, expired.workerId());
            assertEquals(1, expired.attempt());
            assertNull(expired.lastHeartbeatAt());
            assertNull(expired.finishedAt());
        } finally {
            hold.release();
            worker.join();
            neverFires.shutdownNow();
        }

        worker.assertClaimedWithoutThrowing();
        assertTrue(neverFires.onlyTick().isCancelled());
        Task finished = require(created.id());
        assertEquals(TaskStatus.COMPLETED, finished.status());
        assertEquals(workerId, finished.workerId());
    }

    @Test
    void handlerThatIgnoresLeaseLossStillFinishesAndTheWriteMatchesNothing() throws Exception {
        hold.reset(true);
        UUID workerId = UUID.randomUUID();
        Task created = taskService.createTask("HOLD", "{}");

        Worker worker = Worker.start(executor, workerId);
        Task beforeFinish;
        try {
            assertTrue(hold.started.await(WAIT_SECONDS, TimeUnit.SECONDS), "handler did not start");
            Integer bumped = jdbcClient.sql("""
                    UPDATE tasks
                    SET attempt = attempt + 1
                    WHERE id = :id AND status = 'RUNNING'
                    RETURNING attempt
                    """)
                    .param("id", created.id())
                    .query(Integer.class)
                    .single();
            assertEquals(2, bumped);
            assertTrue(hold.sawLeaseLost.await(WAIT_SECONDS, TimeUnit.SECONDS), "handler did not see lease loss");
            beforeFinish = require(created.id());
        } finally {
            hold.release();
            worker.join();
        }

        worker.assertClaimedWithoutThrowing();
        assertEquals(1, hold.executions.get());
        assertEquals(Boolean.TRUE, hold.leaseLostAtReturn.get());
        Task after = require(created.id());
        assertEquals(beforeFinish, after);
        assertEquals(TaskStatus.RUNNING, after.status());
        assertEquals(workerId, after.workerId());
        assertEquals(2, after.attempt());
        assertNull(after.finishedAt());
        assertNull(after.result());
    }

    /**
     * One autocommit read. {@code durations} is how many lease durations
     * after {@code claimed_at} count as passed.
     */
    private Sample sample(UUID id, int durations) {
        long leaseMicros = leaseProperties.leaseMicros();
        return jdbcClient.sql("""
                SELECT status,
                       worker_id,
                       attempt,
                       clock_timestamp() >= claimed_at + (:passedMicros * INTERVAL '1 microsecond') AS passed,
                       lease_expires_at > claimed_at + (:leaseMicros * INTERVAL '1 microsecond') AS moved,
                       lease_expires_at < clock_timestamp() AS expired
                FROM tasks
                WHERE id = :id
                """)
                .param("passedMicros", durations * leaseMicros)
                .param("leaseMicros", leaseMicros)
                .param("id", id)
                .query((rs, rowNum) -> new Sample(
                        TaskStatus.valueOf(rs.getString("status")),
                        rs.getObject("worker_id", UUID.class),
                        rs.getInt("attempt"),
                        rs.getBoolean("passed"),
                        rs.getBoolean("moved"),
                        rs.getBoolean("expired")))
                .single();
    }

    private Task require(UUID id) {
        return taskRepository.findById(id).orElseThrow();
    }

    private record Sample(
            TaskStatus status,
            UUID workerId,
            int attempt,
            boolean durationsPassed,
            boolean leaseMoved,
            boolean leaseExpired) {
    }

    /** {@link TaskExecutor#runOnce} on its own thread, the way a worker loop calls it. */
    private static final class Worker {

        private final Thread thread;
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final AtomicReference<Throwable> thrown = new AtomicReference<>();

        private Worker(TaskExecutor executor, UUID workerId) {
            this.thread = new Thread(() -> {
                try {
                    claimed.set(executor.runOnce(workerId));
                } catch (Throwable t) {
                    thrown.set(t);
                }
            }, "heartbeat-test-worker");
        }

        static Worker start(TaskExecutor executor, UUID workerId) {
            Worker worker = new Worker(executor, workerId);
            worker.thread.start();
            return worker;
        }

        void join() throws InterruptedException {
            thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        }

        void assertClaimedWithoutThrowing() {
            assertFalse(thread.isAlive(), "worker did not return");
            assertNull(thrown.get());
            assertTrue(claimed.get());
        }
    }

    @TestConfiguration
    static class HoldConfig {

        @Bean
        HoldHandler holdHandler() {
            return new HoldHandler();
        }
    }

    /**
     * Blocks until released. With {@code awaitLeaseLost}, it first polls
     * {@link TaskContext#leaseLost()}, then still returns success.
     */
    static final class HoldHandler implements TaskHandler {

        private final AtomicInteger executions = new AtomicInteger();
        private final AtomicReference<Boolean> leaseLostAtReturn = new AtomicReference<>();
        private volatile boolean awaitLeaseLost;
        private volatile CountDownLatch started = new CountDownLatch(1);
        private volatile CountDownLatch sawLeaseLost = new CountDownLatch(1);
        private volatile CountDownLatch release = new CountDownLatch(1);

        void reset(boolean awaitLeaseLost) {
            this.awaitLeaseLost = awaitLeaseLost;
            executions.set(0);
            leaseLostAtReturn.set(null);
            started = new CountDownLatch(1);
            sawLeaseLost = new CountDownLatch(1);
            release = new CountDownLatch(1);
        }

        void release() {
            release.countDown();
        }

        @Override
        public String type() {
            return "HOLD";
        }

        @Override
        public TaskOutcome execute(TaskContext ctx) throws InterruptedException {
            executions.incrementAndGet();
            started.countDown();
            if (awaitLeaseLost) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
                while (!ctx.leaseLost()) {
                    if (System.nanoTime() >= deadline) {
                        throw new IllegalStateException("lease loss was not observed");
                    }
                    Thread.sleep(20);
                }
                sawLeaseLost.countDown();
            }
            if (!release.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException("handler was not released");
            }
            leaseLostAtReturn.set(ctx.leaseLost());
            return new TaskOutcome.Succeeded(null);
        }
    }
}
