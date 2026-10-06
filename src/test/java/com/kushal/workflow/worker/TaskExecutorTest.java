package com.kushal.workflow.worker;

import com.kushal.workflow.support.PostgresIntegrationTest;
import com.kushal.workflow.task.Task;
import com.kushal.workflow.task.TaskRepository;
import com.kushal.workflow.task.TaskService;
import com.kushal.workflow.task.TaskStatus;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link TaskExecutor#runOnce} with workers disabled. The probe handler
 * exists only in this context, so the shared tests never claim a
 * {@code PROBE} task.
 */
@SpringBootTest
class TaskExecutorTest extends PostgresIntegrationTest {

    private static final long WAIT_SECONDS = 5;

    @Autowired
    private TaskExecutor executor;

    @Autowired
    private TaskService taskService;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private DataSource dataSource;

    @Autowired
    @Qualifier("taskHandlers")
    private Map<String, TaskHandler> taskHandlers;

    @Autowired
    private ProbeHandler probe;

    @AfterEach
    void clearInterruptFlag() {
        Thread.interrupted();
        probe.setAction(null);
    }

    @Test
    void builtInHandlersAreRegisteredByType() {
        assertTrue(taskHandlers.keySet().containsAll(Set.of("NOOP", "ECHO", "FAIL", "SLEEP", "PROBE")));
        assertEquals("NOOP", taskHandlers.get("NOOP").type());
    }

    @Test
    void echoCompletesWithThePayloadAsTheResult() {
        UUID workerId = UUID.randomUUID();
        Task created = taskService.createTask("ECHO", "{\"message\":\"hello\"}");

        assertTrue(executor.runOnce(workerId));

        Task finished = require(created.id());
        assertEquals(TaskStatus.COMPLETED, finished.status());
        assertEquals(workerId, finished.workerId());
        assertNotNull(finished.finishedAt());
        assertNotNull(finished.updatedAt());
        assertTrue(resultEqualsPayload(created.id()));
    }

    @Test
    void noopCompletesWithJsonNull() {
        UUID workerId = UUID.randomUUID();
        Task created = taskService.createTask("NOOP", "{}");

        assertTrue(executor.runOnce(workerId));

        Task finished = require(created.id());
        assertEquals(TaskStatus.COMPLETED, finished.status());
        assertEquals(workerId, finished.workerId());
        assertTrue(jsonNullResult(created.id()));
    }

    @Test
    void failedOutcomeIsStored() {
        UUID workerId = UUID.randomUUID();
        Task created = taskService.createTask("FAIL", "{\"reason\":\"nope\"}");

        assertTrue(executor.runOnce(workerId));

        Task finished = require(created.id());
        assertEquals(TaskStatus.FAILED, finished.status());
        assertEquals("nope", finished.error());
        assertEquals(workerId, finished.workerId());
        assertNotNull(finished.finishedAt());
        assertNull(finished.result());
    }

    @Test
    void thrownExceptionIsFailedOnce() {
        UUID workerId = UUID.randomUUID();
        Task created = taskService.createTask("PROBE", "{}");
        probe.setAction(ctx -> {
            throw new IllegalStateException("boom");
        });

        assertTrue(executor.runOnce(workerId));

        Task finished = require(created.id());
        assertEquals(TaskStatus.FAILED, finished.status());
        assertEquals("java.lang.IllegalStateException: boom", finished.error());
        assertEquals(workerId, finished.workerId());
    }

    @Test
    void unknownTypeIsFailedOnce() {
        UUID workerId = UUID.randomUUID();
        Task created = taskService.createTask("UNKNOWN", "{}");

        assertTrue(executor.runOnce(workerId));

        Task finished = require(created.id());
        assertEquals(TaskStatus.FAILED, finished.status());
        assertEquals("no handler for type UNKNOWN", finished.error());
        assertEquals(workerId, finished.workerId());
    }

    @Test
    void nullOutcomeIsFailed() {
        UUID workerId = UUID.randomUUID();
        Task created = taskService.createTask("PROBE", "{}");
        probe.setAction(ctx -> null);

        assertTrue(executor.runOnce(workerId));

        Task finished = require(created.id());
        assertEquals(TaskStatus.FAILED, finished.status());
        assertEquals("handler returned null", finished.error());
    }

    @Test
    void emptyQueueReturnsFalseAndWritesNothing() {
        assertFalse(executor.runOnce(UUID.randomUUID()));
        assertEquals(0L, jdbcClient.sql("SELECT count(*) FROM tasks").query(Long.class).single());
    }

    @Test
    void errorPropagatesAndLeavesTheTaskRunning() {
        UUID workerId = UUID.randomUUID();
        Task created = taskService.createTask("PROBE", "{}");
        probe.setAction(ctx -> {
            throw new Error("handler error");
        });

        Error thrown = assertThrows(Error.class, () -> executor.runOnce(workerId));

        assertEquals("handler error", thrown.getMessage());
        Task current = require(created.id());
        assertEquals(TaskStatus.RUNNING, current.status());
        assertEquals(workerId, current.workerId());
        assertNull(current.finishedAt());
        assertNull(current.error());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void handlerHoldsNoConnection() throws Exception {
        CountDownLatch insideHandler = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<AssertionError> failure = new AtomicReference<>();
        probe.setAction(ctx -> {
            try {
                assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
                assertTrue(TransactionSynchronizationManager.getResourceMap().isEmpty());
            } catch (AssertionError error) {
                failure.set(error);
            }
            insideHandler.countDown();
            assertTrue(release.await(WAIT_SECONDS, TimeUnit.SECONDS), "handler was not released");
            return new TaskOutcome.Succeeded(null);
        });

        UUID workerId = UUID.randomUUID();
        Task created = taskService.createTask("PROBE", "{}");
        AtomicBoolean claimed = new AtomicBoolean();
        AtomicReference<Throwable> boom = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                claimed.set(executor.runOnce(workerId));
            } catch (Throwable thrown) {
                boom.set(thrown);
            }
        }, "probe-worker");
        worker.start();
        try {
            assertTrue(insideHandler.await(WAIT_SECONDS, TimeUnit.SECONDS), "handler did not start");
            HikariPoolMXBean pool = dataSource.unwrap(HikariDataSource.class).getHikariPoolMXBean();
            assertNotNull(pool);
            assertEquals(0, pool.getActiveConnections());
        } finally {
            release.countDown();
            worker.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        }

        assertFalse(worker.isAlive());
        assertNull(boom.get());
        assertNull(failure.get());
        assertTrue(claimed.get());
        assertEquals(TaskStatus.COMPLETED, require(created.id()).status());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void interruptDuringExecuteLeavesTheTaskRunning() throws Exception {
        UUID workerId = UUID.randomUUID();
        Task created = taskService.createTask("SLEEP", "{\"millis\":" + SleepHandler.MAX_MILLIS + "}");
        AtomicBoolean claimed = new AtomicBoolean();
        AtomicReference<Throwable> boom = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                claimed.set(executor.runOnce(workerId));
            } catch (Throwable thrown) {
                boom.set(thrown);
            }
        }, "sleep-worker");
        worker.start();
        try {
            assertEquals(TaskStatus.RUNNING, awaitStatus(created.id(), TaskStatus.RUNNING).status());
            worker.interrupt();
        } finally {
            worker.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        }

        assertFalse(worker.isAlive());
        assertNull(boom.get());
        assertTrue(claimed.get());
        Task current = require(created.id());
        assertEquals(TaskStatus.RUNNING, current.status());
        assertEquals(workerId, current.workerId());
        assertNull(current.finishedAt());
        assertNull(current.error());
    }

    @Test
    void returnedOutcomeIsFinishedEvenIfTheThreadWasInterrupted() {
        UUID workerId = UUID.randomUUID();
        Task succeeded = taskService.createTask("PROBE", "{\"n\":1}");
        probe.setAction(ctx -> {
            Thread.currentThread().interrupt();
            return new TaskOutcome.Succeeded(ctx.payload());
        });

        try {
            assertTrue(executor.runOnce(workerId));
            assertTrue(Thread.interrupted(), "interrupt flag restored after the finish write");
        } finally {
            Thread.interrupted();
        }

        Task completed = require(succeeded.id());
        assertEquals(TaskStatus.COMPLETED, completed.status());
        assertEquals(workerId, completed.workerId());
        assertTrue(resultEqualsPayload(succeeded.id()));

        Task failed = taskService.createTask("PROBE", "{}");
        probe.setAction(ctx -> {
            Thread.currentThread().interrupt();
            return new TaskOutcome.Failed("stopped after work");
        });

        try {
            assertTrue(executor.runOnce(workerId));
            assertTrue(Thread.interrupted(), "interrupt flag restored after the finish write");
        } finally {
            Thread.interrupted();
        }

        Task failedRow = require(failed.id());
        assertEquals(TaskStatus.FAILED, failedRow.status());
        assertEquals("stopped after work", failedRow.error());
        assertEquals(workerId, failedRow.workerId());
    }

    private Task awaitStatus(UUID id, TaskStatus status) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        Task current = require(id);
        while (current.status() != status && System.nanoTime() < deadline) {
            Thread.sleep(20);
            current = require(id);
        }
        return current;
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
    static class ProbeConfig {

        @Bean
        ProbeHandler probeHandler() {
            return new ProbeHandler();
        }
    }

    static final class ProbeHandler implements TaskHandler {

        private final AtomicReference<Action> action = new AtomicReference<>();

        @Override
        public String type() {
            return "PROBE";
        }

        void setAction(Action action) {
            this.action.set(action);
        }

        @Override
        public TaskOutcome execute(TaskContext ctx) throws Exception {
            Action current = action.get();
            if (current == null) {
                throw new IllegalStateException("no probe action");
            }
            return current.execute(ctx);
        }

        @FunctionalInterface
        interface Action {
            TaskOutcome execute(TaskContext ctx) throws Exception;
        }
    }
}
