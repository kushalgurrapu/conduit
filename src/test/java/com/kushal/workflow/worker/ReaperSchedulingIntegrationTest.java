package com.kushal.workflow.worker;

import com.kushal.workflow.support.PostgresIntegrationTest;
import com.kushal.workflow.task.AttemptOutcome;
import com.kushal.workflow.task.Task;
import com.kushal.workflow.task.TaskAttempt;
import com.kushal.workflow.task.TaskService;
import com.kushal.workflow.task.TaskStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The reaper bean on its own thread, in the only context that enables it.
 * The worker pool stays off, so the reaper runs without workers.
 *
 * <p>The context is closed after this class. A reaper left running in a
 * cached context would requeue other classes' fixtures.
 */
@SpringBootTest
@TestPropertySource(properties = {
        "engine.reaper.enabled=true",
        "engine.reaper.interval=100ms"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class ReaperSchedulingIntegrationTest extends PostgresIntegrationTest {

    private static final long WAIT_SECONDS = 10;

    @Autowired
    private LeaseReaper reaper;

    @Autowired
    private WorkerPool workerPool;

    @Autowired
    private TaskService taskService;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    void scheduledReaperRequeuesTheExpiredRowAndLeavesTheLiveOne() throws Exception {
        assertTrue(reaper.isRunning());
        assertFalse(workerPool.isRunning());
        UUID workerId = UUID.randomUUID();
        Task expired = claim(workerId, "expired");
        Task live = claim(workerId, "live");
        setLease(live.id(), "NOW() + INTERVAL '1 hour'");
        setLease(expired.id(), "TIMESTAMPTZ '2000-01-01 00:00:00+00'");

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (true) {
            Task current = taskService.getTask(expired.id());
            List<TaskAttempt> attempts = taskService.getAttempts(expired.id());
            if (current.status() == TaskStatus.PENDING
                    && attempts.size() == 1
                    && attempts.getFirst().outcome() == AttemptOutcome.LEASE_EXPIRED) {
                assertEquals(1, current.attempt());
                assertEquals(workerId, attempts.getFirst().workerId());
                break;
            }
            if (System.nanoTime() >= deadline) {
                fail("expired row was not reaped: " + current + " " + attempts);
            }
            Thread.sleep(50);
        }

        Task stillLive = taskService.getTask(live.id());
        assertEquals(TaskStatus.RUNNING, stillLive.status());
        assertEquals(workerId, stillLive.workerId());
        assertEquals(1, stillLive.attempt());
        assertTrue(reaper.isRunning());
    }

    private Task claim(UUID workerId, String name) {
        Task created = taskService.createTask("NOOP", "{\"test\":\"" + name + "\"}");
        Task claimed = taskService.claimTask(workerId);
        assertNotNull(claimed);
        assertEquals(created.id(), claimed.id());
        return claimed;
    }

    private void setLease(UUID id, String leaseExpression) {
        jdbcClient.sql("UPDATE tasks SET lease_expires_at = " + leaseExpression + " WHERE id = :id")
                .param("id", id)
                .update();
    }
}
