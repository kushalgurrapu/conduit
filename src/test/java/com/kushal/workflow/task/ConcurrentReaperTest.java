package com.kushal.workflow.task;

import com.kushal.workflow.support.PostgresIntegrationTest;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves that concurrent reapers requeue each expired attempt exactly once
 * and close its history row exactly once.
 *
 * <p>Each round inserts 1,000 expired {@code RUNNING} rows at attempt 1, each
 * with the open history row a claim writes. Three threads start on a
 * {@link CyclicBarrier}, and each calls the transactional service method
 * with batches of 25 until a batch comes back empty. Each call is its own
 * transaction on its own pooled connection, so batches can overlap. Overlap
 * is encouraged, not guaranteed.
 *
 * <p>What a passing run asserts: the batches together return every row once,
 * every row is {@code PENDING} at the same attempt, and history has one
 * closed {@code LEASE_EXPIRED} row per task and no open row. It does not
 * prove the reap skips locked rows rather than waiting on them; waiting would
 * also give unique results. {@link ReaperLockTest} pins that.
 *
 * <p>The shared context's pool (at most 10 connections) is enough for three
 * reapers. A larger pool would need its own context.
 */
@SpringBootTest
class ConcurrentReaperTest extends PostgresIntegrationTest {

    private static final int TASK_COUNT = 1_000;
    private static final int REAPERS = 3;
    private static final int BATCH_SIZE = 25;
    private static final long REAP_TIMEOUT_SECONDS = 30;
    private static final Instant FIXTURE_TIME = Instant.parse("2000-01-01T00:00:00Z");
    private static final UUID OWNER = UUID.fromString("00000000-0000-0000-0000-00000000000a");

    @Autowired
    private TaskService taskService;

    @Autowired
    private JdbcClient jdbcClient;

    @RepeatedTest(3)
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void concurrentReapersRequeueEachExpiredAttemptOnce(RepetitionInfo repetition) throws Exception {
        int round = repetition.getCurrentRepetition();
        List<UUID> createdIds = insertExpiredRunning();
        assertEquals(TASK_COUNT, createdIds.size());

        ExecutorService executor = Executors.newFixedThreadPool(REAPERS);
        List<List<ReapedTask>> batches = new ArrayList<>();
        try {
            CyclicBarrier start = new CyclicBarrier(REAPERS);
            List<Future<List<List<ReapedTask>>>> futures = new ArrayList<>();
            for (int i = 0; i < REAPERS; i++) {
                futures.add(executor.submit(() -> reapAfterBarrier(start)));
            }
            for (Future<List<List<ReapedTask>>> future : futures) {
                batches.addAll(future.get(REAP_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(REAP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }

        List<ReapedTask> reaped = batches.stream().flatMap(List::stream).toList();
        assertEquals(TASK_COUNT, batches.stream().mapToInt(List::size).sum(), "round " + round + ": batch sizes");
        assertTrue(batches.stream().allMatch(batch -> batch.size() <= BATCH_SIZE),
                "round " + round + ": a batch was larger than " + BATCH_SIZE);
        List<UUID> reapedIds = reaped.stream().map(ReapedTask::taskId).toList();
        assertEquals(TASK_COUNT, new HashSet<>(reapedIds).size(), "round " + round + ": a task was reaped twice");
        assertEquals(new HashSet<>(createdIds), new HashSet<>(reapedIds), "round " + round + ": reaped ids");
        assertTrue(reaped.stream().allMatch(task ->
                        task.attempt() == 1 && OWNER.equals(task.previousWorkerId()) && task.attemptRecorded()),
                "round " + round + ": every reaped attempt is attempt 1 of OWNER and recorded");

        assertEquals(0, count("SELECT count(*) FROM tasks WHERE status = 'RUNNING'"), "round " + round);
        assertEquals(TASK_COUNT, count("SELECT count(*) FROM tasks WHERE status = 'PENDING' AND attempt = 1"),
                "round " + round);
        assertEquals(TASK_COUNT, count("SELECT count(*) FROM task_attempts WHERE outcome = 'LEASE_EXPIRED'"),
                "round " + round);
        assertEquals(0, count("SELECT count(*) FROM task_attempts WHERE outcome IS NULL"), "round " + round);
        assertEquals(TASK_COUNT, count("SELECT count(*) FROM task_attempts"), "round " + round);
    }

    private List<List<ReapedTask>> reapAfterBarrier(CyclicBarrier start)
            throws BrokenBarrierException, InterruptedException, TimeoutException {
        start.await(REAP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        List<List<ReapedTask>> batches = new ArrayList<>();
        while (true) {
            List<ReapedTask> batch = taskService.reapExpiredLeases(BATCH_SIZE);
            if (batch.isEmpty()) {
                return batches;
            }
            batches.add(batch);
        }
    }

    /** Rows as a claim leaves them, at attempt 1 with an open history row, and leases already past. */
    private List<UUID> insertExpiredRunning() {
        List<UUID> ids = jdbcClient.sql("""
                        INSERT INTO tasks (
                            id, task_type, status, payload, created_at, updated_at,
                            claimed_at, worker_id, attempt, lease_expires_at
                        )
                        SELECT gen_random_uuid(), 'NOOP', 'RUNNING', CAST('{}' AS jsonb), :at, :at,
                               :at, :workerId, 1, :at + n * INTERVAL '1 millisecond'
                        FROM generate_series(1, :count) AS n
                        RETURNING id
                        """)
                .param("at", OffsetDateTime.ofInstant(FIXTURE_TIME, ZoneOffset.UTC), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("workerId", OWNER)
                .param("count", TASK_COUNT)
                .query(UUID.class)
                .list();
        jdbcClient.sql("""
                        INSERT INTO task_attempts (task_id, attempt, worker_id, started_at)
                        SELECT id, attempt, worker_id, claimed_at
                        FROM tasks
                        WHERE status = 'RUNNING'
                        """)
                .update();
        return ids;
    }

    private long count(String sql) {
        return jdbcClient.sql(sql).query(Long.class).single();
    }
}
