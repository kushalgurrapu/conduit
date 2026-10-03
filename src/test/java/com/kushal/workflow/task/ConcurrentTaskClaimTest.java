package com.kushal.workflow.task;

import com.kushal.workflow.support.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

import java.time.Instant;
import java.time.OffsetDateTime;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Proves that concurrent callers of {@link TaskService#claimTask} cannot
 * successfully claim the same task.
 *
 * <p>Each round inserts a known set of PENDING tasks, then starts one thread
 * per worker. A {@link CyclicBarrier} holds every thread until all of them are
 * ready, so the claims overlap instead of lining up. Each thread calls the
 * transactional service method with its own worker id, which gives each caller
 * its own database transaction and connection.
 *
 * <p>The race this is aimed at: without row locking, two transactions can both
 * read the same PENDING id and both return it. The update matches that id
 * rather than re-checking status, so the second commit overwrites worker_id.
 * A sequential test misses that race. The first claim commits RUNNING before
 * the second claim starts, so the second claim no longer sees the row, and
 * both the locked and unlocked SQL pass.
 *
 * <p>What a passing run asserts: every successful claim has a distinct task id,
 * the number of successful claims matches the number of tasks, empty claims
 * match the surplus workers, and each claimed row is RUNNING with that
 * caller's worker id and a claimed_at timestamp.
 *
 * <p>What a passing run does not prove: that the SQL used SKIP LOCKED rather
 * than waiting on the lock. Waiting would also produce unique claims. Overlap
 * is encouraged, not guaranteed. The barrier, a pool large enough for every
 * worker, and repeated rounds make a missing lock likely to fail uniqueness,
 * but one unlucky run can still pass if the transactions never overlap.
 */
@SpringBootTest
@TestPropertySource(properties = "spring.datasource.hikari.maximum-pool-size=30")
class ConcurrentTaskClaimTest extends PostgresIntegrationTest {

    private static final int TASK_COUNT = 20;
    private static final int SURPLUS_WORKERS = 4;
    private static final int ROUNDS = 10;
    private static final long CLAIM_TIMEOUT_SECONDS = 15;

    @Autowired
    private TaskService taskService;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    void concurrentClaimsAreUniqueAndExhaustive() throws Exception {
        for (int round = 1; round <= ROUNDS; round++) {
            runClaimRound(round, TASK_COUNT, TASK_COUNT);
        }
    }

    @Test
    void extraWorkersDoNotDoubleClaim() throws Exception {
        for (int round = 1; round <= ROUNDS; round++) {
            runClaimRound(round, TASK_COUNT, TASK_COUNT + SURPLUS_WORKERS);
        }
    }

    private void runClaimRound(int round, int taskCount, int workerCount) throws Exception {
        List<UUID> createdIds = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        try {
            for (int i = 0; i < taskCount; i++) {
                createdIds.add(taskService.createTask("{\"test\":\"concurrent-claim\"}"));
            }

            CyclicBarrier start = new CyclicBarrier(workerCount);
            List<Future<Task>> futures = new ArrayList<>();
            for (int i = 0; i < workerCount; i++) {
                UUID workerId = UUID.randomUUID();
                futures.add(executor.submit(() -> claimAfterBarrier(start, workerId)));
            }

            List<Task> claimed = new ArrayList<>();
            int emptyClaims = 0;
            for (Future<Task> future : futures) {
                Task task = future.get(CLAIM_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (task == null) {
                    emptyClaims++;
                } else {
                    claimed.add(task);
                }
            }

            int expectedClaims = Math.min(taskCount, workerCount);
            int expectedEmpty = workerCount - expectedClaims;
            assertEquals(expectedClaims, claimed.size(), "round " + round + ": successful claim count");
            assertEquals(expectedEmpty, emptyClaims, "round " + round + ": empty claim count");

            List<UUID> claimedIds = claimed.stream().map(Task::id).toList();
            assertEquals(expectedClaims, new HashSet<>(claimedIds).size(),
                    "round " + round + ": a task id was claimed more than once");
            assertEquals(new HashSet<>(createdIds), new HashSet<>(claimedIds),
                    "round " + round + ": claimed ids");

            List<StoredTask> rows = findTasks(createdIds);
            assertEquals(taskCount, rows.size(), "round " + round + ": stored task count");
            assertEquals(expectedClaims, new HashSet<>(rows.stream().map(StoredTask::workerId).toList()).size(),
                    "round " + round + ": a worker id was stored more than once");
            for (Task task : claimed) {
                StoredTask row = rows.stream()
                        .filter(stored -> stored.id().equals(task.id()))
                        .findFirst()
                        .orElseThrow();
                assertEquals("RUNNING", row.status(), "round " + round + ": status for " + task.id());
                assertNotNull(row.claimedAt(), "round " + round + ": claimed_at for " + task.id());
                assertEquals(task.workerId(), row.workerId(), "round " + round + ": worker_id for " + task.id());
            }
        } finally {
            try {
                executor.shutdown();
                if (!executor.awaitTermination(CLAIM_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } finally {
                deleteTasks(createdIds);
            }
        }
    }

    private Task claimAfterBarrier(CyclicBarrier start, UUID workerId)
            throws BrokenBarrierException, InterruptedException, TimeoutException {
        start.await(CLAIM_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return taskService.claimTask(workerId);
    }

    private List<StoredTask> findTasks(List<UUID> ids) {
        List<StoredTask> rows = new ArrayList<>();
        for (UUID id : ids) {
            rows.add(jdbcClient.sql("""
                    SELECT id, status, claimed_at, worker_id
                    FROM tasks
                    WHERE id = :id
                    """)
                    .param("id", id)
                    .query((rs, rowNum) -> new StoredTask(
                            rs.getObject("id", UUID.class),
                            rs.getString("status"),
                            toInstant(rs.getObject("claimed_at", OffsetDateTime.class)),
                            rs.getObject("worker_id", UUID.class)
                    ))
                    .single());
        }
        return rows;
    }

    private void deleteTasks(List<UUID> ids) {
        for (UUID id : ids) {
            jdbcClient.sql("DELETE FROM tasks WHERE id = :id")
                    .param("id", id)
                    .update();
        }
    }

    private record StoredTask(UUID id, String status, Instant claimedAt, UUID workerId) {
    }

    private static Instant toInstant(OffsetDateTime value) {
        return value == null ? null : value.toInstant();
    }
}
