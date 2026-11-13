package com.kushal.workflow.task;

import com.kushal.workflow.support.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.random.RandomGenerator;
import java.util.random.RandomGeneratorFactory;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A seeded, single-threaded random walk over claims, heartbeats, completes,
 * dead workers, paused workers that wake as zombies, lease expiry, and
 * reaps. Every write is checked against the row read just before it, every
 * reap against the rows the walk expired, and I1 to I5 every 50 steps and
 * after the drain, which ends with every task {@code COMPLETED}.
 *
 * <p>The lease is an hour, so a lease expires only when the walk says time
 * passes. Replay a run with {@code -Drecovery.soak.seed=N}; the last log
 * line gives the seed and the SHA-256 of the trace, which is the same for
 * every run of one seed and size.
 */
@SpringBootTest
@TestPropertySource(properties = "engine.lease.duration=1h")
class RecoverySoakTest extends PostgresIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(RecoverySoakTest.class);

    private static final long DEFAULT_SEED = 20261113L;
    private static final int DEFAULT_TASKS = 100;
    private static final int DEFAULT_WORKERS = 6;
    private static final int DEFAULT_STEPS = 2000;
    private static final int INVARIANT_EVERY = 50;
    private static final int MAX_REAP_BATCH = 20;
    private static final double REUSE_ZOMBIE_ID = 0.3;
    private static final Instant CREATED_BASE = Instant.parse("2000-01-01T00:00:00Z");

    @Autowired
    private TaskService taskService;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    void randomWalkKeepsTheFenceAndTheHistoryAndDrainsToCompleted() {
        long seed = Long.getLong("recovery.soak.seed", DEFAULT_SEED);
        int tasks = Integer.getInteger("recovery.soak.tasks", DEFAULT_TASKS);
        int workers = Integer.getInteger("recovery.soak.workers", DEFAULT_WORKERS);
        int steps = Integer.getInteger("recovery.soak.steps", DEFAULT_STEPS);
        log.info("Recovery soak: seed={} tasks={} workers={} steps={}", seed, tasks, workers, steps);
        Walk walk = new Walk(seed, tasks, workers);

        try {
            walk.run(steps);
            walk.drain();
        } catch (Throwable ex) {
            throw new AssertionError("seed=" + seed + ", step=" + walk.step, ex);
        }

        log.info("Recovery soak: seed={} trace entries={} sha256={} counters={}",
                seed, walk.trace.size(), walk.digest(), walk.counters);
        if (seed == DEFAULT_SEED && tasks == DEFAULT_TASKS && workers == DEFAULT_WORKERS && steps == DEFAULT_STEPS) {
            for (Counter counter : Counter.values()) {
                assertTrue(walk.counters.get(counter) > 0, "coverage counter " + counter + " is 0: " + walk.counters);
            }
        }
    }

    private enum Action {
        CLAIM(4),
        HEARTBEAT(3),
        COMPLETE(2),
        DIE(1),
        PAUSE(2),
        WAKE(3),
        TIME_PASSES(3),
        REAP(2);

        private final int weight;

        Action(int weight) {
            this.weight = weight;
        }
    }

    private enum Counter {
        ZOMBIE_REJECTED_AFTER_SAME_ID_RECLAIM,
        ZOMBIE_REJECTED_AFTER_OTHER_WORKER_RECLAIM,
        ZOMBIE_REJECTED_BEFORE_RECLAIM,
        ZOMBIE_APPLIED,
        REAP_WHILE_HOLDER_PAUSED,
        HEARTBEAT_AFTER_REAP,
        CLAIM_RETURNED_NULL
    }

    /** One claimed attempt, by task index. */
    private record Hold(int task, int attempt) {
    }

    private record Zombie(UUID workerId, Hold hold) {
    }

    private static final class Slot {

        private UUID workerId;
        private Hold hold;

        private Slot(UUID workerId) {
            this.workerId = workerId;
        }
    }

    private record Row(TaskStatus status, UUID workerId, int attempt, String text) {

        boolean ownedBy(UUID worker, int claimedAttempt) {
            return status == TaskStatus.RUNNING && worker.equals(workerId) && attempt == claimedAttempt;
        }
    }

    private record StoredTask(UUID id, TaskStatus status, int attempt, UUID workerId,
                              Integer resultAttempt, UUID resultWorker) {
    }

    private record StoredAttempt(UUID taskId, int attempt, UUID workerId, AttemptOutcome outcome) {
    }

    /** The walk's model. Rows and workers are picked from these lists, never in database order. */
    private final class Walk {

        private final RandomGenerator rng;
        private final List<UUID> taskIds = new ArrayList<>();
        private final Map<UUID, Integer> indexOf = new HashMap<>();
        private final List<List<UUID>> claimers = new ArrayList<>();
        private final int[] lastAttempt;
        private final List<Slot> slots = new ArrayList<>();
        private final List<Zombie> zombies = new ArrayList<>();
        private final TreeSet<Integer> expectedExpired = new TreeSet<>();
        private final Set<Hold> reaped = new HashSet<>();
        private final Map<Counter, Integer> counters = new EnumMap<>(Counter.class);
        private final List<String> trace = new ArrayList<>();
        private int step;
        private int expiries;
        private boolean draining;

        private Walk(long seed, int tasks, int workers) {
            this.rng = RandomGeneratorFactory.of("L64X128MixRandom").create(seed);
            this.lastAttempt = new int[tasks];
            for (Counter counter : Counter.values()) {
                counters.put(counter, 0);
            }
            for (int i = 0; i < tasks; i++) {
                UUID id = taskService.createTask("NOOP", "{\"index\":" + i + "}").id();
                jdbcClient.sql("UPDATE tasks SET created_at = :base + :index * INTERVAL '1 millisecond' WHERE id = :id")
                        .param("base", OffsetDateTime.ofInstant(CREATED_BASE, ZoneOffset.UTC),
                                Types.TIMESTAMP_WITH_TIMEZONE)
                        .param("index", i)
                        .param("id", id)
                        .update();
                taskIds.add(id);
                indexOf.put(id, i);
                claimers.add(new ArrayList<>());
            }
            for (int w = 0; w < workers; w++) {
                slots.add(new Slot(newWorkerId()));
            }
        }

        void run(int steps) {
            for (step = 0; step < steps; step++) {
                List<Action> applicable = applicable();
                int total = applicable.stream().mapToInt(action -> action.weight).sum();
                int pick = rng.nextInt(total);
                Action chosen = null;
                for (Action action : applicable) {
                    pick -= action.weight;
                    if (pick < 0) {
                        chosen = action;
                        break;
                    }
                }
                perform(chosen);
                if ((step + 1) % INVARIANT_EVERY == 0) {
                    checkInvariants();
                }
            }
            checkInvariants();
        }

        private List<Action> applicable() {
            boolean idle = slots.stream().anyMatch(slot -> slot.hold == null);
            boolean holding = slots.stream().anyMatch(slot -> slot.hold != null);
            List<Action> actions = new ArrayList<>();
            for (Action action : Action.values()) {
                boolean ok = switch (action) {
                    case CLAIM -> idle;
                    case HEARTBEAT, COMPLETE, DIE, PAUSE -> holding;
                    case WAKE -> !zombies.isEmpty();
                    case TIME_PASSES, REAP -> true;
                };
                if (ok) {
                    actions.add(action);
                }
            }
            return actions;
        }

        private void perform(Action action) {
            switch (action) {
                case CLAIM -> {
                    int s = pickSlot(true);
                    claim(s);
                }
                case HEARTBEAT -> {
                    int s = pickSlot(false);
                    Slot slot = slots.get(s);
                    boolean extended = extend(slot.workerId, slot.hold, null);
                    record(action, slot.hold.task(), s, extended ? "TRUE" : "FALSE");
                }
                case COMPLETE -> {
                    int s = pickSlot(false);
                    Slot slot = slots.get(s);
                    Hold hold = slot.hold;
                    String result = finish(slot.workerId, hold, true, null);
                    slot.hold = null;
                    record(action, hold.task(), s, result);
                }
                case DIE -> {
                    int s = pickSlot(false);
                    Slot slot = slots.get(s);
                    int task = slot.hold.task();
                    slot.hold = null;
                    slot.workerId = newWorkerId();
                    record(action, task, s, "DIED");
                }
                case PAUSE -> {
                    int s = pickSlot(false);
                    Slot slot = slots.get(s);
                    Hold hold = slot.hold;
                    zombies.add(new Zombie(slot.workerId, hold));
                    slot.hold = null;
                    boolean reuse = rng.nextDouble() < REUSE_ZOMBIE_ID;
                    if (!reuse) {
                        slot.workerId = newWorkerId();
                    }
                    record(action, hold.task(), s, reuse ? "PAUSED REUSE_ID" : "PAUSED NEW_ID");
                }
                case WAKE -> {
                    int z = rng.nextInt(zombies.size());
                    Zombie zombie = zombies.get(z);
                    if (rng.nextBoolean()) {
                        boolean extended = extend(zombie.workerId(), zombie.hold(), zombie);
                        record(action, zombie.hold().task(), z, "EXTEND " + (extended ? "TRUE" : "FALSE"));
                    } else {
                        zombies.remove(z);
                        String result = finishZombie(zombie);
                        record(action, zombie.hold().task(), z, result);
                    }
                }
                case TIME_PASSES -> timePasses();
                case REAP -> reap(1 + rng.nextInt(MAX_REAP_BATCH));
            }
        }

        /** An idle slot when {@code idle}, otherwise a holding slot, in slot order. */
        private int pickSlot(boolean idle) {
            List<Integer> candidates = new ArrayList<>();
            for (int s = 0; s < slots.size(); s++) {
                if ((slots.get(s).hold == null) == idle) {
                    candidates.add(s);
                }
            }
            return candidates.get(rng.nextInt(candidates.size()));
        }

        private Hold claim(int s) {
            Slot slot = slots.get(s);
            Task claimed = taskService.claimTask(slot.workerId);
            if (claimed == null) {
                assertEquals(0L, countRows("SELECT count(*) FROM tasks WHERE status = 'PENDING'"),
                        "claim returned null while a task was PENDING");
                cover(Counter.CLAIM_RETURNED_NULL);
                record(Action.CLAIM, -1, s, "NULL");
                return null;
            }
            int task = indexOf.get(claimed.id());
            claimers.get(task).add(slot.workerId);
            assertEquals(TaskStatus.RUNNING, claimed.status());
            assertEquals(slot.workerId, claimed.workerId());
            assertEquals(claimers.get(task).size(), claimed.attempt(), "claimed attempt");
            slot.hold = new Hold(task, claimed.attempt());
            record(Action.CLAIM, task, s, "ATTEMPT " + claimed.attempt());
            return slot.hold;
        }

        /**
         * {@code true} exactly when the row was {@code RUNNING} with this
         * worker and attempt. Otherwise the row and its history are unchanged.
         */
        private boolean extend(UUID workerId, Hold hold, Zombie zombie) {
            UUID id = taskIds.get(hold.task());
            Row before = row(id);
            String historyBefore = history(id);
            boolean owned = before.ownedBy(workerId, hold.attempt());

            boolean extended = taskService.extendLease(id, workerId, hold.attempt());

            assertEquals(owned, extended, "extend of " + hold + " by " + workerId + " on " + before.text());
            if (extended) {
                expectedExpired.remove(hold.task());
                Row after = row(id);
                assertTrue(after.ownedBy(workerId, hold.attempt()));
                assertTrue(leaseInTheFuture(id), "an applied extend leaves a lease in the future");
            } else {
                assertEquals(before.text(), row(id).text(), "a rejected extend changed the row");
                assertEquals(historyBefore, history(id), "a rejected extend changed the history");
                if (reaped.contains(hold)) {
                    cover(Counter.HEARTBEAT_AFTER_REAP);
                }
            }
            if (zombie != null) {
                countZombie(zombie, extended, false);
            }
            return extended;
        }

        /** A zombie completes when the write would apply. Otherwise it completes or fails at random. */
        private String finishZombie(Zombie zombie) {
            Row before = row(taskIds.get(zombie.hold().task()));
            boolean complete = before.ownedBy(zombie.workerId(), zombie.hold().attempt()) || rng.nextBoolean();
            return finish(zombie.workerId(), zombie.hold(), complete, zombie);
        }

        /**
         * {@code Applied} exactly when the row was {@code RUNNING} with this
         * worker and attempt. Otherwise {@code Rejected} with the row's status
         * and worker, and the row and its history are unchanged.
         */
        private String finish(UUID workerId, Hold hold, boolean complete, Zombie zombie) {
            UUID id = taskIds.get(hold.task());
            Row before = row(id);
            String historyBefore = history(id);
            boolean owned = before.ownedBy(workerId, hold.attempt());
            assertTrue(complete || !owned, "the walk never writes an applied fail");

            TransitionResult result = complete
                    ? taskService.completeTask(id, workerId, hold.attempt(), resultJson(hold.attempt(), workerId))
                    : taskService.failTask(id, workerId, hold.attempt(), "zombie " + workerId);

            String kind = complete ? "COMPLETE" : "FAIL";
            if (owned) {
                TransitionResult.Applied applied = assertInstanceOf(TransitionResult.Applied.class, result,
                        "finish of " + hold + " by " + workerId + " on " + before.text());
                assertEquals(TaskStatus.COMPLETED, applied.task().status());
                expectedExpired.remove(hold.task());
            } else {
                assertEquals(new TransitionResult.Rejected(before.status(), before.workerId()), result,
                        "finish of " + hold + " by " + workerId + " on " + before.text());
                assertEquals(before.text(), row(id).text(), "a rejected finish changed the row");
                assertEquals(historyBefore, history(id), "a rejected finish changed the history");
            }
            if (zombie != null) {
                countZombie(zombie, owned, true);
            }
            return (owned ? "APPLIED " : "REJECTED ") + kind + (owned ? "" : " " + before.status());
        }

        private void countZombie(Zombie zombie, boolean applied, boolean finish) {
            if (applied) {
                if (finish) {
                    cover(Counter.ZOMBIE_APPLIED);
                }
                return;
            }
            List<UUID> claimedBy = claimers.get(zombie.hold().task());
            int attempt = zombie.hold().attempt();
            if (claimedBy.size() == attempt) {
                cover(Counter.ZOMBIE_REJECTED_BEFORE_RECLAIM);
            } else if (claimedBy.subList(attempt, claimedBy.size()).contains(zombie.workerId())) {
                cover(Counter.ZOMBIE_REJECTED_AFTER_SAME_ID_RECLAIM);
            } else {
                cover(Counter.ZOMBIE_REJECTED_AFTER_OTHER_WORKER_RECLAIM);
            }
        }

        private void timePasses() {
            List<Integer> running = runningInIndexOrder();
            if (running.isEmpty()) {
                record(Action.TIME_PASSES, -1, -1, "NONE");
                return;
            }
            expire(running.get(rng.nextInt(running.size())));
        }

        private void expire(int task) {
            // The time is fixed and in the past, so a backward step of the database clock cannot un-expire the row.
            int updated = jdbcClient.sql("""
                            UPDATE tasks
                            SET lease_expires_at = TIMESTAMPTZ '2000-01-01 00:00:00+00' + :n * INTERVAL '1 microsecond'
                            WHERE id = :id AND status = 'RUNNING'
                            """)
                    .param("n", ++expiries)
                    .param("id", taskIds.get(task))
                    .update();
            assertEquals(1, updated);
            expectedExpired.add(task);
            record(Action.TIME_PASSES, task, -1, "EXPIRED");
        }

        /**
         * The reaped rows are rows the walk expired, all of them when the
         * batch has room, each at the attempt and worker of its last claim.
         */
        private List<ReapedTask> reap(int batchSize) {
            Set<Integer> expected = Set.copyOf(expectedExpired);
            List<ReapedTask> batch = taskService.reapExpiredLeases(batchSize);

            TreeSet<Integer> reapedTasks = new TreeSet<>();
            for (ReapedTask task : batch) {
                int index = indexOf.get(task.taskId());
                reapedTasks.add(index);
                List<UUID> claimedBy = claimers.get(index);
                assertEquals(claimedBy.size(), task.attempt(), "reaped attempt of task " + index);
                assertEquals(claimedBy.getLast(), task.previousWorkerId(), "reaped worker of task " + index);
                assertTrue(task.attemptRecorded(), "reaped attempt of task " + index + " was recorded");
                Hold hold = new Hold(index, task.attempt());
                reaped.add(hold);
                if (zombies.stream().anyMatch(zombie -> zombie.hold().equals(hold))) {
                    cover(Counter.REAP_WHILE_HOLDER_PAUSED);
                }
            }
            assertEquals(batch.size(), reapedTasks.size(), "a task was reaped twice in one batch");
            assertTrue(expected.containsAll(reapedTasks),
                    () -> "reaped " + reapedTasks + ", expected within " + expected + expiredRows(expected));
            if (batchSize >= expected.size()) {
                assertEquals(expected, reapedTasks,
                        () -> "a batch of " + batchSize + " reaped" + expiredRows(expected));
            }
            expectedExpired.removeAll(reapedTasks);
            record(Action.REAP, -1, -1, "BATCH " + batchSize + " REAPED " + reapedTasks);
            return batch;
        }

        /** One line per task, in index order, with its row as it is now and the database's {@code NOW()}. */
        private String expiredRows(Set<Integer> indexes) {
            StringBuilder out = new StringBuilder();
            for (int index : new TreeSet<>(indexes)) {
                out.append('\n').append(jdbcClient.sql("""
                                SELECT status, worker_id, attempt, lease_expires_at, NOW() AS now
                                FROM tasks
                                WHERE id = :id
                                """)
                        .param("id", taskIds.get(index))
                        .query((rs, rowNum) -> "  task " + index
                                + " status=" + rs.getString("status")
                                + " worker_id=" + rs.getObject("worker_id", UUID.class)
                                + " attempt=" + rs.getInt("attempt")
                                + " lease_expires_at=" + rs.getObject("lease_expires_at", OffsetDateTime.class)
                                + " now=" + rs.getObject("now", OffsetDateTime.class))
                        .single());
            }
            return out.toString();
        }

        /**
         * Wakes every zombie into a finish, then expires every {@code RUNNING}
         * row, reaps until a batch is empty, and lets the live workers claim
         * and complete, until no task is {@code RUNNING} or {@code PENDING}.
         */
        void drain() {
            draining = true;
            while (!zombies.isEmpty()) {
                Zombie zombie = zombies.removeFirst();
                record(Action.WAKE, zombie.hold().task(), -1, finishZombie(zombie));
                step++;
            }
            int rounds = 0;
            while (countRows("SELECT count(*) FROM tasks WHERE status IN ('RUNNING', 'PENDING')") > 0) {
                if (++rounds > taskIds.size() + 1) {
                    fail("drain did not finish within " + rounds + " rounds");
                }
                for (int task : runningInIndexOrder()) {
                    expire(task);
                    step++;
                }
                int reaps = 0;
                while (!reap(1 + rng.nextInt(MAX_REAP_BATCH)).isEmpty()) {
                    step++;
                    if (++reaps > taskIds.size() + 1) {
                        fail("reaps did not come back empty");
                    }
                }
                boolean claimedAny = true;
                while (claimedAny) {
                    claimedAny = false;
                    for (int s = 0; s < slots.size(); s++) {
                        Slot slot = slots.get(s);
                        if (slot.hold != null) {
                            Hold stale = slot.hold;
                            record(Action.COMPLETE, stale.task(), s, finish(slot.workerId, stale, true, null));
                            slot.hold = null;
                            step++;
                        }
                        Hold hold = claim(s);
                        step++;
                        if (hold != null) {
                            claimedAny = true;
                            record(Action.COMPLETE, hold.task(), s, finish(slot.workerId, hold, true, null));
                            slot.hold = null;
                            step++;
                        }
                    }
                }
            }
            List<StoredTask> rows = storedTasks();
            for (StoredTask row : rows) {
                assertEquals(TaskStatus.COMPLETED, row.status(), "task " + indexOf.get(row.id()) + " after the drain");
            }
            checkInvariants();
        }

        /** I1 to I5, read by SQL for every task. */
        private void checkInvariants() {
            Map<UUID, List<StoredAttempt>> history = new HashMap<>();
            for (StoredAttempt attempt : storedAttempts()) {
                history.computeIfAbsent(attempt.taskId(), id -> new ArrayList<>()).add(attempt);
            }
            Map<UUID, StoredTask> rows = new HashMap<>();
            for (StoredTask row : storedTasks()) {
                rows.put(row.id(), row);
            }
            for (int i = 0; i < taskIds.size(); i++) {
                StoredTask row = rows.get(taskIds.get(i));
                List<StoredAttempt> attempts = history.getOrDefault(row.id(), List.of());
                String where = "task " + i + " " + row + " " + attempts;

                List<StoredAttempt> open = attempts.stream().filter(attempt -> attempt.outcome() == null).toList();
                if (row.status() == TaskStatus.RUNNING) {
                    assertEquals(1, open.size(), "I1: " + where);
                    assertEquals(row.attempt(), open.getFirst().attempt(), "I1: " + where);
                } else {
                    assertEquals(List.of(), open, "I1: " + where);
                }

                assertEquals(IntStream.rangeClosed(1, row.attempt()).boxed().toList(),
                        attempts.stream().map(StoredAttempt::attempt).toList(), "I2: " + where);
                for (StoredAttempt attempt : attempts.subList(0, Math.max(0, attempts.size() - 1))) {
                    assertEquals(AttemptOutcome.LEASE_EXPIRED, attempt.outcome(), "I2: " + where);
                }

                if (row.status() == TaskStatus.COMPLETED) {
                    assertEquals(row.attempt(), row.resultAttempt(), "I3: " + where);
                    assertEquals(row.workerId(), row.resultWorker(), "I3: " + where);
                    List<StoredAttempt> completed = attempts.stream()
                            .filter(attempt -> attempt.outcome() == AttemptOutcome.COMPLETED)
                            .toList();
                    assertEquals(1, completed.size(), "I3: " + where);
                    assertEquals(attempts.getLast(), completed.getFirst(), "I3: " + where);
                    assertEquals(row.workerId(), completed.getFirst().workerId(), "I3: " + where);
                } else {
                    assertNull(row.resultAttempt(), "I3: " + where);
                }

                assertTrue(row.attempt() >= lastAttempt[i], "I4: attempt went from " + lastAttempt[i] + " in " + where);
                lastAttempt[i] = row.attempt();

                assertEquals(claimers.get(i).size(), row.attempt(), "I5: " + where);
            }
        }

        private List<Integer> runningInIndexOrder() {
            Set<UUID> running = new HashSet<>(jdbcClient.sql("SELECT id FROM tasks WHERE status = 'RUNNING'")
                    .query(UUID.class)
                    .list());
            List<Integer> indexes = new ArrayList<>();
            for (int i = 0; i < taskIds.size(); i++) {
                if (running.contains(taskIds.get(i))) {
                    indexes.add(i);
                }
            }
            return indexes;
        }

        private UUID newWorkerId() {
            return new UUID(rng.nextLong(), rng.nextLong());
        }

        private void cover(Counter counter) {
            if (!draining) {
                counters.merge(counter, 1, Integer::sum);
            }
        }

        private void record(Action action, int task, int worker, String result) {
            trace.add(step + " " + action + " " + task + " " + worker + " " + result);
        }

        String digest() {
            try {
                MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
                return HexFormat.of().formatHex(sha256.digest(String.join("\n", trace).getBytes(StandardCharsets.UTF_8)));
            } catch (Exception ex) {
                throw new IllegalStateException(ex);
            }
        }
    }

    private static String resultJson(int attempt, UUID workerId) {
        return "{\"attempt\":" + attempt + ",\"worker\":\"" + workerId + "\"}";
    }

    private Row row(UUID id) {
        return jdbcClient.sql("SELECT status, worker_id, attempt, t::text AS text FROM tasks t WHERE id = :id")
                .param("id", id)
                .query((rs, rowNum) -> new Row(
                        TaskStatus.valueOf(rs.getString("status")),
                        rs.getObject("worker_id", UUID.class),
                        rs.getInt("attempt"),
                        rs.getString("text")))
                .single();
    }

    private boolean leaseInTheFuture(UUID id) {
        return jdbcClient.sql("SELECT lease_expires_at > NOW() FROM tasks WHERE id = :id")
                .param("id", id)
                .query(Boolean.class)
                .single();
    }

    private String history(UUID id) {
        return String.join("\n", jdbcClient.sql("""
                        SELECT a::text FROM task_attempts a WHERE task_id = :id ORDER BY attempt
                        """)
                .param("id", id)
                .query(String.class)
                .list());
    }

    private List<StoredTask> storedTasks() {
        return jdbcClient.sql("""
                        SELECT id, status, attempt, worker_id,
                               (result ->> 'attempt')::int AS result_attempt,
                               (result ->> 'worker')::uuid AS result_worker
                        FROM tasks
                        ORDER BY id
                        """)
                .query((rs, rowNum) -> new StoredTask(
                        rs.getObject("id", UUID.class),
                        TaskStatus.valueOf(rs.getString("status")),
                        rs.getInt("attempt"),
                        rs.getObject("worker_id", UUID.class),
                        rs.getObject("result_attempt", Integer.class),
                        rs.getObject("result_worker", UUID.class)))
                .list();
    }

    private List<StoredAttempt> storedAttempts() {
        return jdbcClient.sql("""
                        SELECT task_id, attempt, worker_id, outcome
                        FROM task_attempts
                        ORDER BY task_id, attempt
                        """)
                .query((rs, rowNum) -> new StoredAttempt(
                        rs.getObject("task_id", UUID.class),
                        rs.getInt("attempt"),
                        rs.getObject("worker_id", UUID.class),
                        rs.getString("outcome") == null ? null : AttemptOutcome.valueOf(rs.getString("outcome"))))
                .list();
    }

    private long countRows(String sql) {
        return jdbcClient.sql(sql).query(Long.class).single();
    }
}
