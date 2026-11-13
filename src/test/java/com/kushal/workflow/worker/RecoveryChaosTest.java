package com.kushal.workflow.worker;

import com.kushal.workflow.task.AttemptOutcome;
import com.kushal.workflow.task.LeaseProperties;
import com.kushal.workflow.task.TaskService;
import com.kushal.workflow.task.TaskStatus;
import com.kushal.workflow.task.TransitionResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Four worker slots and two reapers run 40 tasks whose handlers crash at
 * random. A crashed handler blocks with no heartbeat, its slot is replaced,
 * sometimes under the same worker id, and a reaper requeues the task once
 * the one-second lease expires. Every task completes once, from its last
 * attempt; every attempt runs exactly once; and released zombies change
 * nothing.
 *
 * <p>Replay a seed with {@code -Drecovery.chaos.seed=N}. The seed decides
 * every crash and every replacement id, keyed by task index and attempt.
 * Thread interleaving is not replayed, and no assertion depends on it.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class RecoveryChaosTest extends RecoveryContextTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryChaosTest.class);

    private static final long DEFAULT_SEED = 20261113L;
    private static final int TASKS = 40;
    private static final int SLOTS = 4;
    private static final int REAPERS = 2;
    private static final Duration DEADLINE = Duration.ofSeconds(45);
    private static final Duration JOIN = Duration.ofSeconds(10);
    private static final long REAP_EVERY_MILLIS = 100;
    private static final long IDLE_MILLIS = 20;
    private static final long POLL_MILLIS = 50;
    private static final long CRASH_HOLD_SECONDS = 55;
    private static final double REUSE_CRASHED_ID = 0.5;
    private static final int CRASH_KEY = 1;
    private static final int REUSE_KEY = 2;
    private static final int WORKER_ID_KEY = 3;

    @Autowired
    private TaskService taskService;

    @Autowired
    private LeaseProperties leaseProperties;

    @Autowired
    private JsonMapper jsonMapper;

    @Autowired
    private JdbcClient jdbcClient;

    private final Object lifecycle = new Object();
    private final CountDownLatch release = new CountDownLatch(1);
    private final List<Slot> slots = new CopyOnWriteArrayList<>();
    private final List<Thread> reaperThreads = new CopyOnWriteArrayList<>();
    private final List<LeaseReaper> reapers = new CopyOnWriteArrayList<>();
    private final List<ManualScheduler> schedulers = new CopyOnWriteArrayList<>();
    private final List<Run> executed = new CopyOnWriteArrayList<>();
    private final Set<Run> crashed = ConcurrentHashMap.newKeySet();
    private final List<Throwable> unexpected = new CopyOnWriteArrayList<>();
    private final AtomicInteger slotNumbers = new AtomicInteger();
    private volatile boolean stopping;
    private long seed;
    private RecordingTaskService recording;

    @AfterEach
    void stopEverything() throws InterruptedException {
        synchronized (lifecycle) {
            stopping = true;
        }
        release.countDown();
        List<Thread> threads = new ArrayList<>(reaperThreads);
        slots.forEach(slot -> threads.add(slot.thread));
        threads.forEach(Thread::interrupt);
        try {
            assertEquals(List.of(), joinAll(threads), "threads still alive after the test");
        } finally {
            reapers.forEach(LeaseReaper::stop);
            schedulers.forEach(ManualScheduler::shutdownNow);
        }
    }

    @Test
    void crashedAttemptsAreRequeuedAndEveryTaskCompletesOnceFromItsLastAttempt() throws Exception {
        seed = Long.getLong("recovery.chaos.seed", DEFAULT_SEED);
        log.info("Recovery chaos: seed={}", seed);
        try {
            run();
        } catch (Throwable ex) {
            String unfinished = unfinishedTasks();
            log.error("Recovery chaos failed: seed={}\n{}", seed, unfinished);
            throw new AssertionError("seed=" + seed + "\nunfinished tasks:\n" + unfinished, ex);
        }
    }

    private void run() throws Exception {
        recording = new RecordingTaskService(taskService);
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < TASKS; i++) {
            ids.add(taskService.createTask("CHAOS", "{\"index\":" + i + "}").id());
        }
        long started = System.nanoTime();
        for (int r = 0; r < REAPERS; r++) {
            startReaper(r);
        }
        for (int s = 0; s < SLOTS; s++) {
            startSlot(keyedId(-1 - s, 0));
        }

        Map<UUID, Integer> seenAttempts = new HashMap<>();
        long deadline = started + DEADLINE.toNanos();
        while (true) {
            List<Sample> samples = sample();
            for (Sample sample : samples) {
                Integer seen = seenAttempts.put(sample.id(), sample.attempt());
                assertTrue(seen == null || sample.attempt() >= seen,
                        "I4: attempt of " + sample.id() + " went from " + seen + " to " + sample.attempt());
            }
            if (samples.stream().allMatch(sample -> sample.status() == TaskStatus.COMPLETED)) {
                break;
            }
            if (System.nanoTime() >= deadline) {
                fail("not every task was COMPLETED within " + DEADLINE);
            }
            Thread.sleep(POLL_MILLIS);
        }
        long completedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        // Crashed handlers stay blocked until the snapshot, so their finishes come after it.
        synchronized (lifecycle) {
            stopping = true;
        }
        long joinDeadline = System.nanoTime() + JOIN.toNanos();
        while (true) {
            List<Thread> live = new ArrayList<>(reaperThreads);
            slots.stream().filter(slot -> !slot.crashed).forEach(slot -> live.add(slot.thread));
            List<Thread> alive = live.stream().filter(Thread::isAlive).toList();
            if (alive.isEmpty()) {
                break;
            }
            if (System.nanoTime() >= joinDeadline) {
                fail("still alive after stop: " + alive.stream().map(Thread::getName).toList());
            }
            alive.getFirst().join(POLL_MILLIS);
        }

        List<String> tasksBefore = allTasks();
        List<String> historyBefore = allHistory();

        release.countDown();
        List<Thread> crashedThreads = slots.stream().filter(slot -> slot.crashed).map(slot -> slot.thread).toList();
        assertEquals(List.of(), joinAll(crashedThreads), "crashed handlers still alive after release");

        assertEquals(tasksBefore, allTasks());
        assertEquals(historyBefore, allHistory());
        assertEquals(List.of(), unexpected, "a slot or reaper thread threw");

        Map<UUID, FinalTask> rows = new HashMap<>();
        for (FinalTask row : finalTasks()) {
            rows.put(row.id(), row);
        }
        Map<UUID, List<StoredAttempt>> history = new HashMap<>();
        for (StoredAttempt attempt : storedAttempts()) {
            history.computeIfAbsent(attempt.taskId(), id -> new ArrayList<>()).add(attempt);
        }

        Map<Run, Integer> crashedFinishes = new HashMap<>();
        for (RecordingTaskService.Finish finish : recording.finishes()) {
            int index = ids.indexOf(finish.attempt().taskId());
            FinalTask row = rows.get(finish.attempt().taskId());
            Run run = new Run(index, finish.attempt().attempt());
            if (crashed.contains(run)) {
                crashedFinishes.merge(run, 1, Integer::sum);
                assertEquals(new TransitionResult.Rejected(TaskStatus.COMPLETED, row.workerId()), finish.result(),
                        "zombie finish " + finish);
            }
            if (run.attempt() != row.attempt()) {
                assertInstanceOf(TransitionResult.Rejected.class, finish.result(), "earlier attempt finish " + finish);
            }
        }
        assertFalse(crashed.isEmpty(), "no handler crashed");
        for (Run run : crashed) {
            assertEquals(1, crashedFinishes.getOrDefault(run, 0), "finishes recorded for crashed " + run);
        }

        for (int i = 0; i < TASKS; i++) {
            UUID id = ids.get(i);
            FinalTask row = rows.get(id);
            List<StoredAttempt> attempts = history.getOrDefault(id, List.of());
            String where = "task " + i + " " + row + " " + attempts;

            List<RecordingTaskService.Finish> applied = recording.finishes(id).stream()
                    .filter(finish -> finish.result() instanceof TransitionResult.Applied)
                    .toList();
            assertEquals(1, applied.size(), "applied finishes of " + where);
            assertEquals(new RecordingTaskService.Attempt(id, row.workerId(), row.attempt()),
                    applied.getFirst().attempt(), "applied finish of " + where);

            int index = i;
            List<Integer> runs = executed.stream()
                    .filter(run -> run.task() == index)
                    .map(Run::attempt)
                    .sorted()
                    .toList();
            assertEquals(IntStream.rangeClosed(1, row.attempt()).boxed().toList(), runs, "executions of " + where);

            for (Run run : crashed) {
                if (run.task() == index) {
                    assertEquals(List.of(AttemptOutcome.LEASE_EXPIRED), attempts.stream()
                                    .filter(attempt -> attempt.attempt() == run.attempt())
                                    .map(StoredAttempt::outcome)
                                    .toList(),
                            "crashed attempt " + run.attempt() + " of " + where);
                }
            }

            assertEquals(TaskStatus.COMPLETED, row.status(), where);
            assertTrue(attempts.stream().allMatch(attempt -> attempt.outcome() != null), "I1: " + where);
            assertEquals(IntStream.rangeClosed(1, row.attempt()).boxed().toList(),
                    attempts.stream().map(StoredAttempt::attempt).toList(), "I2: " + where);
            for (StoredAttempt attempt : attempts.subList(0, attempts.size() - 1)) {
                assertEquals(AttemptOutcome.LEASE_EXPIRED, attempt.outcome(), "I2: " + where);
            }
            assertEquals(row.attempt(), row.resultAttempt(), "I3: " + where);
            assertEquals(row.workerId(), row.resultWorker(), "I3: " + where);
            assertEquals(List.of(attempts.getLast()),
                    attempts.stream().filter(attempt -> attempt.outcome() == AttemptOutcome.COMPLETED).toList(),
                    "I3: " + where);
            assertEquals(row.workerId(), attempts.getLast().workerId(), "I3: " + where);
        }

        log.info("Recovery chaos: seed={} completed in {} ms, crashed attempts={}, executions={}, slots started={}",
                seed, completedMillis, crashed.size(), executed.size(), slots.size());
    }

    private void startReaper(int number) {
        ManualScheduler scheduler = new ManualScheduler();
        schedulers.add(scheduler);
        LeaseReaper reaper = new LeaseReaper(taskService, new ReaperProperties(), scheduler);
        reapers.add(reaper);
        Thread thread = new Thread(() -> {
            while (!stopping) {
                try {
                    reaper.reapOnce();
                } catch (Throwable ex) {
                    unexpected.add(ex);
                }
                try {
                    Thread.sleep(REAP_EVERY_MILLIS);
                } catch (InterruptedException ex) {
                    return;
                }
            }
        }, "chaos-reaper-" + number);
        thread.setDaemon(true);
        reaperThreads.add(thread);
        thread.start();
    }

    /** Starts nothing once stopping, so teardown sees every slot thread. */
    private void startSlot(UUID workerId) {
        synchronized (lifecycle) {
            if (stopping) {
                return;
            }
            Slot slot = new Slot(slotNumbers.getAndIncrement(), workerId);
            slots.add(slot);
            slot.thread.start();
        }
    }

    private boolean crashes(Run run) {
        double probability = switch (run.attempt()) {
            case 1 -> 0.5;
            case 2 -> 0.25;
            default -> 0.0;
        };
        return keyed(run.task(), run.attempt(), CRASH_KEY).nextDouble() < probability;
    }

    private UUID replacementId(UUID crashedId, Run run) {
        if (keyed(run.task(), run.attempt(), REUSE_KEY).nextDouble() < REUSE_CRASHED_ID) {
            return crashedId;
        }
        return keyedId(run.task(), run.attempt());
    }

    private UUID keyedId(int task, int attempt) {
        SplittableRandom random = keyed(task, attempt, WORKER_ID_KEY);
        return new UUID(random.nextLong(), random.nextLong());
    }

    private SplittableRandom keyed(int task, int attempt, int key) {
        return new SplittableRandom(seed
                ^ (task * 0x9E3779B97F4A7C15L)
                ^ (attempt * 0xBF58476D1CE4E5B9L)
                ^ (key * 0x94D049BB133111EBL));
    }

    private List<String> joinAll(List<Thread> threads) throws InterruptedException {
        long deadline = System.nanoTime() + JOIN.toNanos();
        for (Thread thread : threads) {
            long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remaining > 0) {
                thread.join(remaining);
            }
        }
        return threads.stream().filter(Thread::isAlive).map(Thread::getName).toList();
    }

    /** One worker loop with its own executor and a heartbeat that never fires. */
    private final class Slot {

        private final UUID workerId;
        private final TaskExecutor executor;
        private final Thread thread;
        private volatile boolean crashed;

        private Slot(int number, UUID workerId) {
            this.workerId = workerId;
            ManualScheduler neverFires = new ManualScheduler();
            schedulers.add(neverFires);
            this.executor = new TaskExecutor(
                    recording,
                    Map.of("CHAOS", new ChaosHandler(this)),
                    jsonMapper,
                    new LeaseHeartbeat(recording, leaseProperties.getHeartbeatInterval(), neverFires));
            this.thread = new Thread(this::loop, "chaos-slot-" + number);
            thread.setDaemon(true);
        }

        private void loop() {
            try {
                while (!stopping && !crashed) {
                    if (!executor.runOnce(workerId)) {
                        Thread.sleep(IDLE_MILLIS);
                    }
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            } catch (Throwable ex) {
                unexpected.add(ex);
            }
        }
    }

    /**
     * Records each run. A crash marks the slot crashed, starts its
     * replacement, and blocks until released, with no heartbeat.
     */
    private final class ChaosHandler implements TaskHandler {

        private final Slot slot;

        private ChaosHandler(Slot slot) {
            this.slot = slot;
        }

        @Override
        public String type() {
            return "CHAOS";
        }

        @Override
        public TaskOutcome execute(TaskContext ctx) throws InterruptedException {
            Run run = new Run(ctx.payload().get("index").asInt(), ctx.attempt());
            executed.add(run);
            if (crashes(run)) {
                crashed.add(run);
                slot.crashed = true;
                startSlot(replacementId(slot.workerId, run));
                if (!release.await(CRASH_HOLD_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("crashed handler for " + run + " was not released");
                }
            }
            return new TaskOutcome.Succeeded(jsonMapper.createObjectNode()
                    .put("attempt", ctx.attempt())
                    .put("worker", ctx.workerId().toString()));
        }
    }

    /** One execution of a task's attempt, by task index. */
    private record Run(int task, int attempt) {
    }

    private record Sample(UUID id, TaskStatus status, int attempt) {
    }

    private record FinalTask(UUID id, TaskStatus status, int attempt, UUID workerId,
                             Integer resultAttempt, UUID resultWorker) {
    }

    private record StoredAttempt(UUID taskId, int attempt, UUID workerId, AttemptOutcome outcome) {
    }

    private List<Sample> sample() {
        return jdbcClient.sql("SELECT id, status, attempt FROM tasks")
                .query((rs, rowNum) -> new Sample(
                        rs.getObject("id", UUID.class),
                        TaskStatus.valueOf(rs.getString("status")),
                        rs.getInt("attempt")))
                .list();
    }

    private List<FinalTask> finalTasks() {
        return jdbcClient.sql("""
                        SELECT id, status, attempt, worker_id,
                               (result ->> 'attempt')::int AS result_attempt,
                               (result ->> 'worker')::uuid AS result_worker
                        FROM tasks
                        """)
                .query((rs, rowNum) -> new FinalTask(
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

    private List<String> allTasks() {
        return jdbcClient.sql("SELECT t::text FROM tasks t ORDER BY t.id")
                .query(String.class)
                .list();
    }

    private List<String> allHistory() {
        return jdbcClient.sql("SELECT a::text FROM task_attempts a ORDER BY a.task_id, a.attempt")
                .query(String.class)
                .list();
    }

    /** Each task that is not {@code COMPLETED}: its {@code tasks} row, then its {@code task_attempts} rows. */
    private String unfinishedTasks() {
        try {
            List<UUID> ids = jdbcClient.sql("SELECT id FROM tasks WHERE status <> 'COMPLETED' ORDER BY created_at")
                    .query(UUID.class)
                    .list();
            StringBuilder out = new StringBuilder();
            for (UUID id : ids) {
                out.append(jdbcClient.sql("SELECT t::text FROM tasks t WHERE id = :id")
                        .param("id", id).query(String.class).single()).append('\n');
                for (String attempt : jdbcClient.sql(
                                "SELECT a::text FROM task_attempts a WHERE task_id = :id ORDER BY attempt")
                        .param("id", id).query(String.class).list()) {
                    out.append("  ").append(attempt).append('\n');
                }
            }
            return ids.isEmpty() ? "(none)" : out.toString();
        } catch (RuntimeException ex) {
            return "(could not read the tasks: " + ex + ")";
        }
    }
}
