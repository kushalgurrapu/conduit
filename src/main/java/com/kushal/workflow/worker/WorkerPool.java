package com.kushal.workflow.worker;

import com.kushal.workflow.task.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.context.LifecycleProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionException;

import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.random.RandomGenerator;

/**
 * Pulls work on a fixed number of platform threads.
 *
 * <p>Each loop calls {@link TaskExecutor#runOnce} for one task at a time.
 * The loop checks {@link #stopping} only before that call. A task that has
 * already been claimed is executed even if shutdown has started.
 *
 * <p>An empty claim waits on a latch that {@link #stop()} opens. The
 * wait starts at {@code poll-interval} and doubles while the queue stays
 * empty, up to {@code max-idle-interval}, with equal jitter
 * ({@link IdleBackoff}). A claim clears that count. A database error
 * still waits the fixed {@code error-backoff}.
 *
 * <p>{@link #stop()} runs before singleton destruction closes the
 * datasource. {@code shutdown-timeout} must be shorter than
 * {@code spring.lifecycle.timeout-per-shutdown-phase} so that wait finishes
 * while the phase is still open.
 */
@Component
public class WorkerPool implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

    private final TaskExecutor taskExecutor;
    private final WorkerProperties properties;
    private final TaskRepository taskRepository;
    private final IdleBackoff idleBackoff;

    /**
     * Start and stop share this lock, including the wait inside
     * {@link #stop()}. Worker loops must not acquire it.
     */
    private final Object lifecycleLock = new Object();

    private volatile boolean stopping;
    private volatile boolean running;
    private volatile CountDownLatch stopLatch = new CountDownLatch(1);
    private volatile ExecutorService pool;
    private volatile List<UUID> workerIds = List.of();
    private volatile int loopGeneration;
    private final AtomicInteger liveLoops = new AtomicInteger();
    private final AtomicInteger unexpectedExits = new AtomicInteger();

    @Autowired
    public WorkerPool(
            TaskExecutor taskExecutor,
            WorkerProperties properties,
            TaskRepository taskRepository,
            LifecycleProperties lifecycleProperties) {
        this(
                taskExecutor,
                properties,
                taskRepository,
                lifecycleProperties,
                idleBackoff(properties, lifecycleProperties));
    }

    private static IdleBackoff idleBackoff(
            WorkerProperties properties, LifecycleProperties lifecycleProperties) {
        properties.validate(lifecycleProperties.getTimeoutPerShutdownPhase());
        return new IdleBackoff(properties.getPollInterval(), properties.getMaxIdleInterval());
    }

    /**
     * Tests pass their own {@link IdleBackoff} to record miss counts.
     * The public constructor builds one from {@code engine.worker.*}.
     */
    WorkerPool(
            TaskExecutor taskExecutor,
            WorkerProperties properties,
            TaskRepository taskRepository,
            LifecycleProperties lifecycleProperties,
            IdleBackoff idleBackoff) {
        this.taskExecutor = taskExecutor;
        this.properties = properties;
        this.taskRepository = taskRepository;
        this.idleBackoff = idleBackoff;
        properties.validate(lifecycleProperties.getTimeoutPerShutdownPhase());
    }

    /**
     * Opens a new set of loops. Does nothing when workers are disabled,
     * which is how the shared test application stays idle.
     */
    @Override
    public void start() {
        synchronized (lifecycleLock) {
            if (running || !properties.isEnabled()) {
                return;
            }
            stopping = false;
            stopLatch = new CountDownLatch(1);
            int generation = ++loopGeneration;
            liveLoops.set(0);
            unexpectedExits.set(0);
            int concurrency = properties.getConcurrency();
            String namePrefix = hostName() + "-" + ProcessHandle.current().pid();
            List<UUID> ids = new ArrayList<>(concurrency);
            ExecutorService created = newPool(concurrency);
            try {
                for (int i = 0; i < concurrency; i++) {
                    UUID workerId = UUID.randomUUID();
                    ids.add(workerId);
                    String threadName = namePrefix + "-loop" + i;
                    created.execute(new Loop(workerId, threadName, generation));
                }
            } catch (RuntimeException ex) {
                stopping = true;
                stopLatch.countDown();
                created.shutdownNow();
                throw ex;
            }
            this.workerIds = List.copyOf(ids);
            this.pool = created;
            this.running = true;
            log.info("Worker pool started with {} loops ({})", concurrency, namePrefix);
        }
    }

    /**
     * Stops claiming, waits for in-flight work, then interrupts whatever
     * is still inside a handler. Threads that ignore the interrupt can
     * finish after this method returns. The log says they were still
     * running at the deadline.
     */
    @Override
    public void stop() {
        synchronized (lifecycleLock) {
            if (!running) {
                return;
            }
            stopping = true;
            stopLatch.countDown();
            ExecutorService stoppingPool = this.pool;
            List<UUID> ids = this.workerIds;
            running = false;
            stoppingPool.shutdown();
            try {
                stoppingPool.awaitTermination(
                        properties.getShutdownTimeout().toNanos(), TimeUnit.NANOSECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            stoppingPool.shutdownNow();
            logStillRunning(ids);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Package-private so a test can see that shutdown has begun. */
    boolean isStopping() {
        return stopping;
    }

    /** Loops inside {@code run}, including those waiting on an empty queue. */
    int liveLoopCount() {
        return liveLoops.get();
    }

    /**
     * Loops that left {@code run} while this pool was still started.
     * Shutdown exits are not counted.
     */
    int unexpectedExitCount() {
        return unexpectedExits.get();
    }

    @Override
    public boolean isAutoStartup() {
        return properties.isEnabled();
    }

    /**
     * Lifecycle stop runs before the context destroys singletons, so this
     * pool finishes with the datasource still open.
     */
    @Override
    public int getPhase() {
        return SmartLifecycle.DEFAULT_PHASE;
    }

    private ExecutorService newPool(int concurrency) {
        return Executors.newFixedThreadPool(concurrency, runnable -> {
            // Platform thread. A virtual thread is not used: the loop is
            // one worker id for the life of one thread.
            Thread thread = new Thread(runnable);
            thread.setDaemon(false);
            thread.setUncaughtExceptionHandler((failed, error) ->
                    log.error("Worker {} died", failed.getName(), error));
            return thread;
        });
    }

    private void logStillRunning(Collection<UUID> ids) {
        try {
            List<UUID> stillRunning = taskRepository.findRunningIdsForWorkers(ids);
            if (!stillRunning.isEmpty()) {
                log.warn(
                        "Tasks still running at shutdown deadline: {}. A late finish is still possible",
                        stillRunning);
            }
        } catch (RuntimeException ex) {
            log.warn("Could not list tasks still running at shutdown deadline", ex);
        }
    }

    private void pause(Duration duration) throws InterruptedException {
        stopLatch.await(duration.toNanos(), TimeUnit.NANOSECONDS);
    }

    private static String hostName() {
        try {
            String host = InetAddress.getLocalHost().getHostName();
            if (host == null || host.isBlank()) {
                return "unknown";
            }
            return host;
        } catch (Exception ex) {
            return "unknown";
        }
    }

    /**
     * One thread, one worker id. Not a public type: callers use the pool.
     */
    private final class Loop implements Runnable {

        private final UUID workerId;
        private final String threadName;
        private final int generation;
        private final RandomGenerator random = RandomGenerator.of("L32X64MixRandom");
        private int consecutiveMisses;

        private Loop(UUID workerId, String threadName, int generation) {
            this.workerId = workerId;
            this.threadName = threadName;
            this.generation = generation;
        }

        @Override
        public void run() {
            Thread.currentThread().setName(threadName);
            log.info("Worker loop {} started", workerId);
            try {
                liveLoops.incrementAndGet();
                // stopping is read here, then runOnce claims and executes
                // without looking at it again. A later start() bumps
                // loopGeneration so a handler that outlived the previous
                // stop does not claim on the new generation.
                while (!stopping
                        && generation == loopGeneration
                        && !Thread.currentThread().isInterrupted()) {
                    try {
                        if (!taskExecutor.runOnce(workerId)) {
                            consecutiveMisses++;
                            pause(idleBackoff.delayAfterMiss(consecutiveMisses, random));
                        } else {
                            consecutiveMisses = 0;
                        }
                    } catch (InterruptedException ex) {
                        break;
                    } catch (DataAccessException | TransactionException ex) {
                        log.warn(
                                "Worker {} could not reach the database; backing off {}",
                                workerId,
                                properties.getErrorBackoff(),
                                ex);
                        try {
                            pause(properties.getErrorBackoff());
                        } catch (InterruptedException interrupted) {
                            break;
                        }
                    } catch (Exception ex) {
                        log.warn("Worker {} loop error", workerId, ex);
                    }
                    // An Error leaves this method. That loop is dead.
                }
            } finally {
                if (generation == loopGeneration) {
                    liveLoops.decrementAndGet();
                    if (!stopping) {
                        unexpectedExits.incrementAndGet();
                        log.warn("Worker loop {} exited while the pool was still running", workerId);
                    }
                }
                log.info("Worker loop {} stopped", workerId);
            }
        }
    }
}
