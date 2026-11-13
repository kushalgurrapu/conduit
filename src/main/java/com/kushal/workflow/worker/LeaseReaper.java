package com.kushal.workflow.worker;

import com.kushal.workflow.task.ReapedTask;
import com.kushal.workflow.task.TaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Returns {@code RUNNING} rows whose lease has expired to {@code PENDING}
 * from one scheduler thread.
 *
 * <p>A tick is one {@link #reapOnce()} call, {@code interval} after the
 * previous tick ended. What escapes it decides what happens next:
 *
 * <ul>
 *   <li>An {@link Exception} is logged. The next tick tries again.</li>
 *   <li>A {@link VirtualMachineError} is logged and rethrown. The executor
 *       runs no later tick, so reaping stops in this process.</li>
 *   <li>Any other {@link Error} is logged, and the schedule survives. The
 *       executor would drop a rethrown error without reporting it, and
 *       recovery would stop for good.</li>
 * </ul>
 *
 * <p>Each batch is its own transaction through the {@link TaskService}
 * proxy. This class must not be {@code @Transactional} or call the reap
 * inside a transaction: the reap's timeout applies only when it starts
 * its own.
 *
 * <p>Phase {@code 0} starts after {@link LeaseHeartbeat} and before
 * {@link WorkerPool}, and stops in the reverse order. Not restartable: a
 * {@code start()} after {@link #stop()} logs a warning and does nothing.
 * It stays pauseable, so a paused test context does not reap other
 * classes' fixtures.
 */
@Component
public class LeaseReaper implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    private static final Duration STOP_AWAIT = Duration.ofSeconds(2);

    private final TaskService taskService;
    private final boolean enabled;
    private final Duration interval;
    private final int batchSize;
    private final ScheduledExecutorService scheduler;
    private final Object lifecycleLock = new Object();
    private volatile boolean running;
    private volatile boolean stopping;

    @Autowired
    public LeaseReaper(TaskService taskService, ReaperProperties properties) {
        this(taskService, properties, newScheduler());
    }

    /** Tests pass a scheduler they fire by hand. */
    LeaseReaper(TaskService taskService, ReaperProperties properties, ScheduledExecutorService scheduler) {
        properties.validate();
        this.taskService = taskService;
        this.enabled = properties.isEnabled();
        this.interval = properties.getInterval();
        this.batchSize = properties.getBatchSize();
        this.scheduler = scheduler;
    }

    /** Starts no thread until something is scheduled, so a disabled reaper holds none. */
    private static ScheduledExecutorService newScheduler() {
        return new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "lease-reaper");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Reaps batches until one comes back short, {@link #stop()} has been
     * called, or this thread is interrupted. Checks the stopping flag, not
     * {@link #isRunning()}, so it also runs on a bean that was never started.
     *
     * @return the number of tasks requeued
     */
    int reapOnce() {
        int total = 0;
        while (!stopping && !Thread.currentThread().isInterrupted()) {
            List<ReapedTask> batch = taskService.reapExpiredLeases(batchSize);
            if (!batch.isEmpty()) {
                log.info("Requeued {} tasks whose lease expired", batch.size());
            }
            for (ReapedTask task : batch) {
                log.debug("Requeued task {} attempt {} from worker {}",
                        task.taskId(), task.attempt(), task.previousWorkerId());
                if (!task.attemptRecorded()) {
                    log.warn("Attempt {} of task {} for worker {} was requeued, but its history row was already closed",
                            task.attempt(), task.taskId(), task.previousWorkerId());
                }
            }
            total += batch.size();
            if (batch.size() < batchSize) {
                break;
            }
        }
        return total;
    }

    private void tick() {
        try {
            reapOnce();
        } catch (Exception ex) {
            log.warn("Lease reap failed; trying again in {}", interval, ex);
        } catch (VirtualMachineError error) {
            log.error("Lease reap failed; lease reaping has stopped in this process until restart", error);
            throw error;
        } catch (Error error) {
            log.error("Lease reap failed with an error; trying again in {}", interval, error);
        }
    }

    /** Does nothing when disabled or already running. After {@link #stop()}, logs a warning instead. */
    @Override
    public void start() {
        synchronized (lifecycleLock) {
            if (!enabled || running) {
                return;
            }
            if (stopping) {
                log.warn("Lease reaper was stopped and does not restart; nothing is reaped in this process");
                return;
            }
            long nanos = interval.toNanos();
            scheduler.scheduleWithFixedDelay(this::tick, nanos, nanos, TimeUnit.NANOSECONDS);
            running = true;
            log.info("Lease reaper started: every {}, batches of up to {}", interval, batchSize);
        }
    }

    /**
     * A reap in progress finishes its current batch, then gets a short
     * wait. Then the thread is interrupted. Every batch commits or rolls
     * back as a whole.
     */
    @Override
    public void stop() {
        synchronized (lifecycleLock) {
            stopping = true;
            running = false;
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(STOP_AWAIT.toNanos(), TimeUnit.NANOSECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            scheduler.shutdownNow();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return 0;
    }
}
