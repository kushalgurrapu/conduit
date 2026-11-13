package com.kushal.workflow.worker;

import com.kushal.workflow.task.LeaseProperties;
import com.kushal.workflow.task.TaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Extends the lease of every in-flight task from one scheduler thread.
 *
 * <p>{@link #start(TaskContext)} schedules one fixed-delay tick per task.
 * The first tick runs one {@code heartbeat-interval} after the handler
 * starts. A tick is one {@link TaskService#extendLease} call. A fixed
 * delay does not catch up in a burst after a slow tick.
 *
 * <ul>
 *   <li>{@code false} while the watch is open marks the context's lease
 *       lost and closes the watch so later ticks do not run. The handler
 *       decides whether to stop.</li>
 *   <li>A thrown {@link Exception} is logged. The next tick tries again;
 *       there is no retry inside a tick, and an error is not lease loss.</li>
 *   <li>An {@link Error} is logged and rethrown. The executor does not
 *       reschedule that tick, so that one watch stops. The scheduler
 *       thread stays up.</li>
 *   <li>A tick that finds the watch closed, before or after the update,
 *       does not mark the lease lost. A finish that commits before an
 *       in-flight extend makes that extend match nothing.</li>
 * </ul>
 *
 * <p>Nothing here interrupts a handler. {@link Watch#close()} is
 * {@code cancel(false)}, so it never interrupts the shared thread either.
 * One slow JDBC call delays every other task's tick.
 *
 * <p>Phase {@code -1} stops after {@link WorkerPool}, so leases keep moving
 * while the pool waits for in-flight handlers. Starts even when workers
 * are disabled: tests call {@link TaskExecutor#runOnce} with the pool off.
 */
@Component
public class LeaseHeartbeat implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(LeaseHeartbeat.class);

    private static final Duration STOP_AWAIT = Duration.ofSeconds(2);

    private final TaskService taskService;
    private final Duration interval;
    private final ScheduledExecutorService scheduler;
    private volatile boolean running;

    @Autowired
    public LeaseHeartbeat(TaskService taskService, LeaseProperties leaseProperties) {
        this(taskService, validatedInterval(leaseProperties), newScheduler());
    }

    /** Tests pass a scheduler they fire by hand. */
    LeaseHeartbeat(TaskService taskService, Duration interval, ScheduledExecutorService scheduler) {
        this.taskService = taskService;
        this.interval = interval;
        this.scheduler = scheduler;
    }

    private static Duration validatedInterval(LeaseProperties leaseProperties) {
        leaseProperties.validate();
        return leaseProperties.getHeartbeatInterval();
    }

    private static ScheduledExecutorService newScheduler() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "lease-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    /**
     * Starts extending the lease for {@code context}. The caller closes the
     * returned watch on every exit from the handler, before the finish write.
     * After this bean has stopped, the watch is already closed and the
     * handler runs without heartbeats.
     */
    public Watch start(TaskContext context) {
        Watch watch = new Watch(context);
        long nanos = interval.toNanos();
        try {
            watch.future = scheduler.scheduleWithFixedDelay(
                    () -> tick(watch), nanos, nanos, TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException ex) {
            watch.close();
            log.warn("Heartbeat scheduler is stopped; task {} attempt {} runs without lease extension",
                    context.taskId(), context.attempt());
        }
        return watch;
    }

    private void tick(Watch watch) {
        if (!watch.isOpen()) {
            return;
        }
        TaskContext context = watch.context;
        boolean extended;
        try {
            extended = taskService.extendLease(context.taskId(), context.workerId(), context.attempt());
        } catch (Exception ex) {
            log.warn("Heartbeat for task {} attempt {} failed; trying again in {}",
                    context.taskId(), context.attempt(), interval, ex);
            return;
        } catch (Error error) {
            log.error("Heartbeat for task {} attempt {} stopped",
                    context.taskId(), context.attempt(), error);
            throw error;
        }
        if (!extended && watch.isOpen()) {
            context.markLeaseLost();
            watch.close();
            log.warn("Lease for task {} attempt {} was not extended; the handler is asked to stop",
                    context.taskId(), context.attempt());
        }
    }

    @Override
    public void start() {
        running = true;
    }

    /**
     * Ticks already running get a short wait. Then the thread is
     * interrupted, which can abort an in-flight extend. That is logged
     * like any other failed tick.
     */
    @Override
    public void stop() {
        running = false;
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(STOP_AWAIT.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        scheduler.shutdownNow();
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
        return -1;
    }

    /**
     * Not stopped on a context pause: {@code stop()} is permanent, so a resumed
     * context would run handlers without heartbeats, and an idle daemon thread
     * costs nothing.
     */
    @Override
    public boolean isPauseable() {
        return false;
    }

    /**
     * One task's scheduled tick. Closing it is idempotent.
     */
    public static final class Watch {

        private final TaskContext context;
        private volatile boolean open = true;
        private volatile ScheduledFuture<?> future;

        private Watch(TaskContext context) {
            this.context = context;
        }

        boolean isOpen() {
            return open;
        }

        /** Stops later ticks. Does not interrupt a tick that is running. */
        public void close() {
            open = false;
            ScheduledFuture<?> scheduled = future;
            if (scheduled != null) {
                scheduled.cancel(false);
            }
        }
    }
}
