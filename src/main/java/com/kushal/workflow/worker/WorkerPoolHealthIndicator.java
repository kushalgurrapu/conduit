package com.kushal.workflow.worker;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * {@code workerPool} component of {@code /actuator/health}.
 *
 * <p>Disabled workers are {@code UP}: tests and a future API-only
 * process turn the pool off on purpose. A pool that is stopped or
 * stopping is {@code OUT_OF_SERVICE}, including a normal shutdown. A
 * started pool whose loops have not all entered {@code run} is also
 * {@code OUT_OF_SERVICE}. {@code start} sets {@code running} after the
 * loops are submitted, and each loop increments {@code liveLoops} when
 * it enters {@code run}. That gap is not a failure.
 *
 * <p>A loop that leaves {@code run} while the pool is still started is
 * {@code DOWN}. An {@code Error} does that. The other loops keep
 * pulling, and nothing starts a replacement. The exit count is
 * incremented after {@code liveLoops} is decremented, so one sample
 * can see the shortfall with no exit yet and report
 * {@code OUT_OF_SERVICE}. The next sample is {@code DOWN}.
 *
 * <p>{@code liveLoops} equal to {@code concurrency} means every
 * configured loop is inside {@code run}, including a loop waiting on
 * an empty queue or backing off after a database error. That is
 * worker-loop readiness only. It does not mean a handler is making
 * progress, that the database is reachable, or that tasks are being
 * processed.
 */
@Component
public class WorkerPoolHealthIndicator implements HealthIndicator {

    private final WorkerPool workerPool;
    private final WorkerProperties properties;

    public WorkerPoolHealthIndicator(WorkerPool workerPool, WorkerProperties properties) {
        this.workerPool = workerPool;
        this.properties = properties;
    }

    @Override
    public Health health() {
        boolean enabled = properties.isEnabled();
        boolean running = workerPool.isRunning();
        boolean stopping = workerPool.isStopping();
        int liveLoops = workerPool.liveLoopCount();
        int unexpectedExits = workerPool.unexpectedExitCount();

        Health.Builder builder;
        if (!enabled) {
            builder = Health.up();
        } else if (stopping || !running) {
            builder = Health.outOfService();
        } else if (unexpectedExits > 0) {
            builder = Health.down();
        } else if (liveLoops < properties.getConcurrency()) {
            builder = Health.outOfService();
        } else {
            builder = Health.up();
        }
        return builder
                .withDetail("enabled", enabled)
                .withDetail("running", running)
                .withDetail("stopping", stopping)
                .withDetail("concurrency", properties.getConcurrency())
                .withDetail("liveLoops", liveLoops)
                .withDetail("unexpectedExits", unexpectedExits)
                .build();
    }
}
