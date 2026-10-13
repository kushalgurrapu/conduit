package com.kushal.workflow.worker;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.context.LifecycleProperties;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Status table for {@link WorkerPoolHealthIndicator}. The pool is a
 * fixed state, so the startup gap and the sample between a live-loop
 * decrement and an unexpected-exit increment do not depend on timing.
 */
class WorkerPoolHealthIndicatorTest {

    @Test
    void disabledWorkersAreUp() {
        WorkerProperties properties = properties(4);
        properties.setEnabled(false);
        ScriptedPool pool = new ScriptedPool(properties);
        pool.setState(false, false, 0, 0);

        assertHealth(health(pool, properties), Status.UP, false, false, false, 4, 0, 0);
    }

    @Test
    void enabledPoolThatHasNotStartedIsOutOfService() {
        WorkerProperties properties = properties(4);
        ScriptedPool pool = new ScriptedPool(properties);
        pool.setState(false, false, 0, 0);

        assertHealth(
                health(pool, properties), Status.OUT_OF_SERVICE, true, false, false, 4, 0, 0);
    }

    @Test
    void runningPoolBeforeEveryLoopIsLiveIsOutOfService() {
        WorkerProperties properties = properties(4);
        ScriptedPool pool = new ScriptedPool(properties);
        pool.setState(true, false, 0, 0);
        assertHealth(health(pool, properties), Status.OUT_OF_SERVICE, true, true, false, 4, 0, 0);

        // Same shape as the sample between decrementing liveLoops and
        // incrementing unexpectedExits. That sample is not a failure.
        pool.setState(true, false, 3, 0);
        assertHealth(health(pool, properties), Status.OUT_OF_SERVICE, true, true, false, 4, 3, 0);
    }

    @Test
    void everyConfiguredLoopLiveIsUp() {
        WorkerProperties properties = properties(4);
        ScriptedPool pool = new ScriptedPool(properties);
        pool.setState(true, false, 4, 0);

        assertHealth(health(pool, properties), Status.UP, true, true, false, 4, 4, 0);
    }

    @Test
    void unexpectedExitWhileOtherLoopsAreLiveIsDown() {
        WorkerProperties properties = properties(4);
        ScriptedPool pool = new ScriptedPool(properties);
        pool.setState(true, false, 3, 1);

        assertHealth(health(pool, properties), Status.DOWN, true, true, false, 4, 3, 1);
    }

    @Test
    void everyLoopExitedUnexpectedlyIsDown() {
        WorkerProperties properties = properties(2);
        ScriptedPool pool = new ScriptedPool(properties);
        pool.setState(true, false, 0, 2);

        assertHealth(health(pool, properties), Status.DOWN, true, true, false, 2, 0, 2);
    }

    @Test
    void stoppingWithLiveLoopsIsOutOfService() {
        WorkerProperties properties = properties(4);
        ScriptedPool pool = new ScriptedPool(properties);
        pool.setState(true, true, 4, 0);

        assertHealth(
                health(pool, properties), Status.OUT_OF_SERVICE, true, true, true, 4, 4, 0);
    }

    @Test
    void stoppingAfterAnUnexpectedExitIsOutOfService() {
        WorkerProperties properties = properties(4);
        ScriptedPool pool = new ScriptedPool(properties);
        pool.setState(false, true, 1, 1);

        assertHealth(
                health(pool, properties), Status.OUT_OF_SERVICE, true, false, true, 4, 1, 1);
    }

    @Test
    void stoppedPoolIsOutOfService() {
        WorkerProperties properties = properties(4);
        ScriptedPool pool = new ScriptedPool(properties);
        pool.setState(false, true, 0, 0);

        assertHealth(
                health(pool, properties), Status.OUT_OF_SERVICE, true, false, true, 4, 0, 0);
    }

    @Test
    void stoppedPoolWithALoopStillLiveIsOutOfService() {
        WorkerProperties properties = properties(4);
        ScriptedPool pool = new ScriptedPool(properties);
        pool.setState(false, true, 1, 0);

        assertHealth(
                health(pool, properties), Status.OUT_OF_SERVICE, true, false, true, 4, 1, 0);
    }

    private static Health health(ScriptedPool pool, WorkerProperties properties) {
        return new WorkerPoolHealthIndicator(pool, properties).health();
    }

    private static void assertHealth(
            Health health,
            Status status,
            boolean enabled,
            boolean running,
            boolean stopping,
            int concurrency,
            int liveLoops,
            int unexpectedExits) {
        assertEquals(status, health.getStatus());
        assertEquals(enabled, health.getDetails().get("enabled"));
        assertEquals(running, health.getDetails().get("running"));
        assertEquals(stopping, health.getDetails().get("stopping"));
        assertEquals(concurrency, health.getDetails().get("concurrency"));
        assertEquals(liveLoops, health.getDetails().get("liveLoops"));
        assertEquals(unexpectedExits, health.getDetails().get("unexpectedExits"));
    }

    private static WorkerProperties properties(int concurrency) {
        WorkerProperties properties = new WorkerProperties();
        properties.setEnabled(true);
        properties.setConcurrency(concurrency);
        return properties;
    }

    /**
     * Reports the state set by the test. Does not start threads.
     */
    static final class ScriptedPool extends WorkerPool {

        private boolean running;
        private boolean stopping;
        private int liveLoops;
        private int unexpectedExits;

        private ScriptedPool(WorkerProperties properties) {
            super(
                    null,
                    properties,
                    null,
                    new LifecycleProperties(),
                    new IdleBackoff(properties.getPollInterval(), properties.getMaxIdleInterval()));
        }

        private void setState(boolean running, boolean stopping, int liveLoops, int unexpectedExits) {
            this.running = running;
            this.stopping = stopping;
            this.liveLoops = liveLoops;
            this.unexpectedExits = unexpectedExits;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        boolean isStopping() {
            return stopping;
        }

        @Override
        int liveLoopCount() {
            return liveLoops;
        }

        @Override
        int unexpectedExitCount() {
            return unexpectedExits;
        }
    }
}
