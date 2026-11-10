package com.kushal.workflow.worker;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Records {@code scheduleWithFixedDelay} calls and never runs them. A test
 * fires a tick by hand. A heartbeat built on one that is never fired
 * extends nothing.
 */
final class ManualScheduler extends ScheduledThreadPoolExecutor {

    private final List<Tick> ticks = new CopyOnWriteArrayList<>();

    ManualScheduler() {
        super(1);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command, long initialDelay, long delay, TimeUnit unit) {
        if (isShutdown()) {
            return super.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }
        Tick tick = new Tick(command, initialDelay, delay, unit);
        ticks.add(tick);
        return tick;
    }

    List<Tick> ticks() {
        return List.copyOf(ticks);
    }

    Tick onlyTick() {
        assertEquals(1, ticks.size(), "scheduled ticks");
        return ticks.getFirst();
    }

    static final class Tick implements ScheduledFuture<Object> {

        private final Runnable command;
        private final long initialDelay;
        private final long delay;
        private final TimeUnit unit;
        private volatile boolean cancelled;
        private volatile Boolean mayInterruptIfRunning;

        private Tick(Runnable command, long initialDelay, long delay, TimeUnit unit) {
            this.command = command;
            this.initialDelay = initialDelay;
            this.delay = delay;
            this.unit = unit;
        }

        /**
         * Runs the command even after cancel, the way a tick already in
         * flight on the real thread would.
         */
        void fire() {
            command.run();
        }

        long initialDelay(TimeUnit target) {
            return target.convert(initialDelay, unit);
        }

        long delay(TimeUnit target) {
            return target.convert(delay, unit);
        }

        /** {@code null} until cancelled. */
        Boolean mayInterruptIfRunning() {
            return mayInterruptIfRunning;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (cancelled) {
                return false;
            }
            this.mayInterruptIfRunning = mayInterruptIfRunning;
            cancelled = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return cancelled;
        }

        @Override
        public Object get() {
            if (cancelled) {
                throw new CancellationException();
            }
            throw new UnsupportedOperationException("a periodic tick has no result");
        }

        @Override
        public Object get(long timeout, TimeUnit timeUnit) {
            return get();
        }

        @Override
        public long getDelay(TimeUnit target) {
            return target.convert(initialDelay, unit);
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
        }
    }
}
