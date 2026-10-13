package com.kushal.workflow.worker;

import java.time.Duration;
import java.util.random.RandomGenerator;

/**
 * How long a loop waits after an empty claim.
 *
 * <p>The delay doubles with each consecutive miss, starting at
 * {@code poll-interval} and stopping at {@code max-idle-interval}.
 * Equal jitter then picks a value from half of that delay through the
 * whole delay. Half is the floor, so a random draw cannot turn the
 * wait into a tight loop. The ceiling is the delay itself, so loops
 * that missed together do not wake on the same tick.
 *
 * <p>The miss count lives on the loop. This type only turns a count
 * into a wait. A claim resets the count; the next empty claim is a
 * short wait again.
 */
class IdleBackoff {

    private final long baseNanos;
    private final long capNanos;

    IdleBackoff(Duration base, Duration cap) {
        if (base == null || cap == null
                || base.isZero() || base.isNegative()
                || cap.isZero() || cap.isNegative()
                || cap.compareTo(base) < 0) {
            throw new IllegalArgumentException(
                    "max idle interval must be at least the poll interval, and both must be positive");
        }
        this.baseNanos = base.toNanos();
        this.capNanos = cap.toNanos();
    }

    /**
     * @param consecutiveMisses how many empty claims in a row, starting at 1
     */
    Duration delayAfterMiss(int consecutiveMisses, RandomGenerator random) {
        if (consecutiveMisses < 1) {
            throw new IllegalArgumentException("consecutiveMisses must be at least 1");
        }
        if (random == null) {
            throw new IllegalArgumentException("random must not be null");
        }
        long capped = cappedNanos(consecutiveMisses);
        long floor = capped / 2;
        long spread = capped - floor;
        long extra = spread == 0 ? 0 : random.nextLong(spread + 1);
        return Duration.ofNanos(floor + extra);
    }

    private long cappedNanos(int consecutiveMisses) {
        long delay = baseNanos;
        for (int i = 1; i < consecutiveMisses; i++) {
            if (delay >= capNanos || delay > Long.MAX_VALUE / 2) {
                return capNanos;
            }
            delay *= 2;
            if (delay > capNanos) {
                return capNanos;
            }
        }
        return delay;
    }
}
