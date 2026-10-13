package com.kushal.workflow.worker;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.random.RandomGenerator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdleBackoffTest {

    private static final Duration BASE = Duration.ofMillis(100);
    private static final Duration CAP = Duration.ofMillis(800);

    @Test
    void delayDoublesUntilTheCap() {
        IdleBackoff backoff = new IdleBackoff(BASE, CAP);
        RandomGenerator ceiling = end(true);
        assertEquals(Duration.ofMillis(100), backoff.delayAfterMiss(1, ceiling));
        assertEquals(Duration.ofMillis(200), backoff.delayAfterMiss(2, ceiling));
        assertEquals(Duration.ofMillis(400), backoff.delayAfterMiss(3, ceiling));
        assertEquals(Duration.ofMillis(800), backoff.delayAfterMiss(4, ceiling));
        assertEquals(Duration.ofMillis(800), backoff.delayAfterMiss(5, ceiling));
        assertEquals(Duration.ofMillis(800), backoff.delayAfterMiss(10_000, ceiling));
    }

    @Test
    void floorIsHalfTheCappedDelay() {
        IdleBackoff backoff = new IdleBackoff(BASE, CAP);
        RandomGenerator floor = end(false);
        assertEquals(Duration.ofMillis(50), backoff.delayAfterMiss(1, floor));
        assertEquals(Duration.ofMillis(100), backoff.delayAfterMiss(2, floor));
        assertEquals(Duration.ofMillis(200), backoff.delayAfterMiss(3, floor));
        assertEquals(Duration.ofMillis(400), backoff.delayAfterMiss(4, floor));
        assertEquals(Duration.ofMillis(400), backoff.delayAfterMiss(10_000, floor));
    }

    @Test
    void samplesStayInsideTheEqualJitterRangeAndVary() {
        IdleBackoff backoff = new IdleBackoff(BASE, CAP);
        RandomGenerator random = new Random(42);
        Set<Long> seen = new HashSet<>();
        long floor = BASE.toNanos() / 2;
        long cap = BASE.toNanos();
        for (int i = 0; i < 40; i++) {
            long nanos = backoff.delayAfterMiss(1, random).toNanos();
            assertTrue(nanos >= floor && nanos <= cap, nanos + " outside [" + floor + ", " + cap + "]");
            seen.add(nanos);
        }
        assertTrue(seen.size() > 1, "jitter returned a single duration");
    }

    @Test
    void oddDelayStillReachesBothEnds() {
        Duration base = Duration.ofNanos(101);
        IdleBackoff backoff = new IdleBackoff(base, base);
        assertEquals(Duration.ofNanos(50), backoff.delayAfterMiss(1, end(false)));
        assertEquals(base, backoff.delayAfterMiss(1, end(true)));
    }

    @Test
    void rejectsANonPositiveMissAndACapBelowTheBase() {
        IdleBackoff backoff = new IdleBackoff(BASE, CAP);
        assertThrows(IllegalArgumentException.class, () -> backoff.delayAfterMiss(0, end(false)));
        assertThrows(IllegalArgumentException.class, () -> backoff.delayAfterMiss(1, null));
        assertThrows(IllegalArgumentException.class, () -> new IdleBackoff(CAP, BASE));
        assertThrows(IllegalArgumentException.class, () -> new IdleBackoff(Duration.ZERO, CAP));
    }

    /** {@code nextLong(bound)} is either {@code 0} or {@code bound - 1}. */
    private static RandomGenerator end(boolean ceiling) {
        return new Random() {
            @Override
            public long nextLong(long bound) {
                return ceiling ? bound - 1 : 0;
            }
        };
    }
}
