package com.kushal.workflow.task;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeasePropertiesTest {

    @Test
    void defaultsAreThirtySecondsAndTenSeconds() {
        LeaseProperties properties = new LeaseProperties();
        properties.validate();
        assertEquals(Duration.ofSeconds(30), properties.getDuration());
        assertEquals(Duration.ofSeconds(10), properties.getHeartbeatInterval());
        assertEquals(30_000_000L, properties.leaseMicros());
    }

    @Test
    void subMicrosecondDurationIsRejected() {
        LeaseProperties properties = new LeaseProperties();
        properties.setDuration(Duration.ofNanos(999));
        IllegalStateException ex = assertThrows(IllegalStateException.class, properties::validate);
        assertTrue(ex.getMessage().contains("duration"));
    }

    @Test
    void nullZeroAndNegativeDurationsAreRejected() {
        assertRejected(properties -> properties.setDuration(null), "duration");
        assertRejected(properties -> properties.setDuration(Duration.ZERO), "duration");
        assertRejected(properties -> properties.setDuration(Duration.ofNanos(-1)), "duration");
        assertRejected(properties -> properties.setHeartbeatInterval(null), "heartbeat-interval");
        assertRejected(properties -> properties.setHeartbeatInterval(Duration.ZERO), "heartbeat-interval");
    }

    @Test
    void intervalNotShorterThanDurationIsRejected() {
        LeaseProperties properties = new LeaseProperties();
        properties.setDuration(Duration.ofSeconds(10));
        properties.setHeartbeatInterval(Duration.ofSeconds(10));
        IllegalStateException ex = assertThrows(IllegalStateException.class, properties::validate);
        assertTrue(ex.getMessage().contains("heartbeat-interval"));
    }

    @Test
    void intervalLongerThanOneThirdStillValidates() {
        LeaseProperties properties = new LeaseProperties();
        properties.setDuration(Duration.ofSeconds(30));
        properties.setHeartbeatInterval(Duration.ofSeconds(20));
        properties.validate();
    }

    @Test
    void leaseMicrosTruncatesToWholeMicroseconds() {
        LeaseProperties properties = new LeaseProperties();
        properties.setDuration(Duration.ofNanos(1_500));
        properties.setHeartbeatInterval(Duration.ofNanos(1_000));
        properties.validate();
        assertEquals(1L, properties.leaseMicros());
    }

    private static void assertRejected(java.util.function.Consumer<LeaseProperties> set, String name) {
        LeaseProperties properties = new LeaseProperties();
        set.accept(properties);
        IllegalStateException ex = assertThrows(IllegalStateException.class, properties::validate);
        assertTrue(ex.getMessage().contains(name));
    }
}
