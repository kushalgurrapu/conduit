package com.kushal.workflow.worker;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerPropertiesTest {

    @Test
    void defaultsFitTheShutdownPhase() {
        new WorkerProperties().validate(Duration.ofSeconds(30));
    }

    @Test
    void maxIdleShorterThanPollIsRejected() {
        WorkerProperties properties = new WorkerProperties();
        properties.setPollInterval(Duration.ofSeconds(31));
        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> properties.validate(Duration.ofSeconds(40)));
        assertTrue(ex.getMessage().contains("max-idle-interval"));
    }

    @Test
    void nonPositiveMaxIdleIsRejected() {
        WorkerProperties properties = new WorkerProperties();
        properties.setMaxIdleInterval(Duration.ZERO);
        assertThrows(IllegalStateException.class, () -> properties.validate(Duration.ofSeconds(30)));
    }
}
