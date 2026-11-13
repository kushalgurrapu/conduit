package com.kushal.workflow.worker;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReaperPropertiesTest {

    @Test
    void defaultsAreEnabledTenSecondsAndOneHundred() {
        ReaperProperties properties = new ReaperProperties();
        properties.validate();
        assertTrue(properties.isEnabled());
        assertEquals(Duration.ofSeconds(10), properties.getInterval());
        assertEquals(100, properties.getBatchSize());
    }

    @Test
    void nullZeroNegativeAndSubFloorIntervalsAreRejected() {
        assertRejected(properties -> properties.setInterval(null), "engine.reaper.interval");
        assertRejected(properties -> properties.setInterval(Duration.ZERO), "engine.reaper.interval");
        assertRejected(properties -> properties.setInterval(Duration.ofMillis(-1)), "engine.reaper.interval");
        assertRejected(properties -> properties.setInterval(Duration.ofMillis(99)), "engine.reaper.interval");
    }

    @Test
    void intervalOfOneHundredMillisecondsIsAccepted() {
        ReaperProperties properties = new ReaperProperties();
        properties.setInterval(Duration.ofMillis(100));
        properties.validate();
    }

    @Test
    void batchSizeOutsideOneToOneThousandIsRejected() {
        assertRejected(properties -> properties.setBatchSize(0), "engine.reaper.batch-size");
        assertRejected(properties -> properties.setBatchSize(-1), "engine.reaper.batch-size");
        assertRejected(properties -> properties.setBatchSize(1001), "engine.reaper.batch-size");
    }

    @Test
    void batchSizesOfOneAndOneThousandAreAccepted() {
        for (int batchSize : new int[] {1, 1000}) {
            ReaperProperties properties = new ReaperProperties();
            properties.setBatchSize(batchSize);
            properties.validate();
        }
    }

    @Test
    void validationDoesNotDependOnEnabled() {
        assertRejected(properties -> {
            properties.setEnabled(false);
            properties.setBatchSize(0);
        }, "engine.reaper.batch-size");
    }

    @Test
    void batchSizeBindsFromItsProperty() {
        Binder binder = new Binder(new MapConfigurationPropertySource(Map.of(
                "engine.reaper.batch-size", "250",
                "engine.reaper.interval", "250ms",
                "engine.reaper.enabled", "false")));

        ReaperProperties properties = binder.bind("engine.reaper", ReaperProperties.class).get();

        assertEquals(250, properties.getBatchSize());
        assertEquals(Duration.ofMillis(250), properties.getInterval());
        assertFalse(properties.isEnabled());
        properties.validate();
    }

    private static void assertRejected(Consumer<ReaperProperties> set, String name) {
        ReaperProperties properties = new ReaperProperties();
        set.accept(properties);
        IllegalStateException ex = assertThrows(IllegalStateException.class, properties::validate);
        assertTrue(ex.getMessage().contains(name), ex.getMessage());
    }
}
