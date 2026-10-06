package com.kushal.workflow.worker;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Startup rules for the handler index, and the sleep cap. Neither needs
 * a database: a bad index must fail before a task is claimed, and the
 * cap must be visible without sleeping for a minute.
 */
class HandlerConfigurationTest {

    private static final JsonMapper JSON = JsonMapper.shared();

    @Test
    void indexesBuiltInHandlersByType() {
        Map<String, TaskHandler> indexed = HandlerConfiguration.index(List.of(
                new NoopHandler(),
                new EchoHandler(),
                new FailHandler(),
                new SleepHandler()));

        assertEquals(Set.of("NOOP", "ECHO", "FAIL", "SLEEP"), indexed.keySet());
    }

    @Test
    void duplicateTypeFailsStartup() {
        TaskHandler first = handler("NOOP");
        TaskHandler second = handler("NOOP");

        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> HandlerConfiguration.index(List.of(first, second)));

        assertTrue(ex.getMessage().contains("Duplicate task handler for type NOOP"));
    }

    @Test
    void typeThatCannotBeStoredFailsStartup() {
        assertThrows(IllegalStateException.class, () -> HandlerConfiguration.index(List.of(handler("noop"))));
        assertThrows(IllegalStateException.class, () -> HandlerConfiguration.index(List.of(handler("A-B"))));
        assertThrows(IllegalStateException.class, () -> HandlerConfiguration.index(List.of(handler(null))));
    }

    @Test
    void sleepMillisAreCapped() {
        assertEquals(0L, SleepHandler.boundedMillis(json("{}")));
        assertEquals(0L, SleepHandler.boundedMillis(json("{\"millis\":\"10\"}")));
        assertEquals(0L, SleepHandler.boundedMillis(json("{\"millis\":-5}")));
        assertEquals(25L, SleepHandler.boundedMillis(json("{\"millis\":25}")));
        assertEquals(SleepHandler.MAX_MILLIS, SleepHandler.boundedMillis(json("{\"millis\":60000}")));
        assertEquals(SleepHandler.MAX_MILLIS, SleepHandler.boundedMillis(json("{\"millis\":60001}")));
        assertEquals(SleepHandler.MAX_MILLIS, SleepHandler.boundedMillis(json("{\"millis\":1.0e20}")));
    }

    @Test
    void failReasonComesFromThePayload() {
        assertEquals("nope", FailHandler.reason(json("{\"reason\":\"nope\"}")));
        assertEquals(FailHandler.DEFAULT_REASON, FailHandler.reason(json("{}")));
        assertEquals(FailHandler.DEFAULT_REASON, FailHandler.reason(json("{\"reason\":\"  \"}")));
    }

    private static TaskHandler handler(String type) {
        return new TaskHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public TaskOutcome execute(TaskContext ctx) {
                return new TaskOutcome.Succeeded(null);
            }
        };
    }

    private static JsonNode json(String text) {
        return JSON.readTree(text);
    }
}
