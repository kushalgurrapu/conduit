package com.kushal.workflow.worker;

import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/**
 * Sleeps, then completes. The payload field {@code millis} is how long to
 * sleep. Missing, non-numeric, and negative values sleep for zero.
 *
 * <p>The duration is capped at {@link #MAX_MILLIS}. That cap only limits
 * this handler. It is not a timeout: the task is not failed when the cap
 * is hit, and a handler that ignores interrupts still cannot be killed.
 * {@link Thread#sleep(long)} throws {@link InterruptedException}, which
 * leaves the task {@code RUNNING}.
 */
@Component
public class SleepHandler implements TaskHandler {

    /** One minute. Long enough for a demo, short of occupying a loop forever. */
    public static final long MAX_MILLIS = 60_000L;

    @Override
    public String type() {
        return "SLEEP";
    }

    @Override
    public TaskOutcome execute(TaskContext ctx) throws InterruptedException {
        Thread.sleep(boundedMillis(ctx.payload()));
        return new TaskOutcome.Succeeded(null);
    }

    static long boundedMillis(JsonNode payload) {
        if (payload == null || payload.isNull() || !payload.isObject()) {
            return 0L;
        }
        JsonNode millis = payload.get("millis");
        if (millis == null || !millis.isNumber()) {
            return 0L;
        }
        double requested = millis.asDouble();
        if (Double.isNaN(requested) || requested <= 0d) {
            return 0L;
        }
        if (requested >= MAX_MILLIS) {
            return MAX_MILLIS;
        }
        return (long) requested;
    }
}
