package com.kushal.workflow.worker;

import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/**
 * Fails the task. A textual {@code reason} field in the payload is stored
 * as the error. Otherwise the error is {@code "failed"}.
 */
@Component
public class FailHandler implements TaskHandler {

    static final String DEFAULT_REASON = "failed";

    @Override
    public String type() {
        return "FAIL";
    }

    @Override
    public TaskOutcome execute(TaskContext ctx) {
        return new TaskOutcome.Failed(reason(ctx.payload()));
    }

    static String reason(JsonNode payload) {
        if (payload == null || payload.isNull() || !payload.isObject()) {
            return DEFAULT_REASON;
        }
        JsonNode reason = payload.get("reason");
        if (reason == null || !reason.isString() || reason.stringValue().isBlank()) {
            return DEFAULT_REASON;
        }
        return reason.stringValue();
    }
}
