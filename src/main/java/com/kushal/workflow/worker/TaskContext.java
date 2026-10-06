package com.kushal.workflow.worker;

import tools.jackson.databind.JsonNode;

import java.util.UUID;

/**
 * The claim a handler is executing.
 *
 * <p>When M3 adds {@code attempt}, this record copies it from the claimed
 * task. Only the claim statement increments that counter.
 */
public record TaskContext(
        UUID taskId,
        String taskType,
        JsonNode payload,
        UUID workerId
) {
}
