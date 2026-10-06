package com.kushal.workflow.worker;

import tools.jackson.databind.JsonNode;

/**
 * What a handler decided. {@link com.kushal.workflow.worker.TaskExecutor}
 * writes it once. A null return is treated as
 * {@code Failed("handler returned null")}.
 */
public sealed interface TaskOutcome permits TaskOutcome.Succeeded, TaskOutcome.Failed {

    /**
     * Complete the task. A null {@code result} is stored as JSON null.
     */
    record Succeeded(JsonNode result) implements TaskOutcome {
    }

    /**
     * Fail the task. The service strips NUL bytes and the database stores
     * at most 4000 characters.
     */
    record Failed(String reason) implements TaskOutcome {
    }
}
