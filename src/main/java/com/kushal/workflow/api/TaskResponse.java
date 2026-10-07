package com.kushal.workflow.api;

import com.kushal.workflow.task.Task;
import com.kushal.workflow.task.TaskStatus;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.UUID;

/**
 * The task a client is allowed to see. This is not the {@link Task} row:
 * {@code payload} and {@code result} are JSON values here, and text at
 * the repository boundary.
 */
public record TaskResponse(
        UUID id,
        String taskType,
        TaskStatus status,
        JsonNode payload,
        JsonNode result,
        String error,
        Instant createdAt,
        Instant updatedAt,
        Instant claimedAt,
        Instant finishedAt,
        UUID workerId
) {

    static TaskResponse from(Task task, JsonMapper jsonMapper) {
        return new TaskResponse(
                task.id(),
                task.taskType(),
                task.status(),
                read(jsonMapper, task.payload()),
                read(jsonMapper, task.result()),
                task.error(),
                task.createdAt(),
                task.updatedAt(),
                task.claimedAt(),
                task.finishedAt(),
                task.workerId());
    }

    /**
     * A SQL null stays a Java null, which Jackson writes as JSON null.
     * Stored JSON is parsed, including the JSON null a completed
     * {@code NOOP} stores. Status is what tells those two apart.
     */
    private static JsonNode read(JsonMapper jsonMapper, String json) {
        if (json == null) {
            return null;
        }
        return jsonMapper.readTree(json);
    }
}
