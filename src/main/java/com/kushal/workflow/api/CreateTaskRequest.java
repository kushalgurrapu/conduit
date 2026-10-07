package com.kushal.workflow.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import tools.jackson.databind.JsonNode;

/**
 * Body of {@code POST /api/v1/tasks}.
 *
 * <p>{@code taskType} is checked against the same pattern as
 * {@code tasks_task_type_format_check}. Whether a handler is registered
 * for that type is not checked here. An unknown type is stored and fails
 * later, when a worker claims it. That keeps this package from depending
 * on the worker package.
 *
 * <p>{@code payload} is any JSON value except JSON null. A NUL byte in a
 * key or a text value is rejected: PostgreSQL will not store one in
 * {@code text} or {@code jsonb}.
 */
public record CreateTaskRequest(
        @NotBlank
        @Pattern(regexp = "^[A-Z][A-Z0-9_]{0,63}$")
        String taskType,
        @NotNull
        @ValidTaskPayload
        JsonNode payload
) {
}
