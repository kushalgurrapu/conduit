package com.kushal.workflow.api;

import java.util.List;
import java.util.UUID;

/**
 * Body of {@code GET /api/v1/tasks/{id}/attempts}. {@code attempts} is
 * ordered by attempt, and is empty for a task that was never claimed or
 * finished before V6.
 */
public record TaskAttemptsResponse(
        UUID taskId,
        List<TaskAttemptResponse> attempts
) {
}
