package com.kushal.workflow.task;

import java.time.Instant;
import java.util.UUID;

public record Task(
        UUID id,
        String taskType,
        TaskStatus status,
        String payload,
        String result,
        String error,
        Instant createdAt,
        Instant updatedAt,
        Instant claimedAt,
        Instant finishedAt,
        UUID workerId
) {
}
