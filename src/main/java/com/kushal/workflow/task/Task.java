package com.kushal.workflow.task;

import java.time.Instant;
import java.util.UUID;

public record Task(
        UUID id,
        String status,
        String payload,
        Instant createdAt,
        Instant claimedAt,
        UUID workerId
) {
}
