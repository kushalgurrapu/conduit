package durable_workflow_engine.task;

import java.time.LocalDateTime;
import java.util.UUID;

public record Task(
        UUID id,
        String status,
        String payload,
        LocalDateTime createdAt,
        LocalDateTime claimedAt,
        UUID workerId
) {
}