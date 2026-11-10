package com.kushal.workflow.task;

import java.time.Instant;
import java.util.UUID;

/** One claim of a task. {@code endedAt} and {@code outcome} are null while the attempt is open. */
public record TaskAttempt(
        UUID taskId,
        int attempt,
        UUID workerId,
        Instant startedAt,
        Instant endedAt,
        AttemptOutcome outcome,
        String error
) {
}
