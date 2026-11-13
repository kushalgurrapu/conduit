package com.kushal.workflow.api;

import com.kushal.workflow.task.AttemptOutcome;
import com.kushal.workflow.task.TaskAttempt;

import java.time.Instant;
import java.util.UUID;

/**
 * One attempt a client is allowed to see. The task id is on
 * {@link TaskAttemptsResponse}, not repeated here. {@code endedAt} and
 * {@code outcome} are null while the attempt is open.
 */
public record TaskAttemptResponse(
        int attempt,
        UUID workerId,
        Instant startedAt,
        Instant endedAt,
        AttemptOutcome outcome,
        String error
) {

    static TaskAttemptResponse from(TaskAttempt attempt) {
        return new TaskAttemptResponse(
                attempt.attempt(),
                attempt.workerId(),
                attempt.startedAt(),
                attempt.endedAt(),
                attempt.outcome(),
                attempt.error());
    }
}
