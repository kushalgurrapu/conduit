package com.kushal.workflow.task;

import java.util.UUID;

/**
 * What a complete or fail attempt did. The update already ran; these values
 * only name the outcome. {@link Rejected#currentStatus()} and
 * {@link Rejected#currentWorker()} are null when no row has that id.
 */
public sealed interface TransitionResult
        permits TransitionResult.Applied, TransitionResult.AlreadyApplied, TransitionResult.Rejected {

    /** The guarded update changed the row. */
    record Applied(Task task) implements TransitionResult {
    }

    /**
     * The update changed nothing because this worker had already moved the
     * task to the same status. The stored result, error, and timestamps stay
     * as they were.
     */
    record AlreadyApplied(Task task) implements TransitionResult {
    }

    /** The update changed nothing, and the row is not the target state for this worker. */
    record Rejected(TaskStatus currentStatus, UUID currentWorker) implements TransitionResult {
    }
}
