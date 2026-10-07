package com.kushal.workflow.api;

import com.kushal.workflow.task.TaskStatus;

import java.util.UUID;

/**
 * Cancel was refused because the task is not {@code PENDING}.
 *
 * <p>The service reports that as a value
 * ({@link com.kushal.workflow.task.CancelResult.NotCancellable}). The
 * controller turns that value into this exception so the HTTP body is
 * built in {@link ApiExceptionHandler}, next to the other error responses.
 * The service does not throw it, and the worker path does not use it.
 */
public final class IllegalTaskTransitionException extends RuntimeException {

    private final UUID taskId;
    private final TaskStatus status;

    public IllegalTaskTransitionException(UUID taskId, TaskStatus status) {
        super("Task " + taskId + " is " + status + " and cannot be cancelled");
        this.taskId = taskId;
        this.status = status;
    }

    public UUID taskId() {
        return taskId;
    }

    public TaskStatus status() {
        return status;
    }
}
