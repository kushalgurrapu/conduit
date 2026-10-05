package com.kushal.workflow.task;

import java.util.UUID;

/** Cancel was asked for an id that is not in {@code tasks}. */
public final class TaskNotFoundException extends RuntimeException {

    private final UUID taskId;

    public TaskNotFoundException(UUID taskId) {
        super("No task with id " + taskId);
        this.taskId = taskId;
    }

    public UUID taskId() {
        return taskId;
    }
}
