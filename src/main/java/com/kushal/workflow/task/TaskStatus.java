package com.kushal.workflow.task;

/**
 * Stored as text in {@code tasks.status}. Whether a status may change is
 * decided by the SQL {@code WHERE} clause of each update, not by this enum.
 */
public enum TaskStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELLED;
    }
}
