package com.kushal.workflow.task;

/**
 * How a stored attempt in {@code task_attempts} ended. This is not
 * {@link com.kushal.workflow.worker.TaskOutcome}, which is what a handler
 * returns. {@link #LEASE_EXPIRED} is written by the lease reaper, never by a
 * handler: no finish was recorded for the attempt before its lease ran out.
 */
public enum AttemptOutcome {
    COMPLETED,
    FAILED,
    LEASE_EXPIRED
}
