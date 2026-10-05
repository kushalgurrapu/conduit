package com.kushal.workflow.task;

/**
 * What a cancel attempt did. A missing id is {@link TaskNotFoundException},
 * not one of these values.
 */
public sealed interface CancelResult
        permits CancelResult.Cancelled, CancelResult.AlreadyCancelled, CancelResult.NotCancellable {

    /** The task was {@code PENDING} and is now {@code CANCELLED}. */
    record Cancelled(Task task) implements CancelResult {
    }

    /** The task was already {@code CANCELLED}. The row was not written again. */
    record AlreadyCancelled(Task task) implements CancelResult {
    }

    /** The task exists and is not {@code PENDING}, so cancel did not change it. */
    record NotCancellable(TaskStatus status) implements CancelResult {
    }
}
