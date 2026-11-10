package com.kushal.workflow.worker;

/**
 * Work for one {@code task_type}. Implementations are Spring beans.
 * {@link HandlerConfiguration} indexes them by {@link #type()} and refuses
 * to start if two share a type or a type fails the database check.
 */
public interface TaskHandler {

    /**
     * Stored in {@code tasks.task_type}. Must match
     * {@code ^[A-Z][A-Z0-9_]{0,63}$}.
     */
    String type();

    /**
     * Runs outside any transaction. A thrown {@link InterruptedException}
     * means the task stays {@code RUNNING}. Any other {@link Exception}
     * is recorded as {@code FAILED}. An {@link Error} is not caught.
     *
     * <p>{@link TaskContext#leaseLost()} becomes {@code true} when a
     * heartbeat could not extend this attempt's lease. A handler that
     * honors it returns a {@link TaskOutcome} and leaves the interrupt flag
     * clear; the finish write is still made and is rejected if the attempt
     * no longer matches. Throwing {@link InterruptedException} is the
     * shutdown path and skips the finish write. The built-in handlers do
     * not poll the flag, and {@code SLEEP} still wakes only on an interrupt.
     */
    TaskOutcome execute(TaskContext ctx) throws Exception;
}
