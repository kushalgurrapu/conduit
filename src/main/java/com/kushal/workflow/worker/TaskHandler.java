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
     * means the task stays {@code RUNNING} until its lease expires and a
     * reaper requeues it. Any other {@link Exception} is recorded as
     * {@code FAILED}. An {@link Error} that is not a
     * {@link VirtualMachineError}, and a {@link StackOverflowError}, are one
     * {@code FAILED} finish too. Any other {@link VirtualMachineError}
     * propagates and leaves the row {@code RUNNING} until it is reaped.
     *
     * <p>A task may run more than once, possibly at the same time as
     * another run of it, because a requeued task is claimed again while the
     * old run may still be going.
     *
     * <p>{@link TaskContext#leaseLost()} becomes {@code true} when a
     * heartbeat could not extend this attempt's lease. A handler that
     * honors it returns a {@link TaskOutcome} and leaves the interrupt flag
     * clear; the finish write is still made and is rejected once this
     * attempt no longer owns the row, by the status guard after a reap or a
     * cancel, or by the attempt guard after a later claim. Throwing
     * {@link InterruptedException} is the shutdown path and skips the
     * finish write. The built-in handlers do not poll the flag, and
     * {@code SLEEP} still wakes only on an interrupt.
     */
    TaskOutcome execute(TaskContext ctx) throws Exception;
}
