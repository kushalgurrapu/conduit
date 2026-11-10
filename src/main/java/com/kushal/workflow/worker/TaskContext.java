package com.kushal.workflow.worker;

import tools.jackson.databind.JsonNode;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The claim a handler is executing.
 *
 * <p>{@link #attempt()} is copied from the claimed task. Only the claim
 * statement increments that counter.
 *
 * <p>{@link #leaseLost()} becomes {@code true} when a heartbeat's lease
 * extension matched no row while the handler was still running. It never
 * becomes {@code false} again. It is a request to stop, not an interrupt:
 * nothing stops a handler that ignores it.
 */
public final class TaskContext {

    private final UUID taskId;
    private final String taskType;
    private final JsonNode payload;
    private final UUID workerId;
    private final int attempt;
    private final AtomicBoolean leaseLost = new AtomicBoolean();

    public TaskContext(UUID taskId, String taskType, JsonNode payload, UUID workerId, int attempt) {
        this.taskId = taskId;
        this.taskType = taskType;
        this.payload = payload;
        this.workerId = workerId;
        this.attempt = attempt;
    }

    public UUID taskId() {
        return taskId;
    }

    public String taskType() {
        return taskType;
    }

    public JsonNode payload() {
        return payload;
    }

    public UUID workerId() {
        return workerId;
    }

    public int attempt() {
        return attempt;
    }

    public boolean leaseLost() {
        return leaseLost.get();
    }

    void markLeaseLost() {
        leaseLost.set(true);
    }

    @Override
    public String toString() {
        return "TaskContext[taskId=" + taskId
                + ", taskType=" + taskType
                + ", workerId=" + workerId
                + ", attempt=" + attempt
                + ", leaseLost=" + leaseLost.get() + "]";
    }
}
