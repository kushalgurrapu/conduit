package com.kushal.workflow.task;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

/**
 * Transaction boundary for claim, lease extension, complete, fail, and cancel.
 *
 * <p>Each write is a single statement. Call this bean (the Spring proxy).
 * A direct self-call would skip the transaction, so the update and the
 * follow-up read would not stay on one connection.
 */
@Service
public class TaskService {

    private final TaskRepository taskRepository;
    private final LeaseProperties leaseProperties;

    public TaskService(TaskRepository taskRepository, LeaseProperties leaseProperties) {
        this.taskRepository = taskRepository;
        this.leaseProperties = leaseProperties;
        leaseProperties.validate();
    }

    public Task createTask(String taskType, String payload) {
        return taskRepository.createTask(UUID.randomUUID(), taskType, payload);
    }

    /**
     * The row as stored. A missing id is {@link TaskNotFoundException}.
     * This read does not lock the row and does not change it.
     */
    public Task getTask(UUID taskId) {
        requireTaskId(taskId);
        return taskRepository.findById(taskId)
                .orElseThrow(() -> new TaskNotFoundException(taskId));
    }

    @Transactional
    public Task claimTask(UUID workerId) {
        requireWorkerId(workerId);
        return taskRepository.claimTask(workerId, leaseProperties.leaseMicros());
    }

    /**
     * Extends the lease for the worker and attempt that claim returned.
     * The duration is {@link LeaseProperties#leaseMicros()}, the same
     * conversion claim uses. {@code false} means the guard matched nothing.
     * This method does not retry and does not treat a past lease as lost.
     */
    @Transactional
    public boolean extendLease(UUID taskId, UUID workerId, int attempt) {
        requireTaskId(taskId);
        requireWorkerId(workerId);
        return taskRepository.extendLease(taskId, workerId, attempt, leaseProperties.leaseMicros());
    }

    @Transactional
    public TransitionResult completeTask(UUID taskId, UUID workerId, int attempt, String result) {
        requireTaskId(taskId);
        requireWorkerId(workerId);
        return classifyFinish(
                taskRepository.completeTask(taskId, workerId, attempt, result),
                taskId,
                workerId,
                attempt,
                TaskStatus.COMPLETED);
    }

    @Transactional
    public TransitionResult failTask(UUID taskId, UUID workerId, int attempt, String error) {
        requireTaskId(taskId);
        requireWorkerId(workerId);
        return classifyFinish(
                taskRepository.failTask(taskId, workerId, attempt, stripNullBytes(error)),
                taskId,
                workerId,
                attempt,
                TaskStatus.FAILED);
    }

    @Transactional
    public CancelResult cancelTask(UUID taskId) {
        requireTaskId(taskId);
        Optional<Task> updated = taskRepository.cancelTask(taskId);
        if (updated.isPresent()) {
            return new CancelResult.Cancelled(updated.get());
        }
        Task current = taskRepository.findById(taskId)
                .orElseThrow(() -> new TaskNotFoundException(taskId));
        if (current.status() == TaskStatus.CANCELLED) {
            return new CancelResult.AlreadyCancelled(current);
        }
        return new CancelResult.NotCancellable(current.status());
    }

    /**
     * Names a zero-row finish. {@link TransitionResult.AlreadyApplied} is
     * this worker and this attempt already at the target status. A different
     * attempt is {@link TransitionResult.Rejected} even when the worker and
     * status match. This read must not be followed by another update: a
     * lost acknowledgement and a genuine rejection are both "zero rows",
     * and retrying the write is how a stale worker would get a second chance.
     */
    private TransitionResult classifyFinish(
            Optional<Task> updated, UUID taskId, UUID workerId, int attempt, TaskStatus target) {
        if (updated.isPresent()) {
            return new TransitionResult.Applied(updated.get());
        }
        Optional<Task> current = taskRepository.findById(taskId);
        if (current.isPresent()
                && current.get().status() == target
                && workerId.equals(current.get().workerId())
                && current.get().attempt() == attempt) {
            return new TransitionResult.AlreadyApplied(current.get());
        }
        return new TransitionResult.Rejected(
                current.map(Task::status).orElse(null),
                current.map(Task::workerId).orElse(null));
    }

    private static void requireTaskId(UUID taskId) {
        if (taskId == null) {
            throw new IllegalArgumentException("taskId is required");
        }
    }

    private static void requireWorkerId(UUID workerId) {
        if (workerId == null) {
            throw new IllegalArgumentException("workerId is required");
        }
    }

    /** PostgreSQL rejects a NUL byte in {@code text}. The column is truncated in SQL. */
    private static String stripNullBytes(String value) {
        if (value == null || value.indexOf('\0') < 0) {
            return value;
        }
        return value.replace("\u0000", "");
    }
}
