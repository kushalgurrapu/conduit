package com.kushal.workflow.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Transaction boundary for claim, lease extension, complete, fail, and cancel.
 *
 * <p>Each write to {@code tasks} is a single statement. Claim and an applied
 * complete or fail add one {@code task_attempts} statement in the same
 * transaction. {@code tasks} is the source of truth and {@code task_attempts}
 * records it: a history write that cannot be made is logged at WARN, and the
 * {@code tasks} change still commits. Call this bean (the Spring proxy).
 * A direct self-call would skip the transaction, so the update, its history
 * write, and the follow-up read would not stay on one connection.
 */
@Service
public class TaskService {

    private static final Logger log = LoggerFactory.getLogger(TaskService.class);

    private final TaskRepository taskRepository;
    private final TaskAttemptRepository taskAttemptRepository;
    private final LeaseProperties leaseProperties;

    public TaskService(
            TaskRepository taskRepository,
            TaskAttemptRepository taskAttemptRepository,
            LeaseProperties leaseProperties) {
        this.taskRepository = taskRepository;
        this.taskAttemptRepository = taskAttemptRepository;
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

    /**
     * The task's attempts, ordered by attempt. A task that was never claimed,
     * or finished before V6, has none. A missing id is
     * {@link TaskNotFoundException}. The two reads are not one snapshot; that
     * is fine because nothing deletes tasks in production.
     */
    public List<TaskAttempt> getAttempts(UUID taskId) {
        requireTaskId(taskId);
        List<TaskAttempt> attempts = taskAttemptRepository.findByTaskId(taskId);
        if (attempts.isEmpty() && taskRepository.findById(taskId).isEmpty()) {
            throw new TaskNotFoundException(taskId);
        }
        return attempts;
    }

    @Transactional
    public Task claimTask(UUID workerId) {
        requireWorkerId(workerId);
        Task claimed = taskRepository.claimTask(workerId, leaseProperties.leaseMicros());
        if (claimed != null
                && !taskAttemptRepository.recordStart(claimed.id(), workerId, claimed.attempt())) {
            log.warn("Attempt {} of task {} for worker {} was not opened: a history row already exists",
                    claimed.attempt(), claimed.id(), workerId);
        }
        return claimed;
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
        Optional<Task> updated = taskRepository.completeTask(taskId, workerId, attempt, result);
        updated.ifPresent(task -> recordFinish(task, AttemptOutcome.COMPLETED));
        return classifyFinish(updated, taskId, workerId, attempt, TaskStatus.COMPLETED);
    }

    @Transactional
    public TransitionResult failTask(UUID taskId, UUID workerId, int attempt, String error) {
        requireTaskId(taskId);
        requireWorkerId(workerId);
        Optional<Task> updated = taskRepository.failTask(taskId, workerId, attempt, stripNullBytes(error));
        updated.ifPresent(task -> recordFinish(task, AttemptOutcome.FAILED));
        return classifyFinish(updated, taskId, workerId, attempt, TaskStatus.FAILED);
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
     * One reap batch in its own transaction. The timeout bounds a reap that
     * waits on a table lock. Callers must not depend on the order of the result.
     */
    @Transactional(timeout = 5)
    public List<ReapedTask> reapExpiredLeases(int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be at least 1");
        }
        return taskRepository.reapExpiredLeases(batchSize);
    }

    /** Runs only for an applied finish, never after the zero-row read in {@link #classifyFinish}. */
    private void recordFinish(Task finished, AttemptOutcome outcome) {
        if (!taskAttemptRepository.recordFinish(finished.id(), finished.attempt(), outcome)) {
            log.warn("Attempt {} of task {} for worker {} was not closed as {}: it was already closed",
                    finished.attempt(), finished.id(), finished.workerId(), outcome);
        }
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
