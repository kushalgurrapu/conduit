package com.kushal.workflow.worker;

import com.kushal.workflow.task.CancelResult;
import com.kushal.workflow.task.LeaseProperties;
import com.kushal.workflow.task.ReapedTask;
import com.kushal.workflow.task.Task;
import com.kushal.workflow.task.TaskAttempt;
import com.kushal.workflow.task.TaskService;
import com.kushal.workflow.task.TransitionResult;
import org.springframework.dao.DataAccessResourceFailureException;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A {@link TaskService} for hand-built executors that passes every call to
 * the context's proxy and records what each attempt was told.
 *
 * <p>Every public {@link TaskService} method is overridden, because the
 * superclass has no repositories. Claims, finish results, and extend
 * results are kept per {@link Attempt}, in the order the calls returned. A
 * call that throws records no result. One worker id can run many tasks,
 * so the key includes the task.
 *
 * <p>{@link #throwOnceFromComplete} makes the next {@code completeTask} from
 * one worker throw before the proxy is called, so nothing is written.
 */
final class RecordingTaskService extends TaskService {

    private final TaskService delegate;
    private final List<Attempt> claims = new CopyOnWriteArrayList<>();
    private final List<Finish> finishes = new CopyOnWriteArrayList<>();
    private final List<Extend> extendCalls = new CopyOnWriteArrayList<>();
    private final List<Attempt> scriptedThrows = new CopyOnWriteArrayList<>();
    private final Set<UUID> throwOnceFromComplete = ConcurrentHashMap.newKeySet();

    /** {@code delegate} must be the Spring proxy, so each call is its own transaction. */
    RecordingTaskService(TaskService delegate) {
        super(null, null, new LeaseProperties());
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    /**
     * The next {@code completeTask} from {@code workerId}, for any task and
     * attempt, throws {@link DataAccessResourceFailureException} without
     * reaching the database. Later calls delegate.
     */
    void throwOnceFromComplete(UUID workerId) {
        throwOnceFromComplete.add(workerId);
    }

    /** Every claim that returned a task, in order. */
    List<Attempt> claims() {
        return List.copyOf(claims);
    }

    /** Every recorded complete and fail, in order. */
    List<Finish> finishes() {
        return List.copyOf(finishes);
    }

    /** One task's recorded completes and fails, from any worker and attempt, in order. */
    List<Finish> finishes(UUID taskId) {
        return finishes.stream().filter(finish -> finish.attempt().taskId().equals(taskId)).toList();
    }

    /** What this worker's completes and fails for this attempt returned, in order. */
    List<TransitionResult> finishResults(UUID taskId, UUID workerId, int attempt) {
        Attempt key = new Attempt(taskId, workerId, attempt);
        return finishes.stream().filter(finish -> finish.attempt().equals(key)).map(Finish::result).toList();
    }

    /** What this worker's extends for this attempt returned, in order. */
    List<Boolean> extendResults(UUID taskId, UUID workerId, int attempt) {
        Attempt key = new Attempt(taskId, workerId, attempt);
        return extendCalls.stream().filter(extend -> extend.attempt().equals(key)).map(Extend::extended).toList();
    }

    /** The completes that {@link #throwOnceFromComplete} made throw, in order. */
    List<Attempt> scriptedThrows() {
        return List.copyOf(scriptedThrows);
    }

    @Override
    public Task createTask(String taskType, String payload) {
        return delegate.createTask(taskType, payload);
    }

    @Override
    public Task getTask(UUID taskId) {
        return delegate.getTask(taskId);
    }

    @Override
    public List<TaskAttempt> getAttempts(UUID taskId) {
        return delegate.getAttempts(taskId);
    }

    @Override
    public Task claimTask(UUID workerId) {
        Task claimed = delegate.claimTask(workerId);
        if (claimed != null) {
            claims.add(new Attempt(claimed.id(), workerId, claimed.attempt()));
        }
        return claimed;
    }

    @Override
    public boolean extendLease(UUID taskId, UUID workerId, int attempt) {
        boolean extended = delegate.extendLease(taskId, workerId, attempt);
        extendCalls.add(new Extend(new Attempt(taskId, workerId, attempt), extended));
        return extended;
    }

    @Override
    public TransitionResult completeTask(UUID taskId, UUID workerId, int attempt, String result) {
        Attempt key = new Attempt(taskId, workerId, attempt);
        if (workerId != null && throwOnceFromComplete.remove(workerId)) {
            scriptedThrows.add(key);
            throw new DataAccessResourceFailureException(
                    "scripted complete failure for task " + taskId + " worker " + workerId + " attempt " + attempt);
        }
        TransitionResult outcome = delegate.completeTask(taskId, workerId, attempt, result);
        finishes.add(new Finish(key, FinishKind.COMPLETE, outcome));
        return outcome;
    }

    @Override
    public TransitionResult failTask(UUID taskId, UUID workerId, int attempt, String error) {
        TransitionResult outcome = delegate.failTask(taskId, workerId, attempt, error);
        finishes.add(new Finish(new Attempt(taskId, workerId, attempt), FinishKind.FAIL, outcome));
        return outcome;
    }

    @Override
    public CancelResult cancelTask(UUID taskId) {
        return delegate.cancelTask(taskId);
    }

    @Override
    public List<ReapedTask> reapExpiredLeases(int batchSize) {
        return delegate.reapExpiredLeases(batchSize);
    }

    /** One claimed attempt as a worker saw it. */
    record Attempt(UUID taskId, UUID workerId, int attempt) {
    }

    enum FinishKind {
        COMPLETE,
        FAIL
    }

    record Finish(Attempt attempt, FinishKind kind, TransitionResult result) {
    }

    private record Extend(Attempt attempt, boolean extended) {
    }
}
