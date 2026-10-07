package com.kushal.workflow.worker;

import com.kushal.workflow.task.Task;
import com.kushal.workflow.task.TaskService;
import com.kushal.workflow.task.TransitionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;
import java.util.UUID;

/**
 * One claim, one handler call, one finish write.
 *
 * <p>This class is not {@code @Transactional}. Claim and the finish write
 * are transactions because they go through the {@link TaskService} proxy.
 * The handler runs only after claim has committed, so it does not hold a
 * pooled connection. A finish write that throws is logged and not retried;
 * the row stays {@code RUNNING}.
 */
@Component
public class TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(TaskExecutor.class);

    private static final Runnable NO_AFTER_CLAIM = () -> {
    };

    private final TaskService taskService;
    private final Map<String, TaskHandler> taskHandlers;
    private final JsonMapper jsonMapper;

    /**
     * Runs after a claim has committed and before the handler. Empty in
     * production. A test stops the pool from here. Nothing in this class
     * reads a shutdown flag: a claimed task is always executed.
     */
    private volatile Runnable afterClaim = NO_AFTER_CLAIM;

    public TaskExecutor(
            TaskService taskService,
            @Qualifier("taskHandlers") Map<String, TaskHandler> taskHandlers,
            JsonMapper jsonMapper) {
        this.taskService = taskService;
        this.taskHandlers = requireTypeKeys(taskHandlers);
        this.jsonMapper = jsonMapper;
    }

    /**
     * Claims one pending task for {@code workerId} and finishes it.
     *
     * @return {@code false} when there was nothing to claim. {@code true}
     *         when a task was claimed, including when it is left
     *         {@code RUNNING} because the handler was interrupted or the
     *         finish write threw.
     */
    public boolean runOnce(UUID workerId) {
        MDC.put("workerId", String.valueOf(workerId));
        try {
            Task claimed = taskService.claimTask(workerId);
            if (claimed == null) {
                return false;
            }
            MDC.put("taskId", claimed.id().toString());
            MDC.put("taskType", claimed.taskType());
            try {
                afterClaim.run();
                runClaimed(claimed, workerId);
                return true;
            } finally {
                MDC.remove("taskId");
                MDC.remove("taskType");
            }
        } finally {
            MDC.remove("workerId");
        }
    }

    /**
     * Test hook. Pass {@code null} to clear it. The runnable runs on the
     * worker thread, after claim and before {@link TaskHandler#execute}.
     */
    void setAfterClaim(Runnable afterClaim) {
        this.afterClaim = afterClaim == null ? NO_AFTER_CLAIM : afterClaim;
    }

    private void runClaimed(Task claimed, UUID workerId) {
        TaskHandler handler = taskHandlers.get(claimed.taskType());
        if (handler == null) {
            log.warn("No handler for task {} type {}", claimed.id(), claimed.taskType());
            finish(claimed, workerId, new TaskOutcome.Failed("no handler for type " + claimed.taskType()));
            return;
        }

        TaskOutcome outcome;
        try {
            TaskContext context = new TaskContext(
                    claimed.id(),
                    claimed.taskType(),
                    jsonMapper.readTree(claimed.payload()),
                    workerId);
            outcome = handler.execute(context);
        } catch (Exception ex) {
            // An interrupt is not a failed task. The flag is restored so
            // the caller can leave the loop without claiming again.
            if (interrupted(ex)) {
                Thread.currentThread().interrupt();
                log.info("Interrupted while executing task {}; leaving it RUNNING", claimed.id());
                return;
            }
            log.warn("Task {} failed in the handler", claimed.id(), ex);
            finish(claimed, workerId, new TaskOutcome.Failed(failureMessage(ex)));
            return;
        }

        if (outcome == null) {
            outcome = new TaskOutcome.Failed("handler returned null");
        }
        finish(claimed, workerId, outcome);
    }

    /**
     * One complete or fail. The interrupt flag is cleared around the write
     * because a pooled connection aborts when the thread is interrupted,
     * then restored so the caller still sees it.
     */
    private void finish(Task claimed, UUID workerId, TaskOutcome outcome) {
        boolean interrupted = Thread.interrupted();
        try {
            TransitionResult result = switch (outcome) {
                case TaskOutcome.Succeeded succeeded -> taskService.completeTask(
                        claimed.id(), workerId, jsonText(succeeded.result()));
                case TaskOutcome.Failed failed -> taskService.failTask(
                        claimed.id(), workerId, failed.reason());
            };
            if (!(result instanceof TransitionResult.Applied)
                    && !(result instanceof TransitionResult.AlreadyApplied)) {
                log.warn(
                        "Finish rejected for task {} worker {}: {}",
                        claimed.id(),
                        workerId,
                        result);
            }
        } catch (RuntimeException ex) {
            log.warn("Finish write failed for task {}; leaving it RUNNING", claimed.id(), ex);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * JSON null is the text {@code "null"}, not a Java null. A Java null
     * parameter has no SQL type, and {@code CAST(:result AS jsonb)} would
     * fail.
     */
    private String jsonText(JsonNode result) {
        if (result == null || result.isNull()) {
            return "null";
        }
        return jsonMapper.writeValueAsString(result);
    }

    private static boolean interrupted(Throwable thrown) {
        Throwable current = thrown;
        int depth = 0;
        while (current != null && depth++ < 32) {
            if (current instanceof InterruptedException) {
                return true;
            }
            current = current.getCause();
        }
        return Thread.currentThread().isInterrupted();
    }

    private static String failureMessage(Exception ex) {
        String message = ex.getMessage();
        if (message == null || message.isEmpty()) {
            return ex.getClass().getName();
        }
        return ex.getClass().getName() + ": " + message;
    }

    /**
     * Spring will inject every {@link TaskHandler} keyed by bean name when
     * a {@code Map<String, TaskHandler>} parameter has no qualifier. Those
     * keys are not task types, so every claim would look like a missing
     * handler. Fail here instead.
     */
    private static Map<String, TaskHandler> requireTypeKeys(Map<String, TaskHandler> handlers) {
        for (Map.Entry<String, TaskHandler> entry : handlers.entrySet()) {
            String type = entry.getValue().type();
            if (!entry.getKey().equals(type)) {
                throw new IllegalStateException(
                        "Handler map is keyed by '" + entry.getKey()
                                + "' but the handler type is '" + type + "'");
            }
        }
        return Map.copyOf(handlers);
    }
}
