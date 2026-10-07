package com.kushal.workflow.api;

import com.kushal.workflow.task.CancelResult;
import com.kushal.workflow.task.Task;
import com.kushal.workflow.task.TaskService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.util.UUID;

/**
 * Create, read, and cancel. Claim, complete, and fail are not HTTP
 * operations. A worker calls {@link TaskService} for those.
 */
@RestController
@RequestMapping("/api/v1/tasks")
public class TaskController {

    private final TaskService taskService;
    private final JsonMapper jsonMapper;

    public TaskController(TaskService taskService, JsonMapper jsonMapper) {
        this.taskService = taskService;
        this.jsonMapper = jsonMapper;
    }

    @PostMapping
    public ResponseEntity<TaskResponse> create(@Valid @RequestBody CreateTaskRequest request) {
        Task created = taskService.createTask(
                request.taskType(),
                jsonMapper.writeValueAsString(request.payload()));
        URI location = URI.create("/api/v1/tasks/" + created.id());
        return ResponseEntity.created(location).body(TaskResponse.from(created, jsonMapper));
    }

    @GetMapping("/{id}")
    public TaskResponse get(@PathVariable UUID id) {
        return TaskResponse.from(taskService.getTask(id), jsonMapper);
    }

    /**
     * {@code 200} when the task is pending or already cancelled.
     * {@code 409} when it is running or in another terminal status.
     * {@code 404} when the id is missing.
     */
    @PostMapping("/{id}/cancel")
    public TaskResponse cancel(@PathVariable UUID id) {
        CancelResult result = taskService.cancelTask(id);
        return switch (result) {
            case CancelResult.Cancelled cancelled -> TaskResponse.from(cancelled.task(), jsonMapper);
            case CancelResult.AlreadyCancelled already -> TaskResponse.from(already.task(), jsonMapper);
            case CancelResult.NotCancellable rejected ->
                    throw new IllegalTaskTransitionException(id, rejected.status());
        };
    }
}
