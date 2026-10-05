package com.kushal.workflow.task;

import org.springframework.web.bind.annotation.*;
import java.util.UUID;

@RestController
@RequestMapping("/tasks")
public class TaskController {

    private final TaskService taskService;

    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    @PostMapping
    public UUID createTask(@RequestBody String payload) {
        // M1.5 accepts a task type. Until then this endpoint stores NOOP, the
        // same type V4 assigns to rows that already existed.
        return taskService.createTask("NOOP", payload).id();
    }

    @PostMapping(value = "/claim", consumes = {})
    public Task claimTask() {
        UUID workerId = UUID.randomUUID();
        return taskService.claimTask(workerId);
    }
}
