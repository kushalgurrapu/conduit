package durable_workflow_engine.task;

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
        return taskService.createTask(payload);
    }

    @PostMapping(value = "/claim", consumes = {})
    public Task claimTask() {
        UUID workerId = UUID.randomUUID();
        return taskService.claimTask(workerId);
    }
}