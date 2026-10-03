package com.kushal.workflow.task;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class TaskService {

    private final TaskRepository taskRepository;

    public TaskService(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    public UUID createTask(String payload) {
        UUID id = UUID.randomUUID();

        taskRepository.createTask(
                id,
                "PENDING",
                payload
        );
        return id;
    }
    
    @Transactional
    public Task claimTask(UUID workerId) {
        return taskRepository.claimTask(workerId);
    }
}
