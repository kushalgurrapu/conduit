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

    public Task createTask(String taskType, String payload) {
        return taskRepository.createTask(UUID.randomUUID(), taskType, payload);
    }

    @Transactional
    public Task claimTask(UUID workerId) {
        if (workerId == null) {
            throw new IllegalArgumentException("workerId is required");
        }
        return taskRepository.claimTask(workerId);
    }
}
