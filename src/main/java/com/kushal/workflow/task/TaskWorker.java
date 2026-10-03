package com.kushal.workflow.task;

import org.springframework.stereotype.Component;

@Component
public class TaskWorker {

    public void run() {
        System.out.println("Worker is running");
    }
}
