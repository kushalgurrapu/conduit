package com.kushal.workflow.worker;

import org.springframework.stereotype.Component;

/**
 * Completes the task and stores JSON null. Rows that existed before V4
 * were backfilled to this type.
 */
@Component
public class NoopHandler implements TaskHandler {

    @Override
    public String type() {
        return "NOOP";
    }

    @Override
    public TaskOutcome execute(TaskContext ctx) {
        return new TaskOutcome.Succeeded(null);
    }
}
