package com.kushal.workflow.worker;

import org.springframework.stereotype.Component;

/** Completes the task with the payload as the result. */
@Component
public class EchoHandler implements TaskHandler {

    @Override
    public String type() {
        return "ECHO";
    }

    @Override
    public TaskOutcome execute(TaskContext ctx) {
        return new TaskOutcome.Succeeded(ctx.payload());
    }
}
