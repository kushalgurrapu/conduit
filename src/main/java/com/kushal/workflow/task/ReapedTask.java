package com.kushal.workflow.task;

import java.util.UUID;

/**
 * One task the reap returned to {@code PENDING}. {@code attempt} is unchanged by the reap.
 * {@code attemptRecorded} is false when that attempt's history row was already closed.
 */
public record ReapedTask(
        UUID taskId,
        int attempt,
        UUID previousWorkerId,
        boolean attemptRecorded
) {
}
