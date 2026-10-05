package com.kushal.workflow.task;

import com.kushal.workflow.support.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class ClaimUpdatedAtTest extends PostgresIntegrationTest {

    @Autowired
    private TaskService taskService;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    void claimMovesUpdatedAtToTheSameInstantAsClaimedAt() {
        Task created = taskService.createTask("NOOP", "{\"test\":\"updated-at\"}");
        Instant backdated = Instant.parse("2000-01-01T00:00:00Z");
        jdbcClient.sql("""
                UPDATE tasks
                SET updated_at = :updatedAt
                WHERE id = :id
                """)
                .param("updatedAt", OffsetDateTime.ofInstant(backdated, ZoneOffset.UTC))
                .param("id", created.id())
                .update();

        UUID workerId = UUID.randomUUID();
        Task claimed = taskService.claimTask(workerId);

        assertEquals(created.id(), claimed.id());
        assertEquals(TaskStatus.RUNNING, claimed.status());
        assertEquals("NOOP", claimed.taskType());
        assertEquals(workerId, claimed.workerId());
        assertNotNull(claimed.claimedAt());
        assertNotNull(claimed.updatedAt());
        assertNull(claimed.finishedAt());
        assertTrue(claimed.updatedAt().isAfter(backdated));
        assertEquals(claimed.claimedAt(), claimed.updatedAt());

        Instant storedUpdatedAt = jdbcClient.sql("""
                SELECT updated_at
                FROM tasks
                WHERE id = :id
                """)
                .param("id", claimed.id())
                .query((rs, rowNum) -> rs.getObject("updated_at", OffsetDateTime.class).toInstant())
                .single();
        Instant storedClaimedAt = jdbcClient.sql("""
                SELECT claimed_at
                FROM tasks
                WHERE id = :id
                """)
                .param("id", claimed.id())
                .query((rs, rowNum) -> rs.getObject("claimed_at", OffsetDateTime.class).toInstant())
                .single();
        assertEquals(claimed.updatedAt(), storedUpdatedAt);
        assertEquals(claimed.claimedAt(), storedClaimedAt);
    }

    @Test
    void claimRejectsANullWorkerId() {
        Task created = taskService.createTask("NOOP", "{\"test\":\"null-worker\"}");

        assertThrows(IllegalArgumentException.class, () -> taskService.claimTask(null));

        String status = jdbcClient.sql("SELECT status FROM tasks WHERE id = :id")
                .param("id", created.id())
                .query(String.class)
                .single();
        assertEquals("PENDING", status);
    }
}
