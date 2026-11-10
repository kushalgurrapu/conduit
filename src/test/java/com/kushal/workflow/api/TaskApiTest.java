package com.kushal.workflow.api;

import com.jayway.jsonpath.JsonPath;
import com.kushal.workflow.support.PostgresIntegrationTest;
import com.kushal.workflow.task.TaskService;
import com.kushal.workflow.task.TaskStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.springframework.http.HttpHeaders.LOCATION;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Create, get, and cancel against PostgreSQL. Workers are off, so a task
 * stays {@code PENDING} until this test cancels it or claims it.
 */
@SpringBootTest
@AutoConfigureMockMvc
class TaskApiTest extends PostgresIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private TaskService taskService;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    void createGetAndCancel() throws Exception {
        MvcResult created = mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"taskType\":\"NO_SUCH\",\"payload\":{\"n\":1}}"))
                .andExpect(status().isCreated())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.taskType").value("NO_SUCH"))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.payload.n").value(1))
                .andExpect(jsonPath("$.claimedAt").value(nullValue()))
                .andExpect(jsonPath("$.workerId").value(nullValue()))
                .andExpect(jsonPath("$.finishedAt").value(nullValue()))
                .andExpect(jsonPath("$.attempt").value(0))
                .andExpect(jsonPath("$.leaseExpiresAt").value(nullValue()))
                .andReturn();

        UUID first = UUID.fromString(JsonPath.read(created.getResponse().getContentAsString(), "$.id"));
        assertEquals("/api/v1/tasks/" + first, created.getResponse().getHeader(LOCATION));

        MvcResult pending = mockMvc.perform(get("/api/v1/tasks/{id}", first))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.payload.n").value(1))
                .andExpect(jsonPath("$.attempt").value(0))
                .andExpect(jsonPath("$.leaseExpiresAt").value(nullValue()))
                .andReturn();
        assertWorkerIdOnceAndNoHeartbeat(pending.getResponse().getContentAsString());

        MvcResult secondCreated = mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"taskType\":\"ECHO\",\"payload\":{\"message\":\"two\"}}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andReturn();
        UUID second = UUID.fromString(JsonPath.read(secondCreated.getResponse().getContentAsString(), "$.id"));

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", second))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"))
                .andExpect(jsonPath("$.finishedAt").isNotEmpty())
                .andExpect(jsonPath("$.workerId").value(nullValue()));

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", second))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        mockMvc.perform(get("/api/v1/tasks/{id}", first))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.finishedAt").value(nullValue()));
        mockMvc.perform(get("/api/v1/tasks/{id}", second))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        mockMvc.perform(post("/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"message\":\"old\"}"))
                .andExpect(status().isNotFound());
        mockMvc.perform(post("/tasks/claim"))
                .andExpect(status().isNotFound());
        mockMvc.perform(post("/api/v1/tasks/{id}/complete", first))
                .andExpect(status().isNotFound());
        mockMvc.perform(post("/api/v1/tasks/{id}/fail", first))
                .andExpect(status().isNotFound());
    }

    @Test
    void getAfterClaimShowsTheAttemptAndTheLease() throws Exception {
        UUID id = createNoop();
        UUID workerId = UUID.fromString("00000000-0000-0000-0000-0000000000b2");
        assertEquals(id, taskService.claimTask(workerId).id());

        MvcResult running = mockMvc.perform(get("/api/v1/tasks/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(TaskStatus.RUNNING.name()))
                .andExpect(jsonPath("$.workerId").value(workerId.toString()))
                .andExpect(jsonPath("$.attempt").value(1))
                .andReturn();
        String body = running.getResponse().getContentAsString();

        String leaseExpiresAt = JsonPath.read(body, "$.leaseExpiresAt");
        assertEquals(lease(id).leaseExpiresAt(), Instant.parse(leaseExpiresAt));
        assertWorkerIdOnceAndNoHeartbeat(body);
    }

    @Test
    void cancelOfARunningTaskIsConflictAndLeavesTheRow() throws Exception {
        UUID id = createNoop();
        UUID workerId = UUID.fromString("00000000-0000-0000-0000-0000000000b1");
        assertEquals(id, taskService.claimTask(workerId).id());
        Lease claimed = lease(id);

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", id))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.title").exists())
                .andExpect(jsonPath("$.detail").exists());

        assertEquals(claimed, lease(id));

        mockMvc.perform(get("/api/v1/tasks/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(TaskStatus.RUNNING.name()))
                .andExpect(jsonPath("$.workerId").value(workerId.toString()))
                .andExpect(jsonPath("$.claimedAt").isNotEmpty())
                .andExpect(jsonPath("$.attempt").value(1))
                .andExpect(jsonPath("$.leaseExpiresAt").value(claimed.leaseExpiresAt().toString()));
    }

    private UUID createNoop() throws Exception {
        MvcResult created = mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"taskType\":\"NOOP\",\"payload\":{}}"))
                .andExpect(status().isCreated())
                .andReturn();
        return UUID.fromString(JsonPath.read(created.getResponse().getContentAsString(), "$.id"));
    }

    private Lease lease(UUID id) {
        return jdbcClient.sql("""
                        SELECT attempt, lease_expires_at, last_heartbeat_at
                        FROM tasks
                        WHERE id = :id
                        """)
                .param("id", id)
                .query((rs, rowNum) -> new Lease(
                        rs.getInt("attempt"),
                        instant(rs, "lease_expires_at"),
                        instant(rs, "last_heartbeat_at")))
                .single();
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private static void assertWorkerIdOnceAndNoHeartbeat(String body) {
        assertEquals(1, body.split("\"workerId\"", -1).length - 1, body);
        assertFalse(body.contains("lastHeartbeatAt"), body);
    }

    private record Lease(int attempt, Instant leaseExpiresAt, Instant lastHeartbeatAt) {
    }
}
