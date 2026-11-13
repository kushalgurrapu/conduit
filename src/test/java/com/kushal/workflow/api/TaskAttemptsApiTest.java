package com.kushal.workflow.api;

import com.jayway.jsonpath.JsonPath;
import com.kushal.workflow.support.PostgresIntegrationTest;
import com.kushal.workflow.task.ReapedTask;
import com.kushal.workflow.task.Task;
import com.kushal.workflow.task.TaskService;
import com.kushal.workflow.task.TransitionResult;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * {@code GET /api/v1/tasks/{id}/attempts} against PostgreSQL, in the same
 * context as {@link TaskApiTest}. Workers and the reaper are off, so a
 * claim is a {@link TaskService#claimTask} call, expiry is set by SQL, and
 * a reap is a {@link TaskService#reapExpiredLeases} call.
 */
@SpringBootTest
@AutoConfigureMockMvc
class TaskAttemptsApiTest extends PostgresIntegrationTest {

    private static final UUID WORKER_A = UUID.fromString("00000000-0000-0000-0000-0000000000a1");
    private static final UUID WORKER_B = UUID.fromString("00000000-0000-0000-0000-0000000000a2");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private TaskService taskService;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    void createdTaskHasNoAttempts() throws Exception {
        UUID id = createNoop();

        mockMvc.perform(get("/api/v1/tasks/{id}/attempts", id))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.taskId").value(id.toString()))
                .andExpect(jsonPath("$.attempts", hasSize(0)));
    }

    @Test
    void claimOpensOneAttemptAtClaimedAt() throws Exception {
        UUID id = createNoop();
        claim(id, WORKER_A, 1);
        String claimedAt = JsonPath.read(getTask(id), "$.claimedAt");

        String body = mockMvc.perform(get("/api/v1/tasks/{id}/attempts", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.taskId").value(id.toString()))
                .andExpect(jsonPath("$.attempts", hasSize(1)))
                .andExpect(jsonPath("$.attempts[0].attempt").value(1))
                .andExpect(jsonPath("$.attempts[0].workerId").value(WORKER_A.toString()))
                .andExpect(jsonPath("$.attempts[0].endedAt").value(nullValue()))
                .andExpect(jsonPath("$.attempts[0].outcome").value(nullValue()))
                .andExpect(jsonPath("$.attempts[0].error").value(nullValue()))
                .andReturn().getResponse().getContentAsString();

        assertEquals(Instant.parse(claimedAt), Instant.parse(JsonPath.read(body, "$.attempts[0].startedAt")));
    }

    @Test
    void completeClosesTheAttemptAtFinishedAt() throws Exception {
        UUID id = createNoop();
        Task claimed = claim(id, WORKER_A, 1);
        assertInstanceOf(TransitionResult.Applied.class,
                taskService.completeTask(id, WORKER_A, claimed.attempt(), "{\"ok\":true}"));
        String finishedAt = JsonPath.read(getTask(id), "$.finishedAt");
        assertNotNull(finishedAt);

        String body = mockMvc.perform(get("/api/v1/tasks/{id}/attempts", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.attempts", hasSize(1)))
                .andExpect(jsonPath("$.attempts[0].attempt").value(1))
                .andExpect(jsonPath("$.attempts[0].workerId").value(WORKER_A.toString()))
                .andExpect(jsonPath("$.attempts[0].outcome").value("COMPLETED"))
                .andExpect(jsonPath("$.attempts[0].error").value(nullValue()))
                .andReturn().getResponse().getContentAsString();

        assertEquals(Instant.parse(finishedAt), Instant.parse(JsonPath.read(body, "$.attempts[0].endedAt")));
    }

    @Test
    void reapClosesTheAttemptAndTheNextClaimOpensAttemptTwo() throws Exception {
        UUID id = createNoop();
        claim(id, WORKER_A, 1);
        expireLease(id);
        assertEquals(List.of(new ReapedTask(id, 1, WORKER_A, true)), taskService.reapExpiredLeases(10));

        mockMvc.perform(get("/api/v1/tasks/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.attempt").value(1))
                .andExpect(jsonPath("$.workerId").value(nullValue()))
                .andExpect(jsonPath("$.claimedAt").value(nullValue()))
                .andExpect(jsonPath("$.leaseExpiresAt").value(nullValue()));

        claim(id, WORKER_B, 2);

        mockMvc.perform(get("/api/v1/tasks/{id}/attempts", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.attempts", hasSize(2)))
                .andExpect(jsonPath("$.attempts[0].attempt").value(1))
                .andExpect(jsonPath("$.attempts[0].workerId").value(WORKER_A.toString()))
                .andExpect(jsonPath("$.attempts[0].endedAt").value(notNullValue()))
                .andExpect(jsonPath("$.attempts[0].outcome").value("LEASE_EXPIRED"))
                .andExpect(jsonPath("$.attempts[1].attempt").value(2))
                .andExpect(jsonPath("$.attempts[1].workerId").value(WORKER_B.toString()))
                .andExpect(jsonPath("$.attempts[1].endedAt").value(nullValue()))
                .andExpect(jsonPath("$.attempts[1].outcome").value(nullValue()));
    }

    @Test
    void cancelAfterAReapKeepsTheExpiredAttempt() throws Exception {
        UUID id = createNoop();
        claim(id, WORKER_A, 1);
        expireLease(id);
        assertEquals(List.of(new ReapedTask(id, 1, WORKER_A, true)), taskService.reapExpiredLeases(10));

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"))
                .andExpect(jsonPath("$.attempt").value(1))
                .andExpect(jsonPath("$.workerId").value(nullValue()));

        mockMvc.perform(get("/api/v1/tasks/{id}/attempts", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.attempts", hasSize(1)))
                .andExpect(jsonPath("$.attempts[0].attempt").value(1))
                .andExpect(jsonPath("$.attempts[0].workerId").value(WORKER_A.toString()))
                .andExpect(jsonPath("$.attempts[0].outcome").value("LEASE_EXPIRED"));
    }

    @Test
    void missingTaskIsNotFound() throws Exception {
        UUID missing = UUID.randomUUID();

        mockMvc.perform(get("/api/v1/tasks/{id}/attempts", missing))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.detail").value(containsString(missing.toString())));
    }

    private UUID createNoop() throws Exception {
        MvcResult created = mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"taskType\":\"NOOP\",\"payload\":{}}"))
                .andExpect(status().isCreated())
                .andReturn();
        return UUID.fromString(JsonPath.read(created.getResponse().getContentAsString(), "$.id"));
    }

    private Task claim(UUID id, UUID workerId, int attempt) {
        Task claimed = taskService.claimTask(workerId);
        assertNotNull(claimed);
        assertEquals(id, claimed.id());
        assertEquals(attempt, claimed.attempt());
        return claimed;
    }

    private String getTask(UUID id) throws Exception {
        return mockMvc.perform(get("/api/v1/tasks/{id}", id))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
    }

    private void expireLease(UUID id) {
        jdbcClient.sql("UPDATE tasks SET lease_expires_at = NOW() - INTERVAL '1 hour' WHERE id = :id")
                .param("id", id)
                .update();
    }
}
