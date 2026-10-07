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
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.UUID;

import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
                .andReturn();

        UUID first = UUID.fromString(JsonPath.read(created.getResponse().getContentAsString(), "$.id"));
        assertEquals("/api/v1/tasks/" + first, created.getResponse().getHeader(LOCATION));

        mockMvc.perform(get("/api/v1/tasks/{id}", first))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.payload.n").value(1));

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
    void cancelOfARunningTaskIsConflictAndLeavesTheRow() throws Exception {
        MvcResult created = mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"taskType\":\"NOOP\",\"payload\":{}}"))
                .andExpect(status().isCreated())
                .andReturn();
        UUID id = UUID.fromString(JsonPath.read(created.getResponse().getContentAsString(), "$.id"));
        UUID workerId = UUID.fromString("00000000-0000-0000-0000-0000000000b1");
        assertEquals(id, taskService.claimTask(workerId).id());

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", id))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.title").exists())
                .andExpect(jsonPath("$.detail").exists());

        mockMvc.perform(get("/api/v1/tasks/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(TaskStatus.RUNNING.name()))
                .andExpect(jsonPath("$.workerId").value(workerId.toString()))
                .andExpect(jsonPath("$.claimedAt").isNotEmpty());
    }
}
