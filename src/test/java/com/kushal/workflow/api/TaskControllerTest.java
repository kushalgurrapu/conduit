package com.kushal.workflow.api;

import com.kushal.workflow.task.CancelResult;
import com.kushal.workflow.task.Task;
import com.kushal.workflow.task.TaskNotFoundException;
import com.kushal.workflow.task.TaskService;
import com.kushal.workflow.task.TaskStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpHeaders.LOCATION;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP status and {@code ProblemDetail} shape. The service is a mock, so
 * these tests do not open a database. The real create/get/cancel path is
 * {@link TaskApiTest}.
 */
@WebMvcTest(controllers = TaskController.class)
class TaskControllerTest {

    private static final Instant CREATED = Instant.parse("2026-10-06T12:00:00Z");
    private static final Instant UPDATED = Instant.parse("2026-10-06T12:00:01Z");
    private static final Instant FINISHED = Instant.parse("2026-10-06T12:00:02Z");

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private TaskService taskService;

    @Test
    void createReturns201WithLocationAndJsonFields() throws Exception {
        UUID id = UUID.fromString("00000000-0000-0000-0000-0000000000c1");
        when(taskService.createTask(eq("ECHO"), anyString())).thenReturn(
                task(id, "ECHO", TaskStatus.PENDING, "{\"message\":\"hi\"}", null, null, null, null, null));

        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"taskType\":\"ECHO\",\"payload\":{\"message\":\"hi\"}}"))
                .andExpect(status().isCreated())
                .andExpect(header().string(LOCATION, "/api/v1/tasks/" + id))
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.taskType").value("ECHO"))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.payload.message").value("hi"))
                .andExpect(jsonPath("$.result").value(nullValue()))
                .andExpect(jsonPath("$.claimedAt").value(nullValue()))
                .andExpect(jsonPath("$.workerId").value(nullValue()));
    }

    @Test
    void createAcceptsATypeThatHasNoHandler() throws Exception {
        UUID id = UUID.fromString("00000000-0000-0000-0000-0000000000c2");
        when(taskService.createTask(eq("NO_SUCH"), anyString())).thenReturn(
                task(id, "NO_SUCH", TaskStatus.PENDING, "{}", null, null, null, null, null));

        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"taskType\":\"NO_SUCH\",\"payload\":{}}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.taskType").value("NO_SUCH"))
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    void createRejectsATaskTypeThatFailsTheFormatCheck() throws Exception {
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"taskType\":\"echo\",\"payload\":{}}"))
                .andExpect(status().isBadRequest())
                .andExpect(problemDetail(400));

        verify(taskService, never()).createTask(anyString(), anyString());
    }

    @Test
    void createRejectsJsonNullAndNulBytes() throws Exception {
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"taskType\":\"ECHO\",\"payload\":null}"))
                .andExpect(status().isBadRequest())
                .andExpect(problemDetail(400));

        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"taskType\":\"ECHO\",\"payload\":{\"note\":\"a\\u0000b\"}}"))
                .andExpect(status().isBadRequest())
                .andExpect(problemDetail(400));

        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"taskType\":\"ECHO\",\"payload\":{\"bad\\u0000key\":1}}"))
                .andExpect(status().isBadRequest())
                .andExpect(problemDetail(400));

        verify(taskService, never()).createTask(anyString(), anyString());
    }

    @Test
    void unreadableJsonIsProblemDetail() throws Exception {
        mockMvc.perform(post("/api/v1/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{"))
                .andExpect(status().isBadRequest())
                .andExpect(problemDetail(400));

        verify(taskService, never()).createTask(anyString(), anyString());
    }

    @Test
    void getReturnsTheTaskOrNotFound() throws Exception {
        UUID id = UUID.fromString("00000000-0000-0000-0000-0000000000c3");
        when(taskService.getTask(id)).thenReturn(
                task(id, "ECHO", TaskStatus.COMPLETED, "{\"message\":\"hi\"}", "{\"message\":\"hi\"}",
                        null, null, FINISHED, null));

        mockMvc.perform(get("/api/v1/tasks/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.result.message").value("hi"))
                .andExpect(jsonPath("$.finishedAt").value(FINISHED.toString()))
                .andExpect(jsonPath("$.workerId").value(nullValue()));

        UUID missing = UUID.fromString("00000000-0000-0000-0000-0000000000c4");
        when(taskService.getTask(missing)).thenThrow(new TaskNotFoundException(missing));

        mockMvc.perform(get("/api/v1/tasks/{id}", missing))
                .andExpect(status().isNotFound())
                .andExpect(problemDetail(404))
                .andExpect(jsonPath("$.detail").value(containsString(missing.toString())));
    }

    @Test
    void badTaskIdIsProblemDetail() throws Exception {
        mockMvc.perform(get("/api/v1/tasks/not-a-uuid"))
                .andExpect(status().isBadRequest())
                .andExpect(problemDetail(400));
    }

    @Test
    void cancelIs200ForPendingOrAlreadyCancelledAnd409Otherwise() throws Exception {
        UUID pendingId = UUID.fromString("00000000-0000-0000-0000-0000000000c5");
        Task cancelled = task(pendingId, "ECHO", TaskStatus.CANCELLED, "{}", null, null, null, FINISHED, null);
        when(taskService.cancelTask(pendingId)).thenReturn(new CancelResult.Cancelled(cancelled));

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", pendingId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"))
                .andExpect(jsonPath("$.finishedAt").value(FINISHED.toString()))
                .andExpect(jsonPath("$.workerId").value(nullValue()));

        UUID againId = UUID.fromString("00000000-0000-0000-0000-0000000000c6");
        Task already = task(againId, "ECHO", TaskStatus.CANCELLED, "{}", null, null, null, FINISHED, null);
        when(taskService.cancelTask(againId)).thenReturn(new CancelResult.AlreadyCancelled(already));

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", againId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        UUID runningId = UUID.fromString("00000000-0000-0000-0000-0000000000c7");
        when(taskService.cancelTask(runningId)).thenReturn(new CancelResult.NotCancellable(TaskStatus.RUNNING));

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", runningId))
                .andExpect(status().isConflict())
                .andExpect(problemDetail(409))
                .andExpect(jsonPath("$.detail").value(containsString("RUNNING")));

        UUID completedId = UUID.fromString("00000000-0000-0000-0000-0000000000c8");
        when(taskService.cancelTask(completedId)).thenReturn(new CancelResult.NotCancellable(TaskStatus.COMPLETED));

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", completedId))
                .andExpect(status().isConflict())
                .andExpect(problemDetail(409))
                .andExpect(jsonPath("$.detail").value(containsString("COMPLETED")));

        UUID missing = UUID.fromString("00000000-0000-0000-0000-0000000000c9");
        when(taskService.cancelTask(missing)).thenThrow(new TaskNotFoundException(missing));

        mockMvc.perform(post("/api/v1/tasks/{id}/cancel", missing))
                .andExpect(status().isNotFound())
                .andExpect(problemDetail(404));
    }

    @Test
    void databaseErrorIsGenericProblemDetail() throws Exception {
        UUID id = UUID.fromString("00000000-0000-0000-0000-0000000000ca");
        when(taskService.getTask(id)).thenThrow(
                new DataRetrievalFailureException("select secret_column from tasks"));

        mockMvc.perform(get("/api/v1/tasks/{id}", id))
                .andExpect(status().isInternalServerError())
                .andExpect(problemDetail(500))
                .andExpect(jsonPath("$.detail").value("The request could not be completed"))
                .andExpect(content().string(not(containsString("secret_column"))));
    }

    private static org.springframework.test.web.servlet.ResultMatcher problemDetail(int status) {
        return result -> {
            content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON).match(result);
            jsonPath("$.status").value(status).match(result);
            jsonPath("$.title").exists().match(result);
            jsonPath("$.detail").exists().match(result);
        };
    }

    private static Task task(
            UUID id,
            String taskType,
            TaskStatus status,
            String payload,
            String result,
            String error,
            Instant claimedAt,
            Instant finishedAt,
            UUID workerId) {
        return new Task(
                id,
                taskType,
                status,
                payload,
                result,
                error,
                CREATED,
                UPDATED,
                claimedAt,
                finishedAt,
                workerId);
    }
}
