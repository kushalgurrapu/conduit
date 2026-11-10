package com.kushal.workflow.task;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Writes {@code task_attempts} from the {@code tasks} row it records. Each
 * statement copies the owner and timestamps from {@code tasks} instead of
 * taking them from the caller, and must run in the transaction of the
 * {@code tasks} write it records.
 */
@Repository
public class TaskAttemptRepository {

    private static final RowMapper<TaskAttempt> ATTEMPT_ROW_MAPPER = (rs, rowNum) -> {
        String outcome = rs.getString("outcome");
        return new TaskAttempt(
                rs.getObject("task_id", UUID.class),
                rs.getInt("attempt"),
                rs.getObject("worker_id", UUID.class),
                instant(rs, "started_at"),
                instant(rs, "ended_at"),
                outcome == null ? null : AttemptOutcome.valueOf(outcome),
                rs.getString("error"));
    };

    private final JdbcClient jdbcClient;

    public TaskAttemptRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    /**
     * Opens the attempt the claim just wrote. {@code false} means a row for
     * that attempt already exists (written by hand) or the task no longer
     * matches the claim. The existing row is left as it is.
     */
    public boolean recordStart(UUID taskId, UUID workerId, int attempt) {
        return jdbcClient.sql("""
                INSERT INTO task_attempts (task_id, attempt, worker_id, started_at)
                SELECT id, attempt, worker_id, claimed_at
                FROM tasks
                WHERE id = :taskId
                  AND status = 'RUNNING'
                  AND worker_id = :workerId
                  AND attempt = :attempt
                ON CONFLICT (task_id, attempt) DO NOTHING
                """)
                .param("taskId", taskId)
                .param("workerId", workerId)
                .param("attempt", attempt)
                .update() == 1;
    }

    /**
     * Closes the open attempt with the finish just written, or creates a
     * closed row when none exists (fixtures, rows claimed before V6, manual
     * requeues). {@code false} means the attempt was already closed; it is
     * not overwritten.
     */
    public boolean recordFinish(UUID taskId, int attempt, AttemptOutcome outcome) {
        return jdbcClient.sql("""
                INSERT INTO task_attempts (task_id, attempt, worker_id, started_at, ended_at, outcome, error)
                SELECT id, attempt, worker_id, claimed_at, finished_at, :outcome, error
                FROM tasks
                WHERE id = :taskId
                  AND attempt = :attempt
                  AND status = :outcome
                ON CONFLICT (task_id, attempt) DO UPDATE
                    SET ended_at = EXCLUDED.ended_at,
                        outcome = EXCLUDED.outcome,
                        error = EXCLUDED.error
                    WHERE task_attempts.outcome IS NULL
                """)
                .param("taskId", taskId)
                .param("attempt", attempt)
                .param("outcome", outcome.name())
                .update() == 1;
    }

    public List<TaskAttempt> findByTaskId(UUID taskId) {
        return jdbcClient.sql("""
                SELECT task_id, attempt, worker_id, started_at, ended_at, outcome, error
                FROM task_attempts
                WHERE task_id = :taskId
                ORDER BY attempt
                """)
                .param("taskId", taskId)
                .query(ATTEMPT_ROW_MAPPER)
                .list();
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
