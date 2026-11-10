package com.kushal.workflow.task;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Repository
public class TaskRepository {

    /**
     * Every read of a task lists these expressions. {@code RETURNING *} is not
     * used, so a new column cannot silently change the mapped row.
     */
    static final String TASK_COLUMNS = taskColumns(null);

    private static final RowMapper<Task> TASK_ROW_MAPPER = (rs, rowNum) -> new Task(
            rs.getObject("id", UUID.class),
            rs.getString("task_type"),
            TaskStatus.valueOf(rs.getString("status")),
            rs.getString("payload"),
            rs.getString("result"),
            rs.getString("error"),
            instant(rs, "created_at"),
            instant(rs, "updated_at"),
            instant(rs, "claimed_at"),
            instant(rs, "finished_at"),
            rs.getObject("worker_id", UUID.class),
            rs.getInt("attempt"),
            instant(rs, "lease_expires_at"),
            instant(rs, "last_heartbeat_at")
    );

    private final JdbcClient jdbcClient;

    public TaskRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    public Task createTask(UUID id, String taskType, String payload) {
        return jdbcClient.sql("""
                INSERT INTO tasks (id, task_type, status, payload)
                VALUES (:id, :taskType, 'PENDING', CAST(:payload AS jsonb))
                RETURNING
                %s
                """.formatted(TASK_COLUMNS))
                .param("id", id)
                .param("taskType", taskType)
                .param("payload", payload)
                .query(TASK_ROW_MAPPER)
                .single();
    }

    public Task claimTask(UUID workerId, long leaseMicros) {
        return jdbcClient.sql("""
                WITH next_task AS (
                    SELECT id
                    FROM tasks
                    WHERE status = 'PENDING'
                    ORDER BY created_at
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE tasks t
                SET status = 'RUNNING',
                    claimed_at = NOW(),
                    worker_id = :workerId,
                    updated_at = NOW(),
                    lease_expires_at = NOW() + (:leaseMicros * INTERVAL '1 microsecond'),
                    attempt = attempt + 1,
                    last_heartbeat_at = NULL
                FROM next_task
                WHERE t.id = next_task.id
                  AND t.status = 'PENDING'
                RETURNING
                %s
                """.formatted(taskColumns("t")))
                .param("workerId", workerId)
                .param("leaseMicros", leaseMicros)
                .query(TASK_ROW_MAPPER)
                .optional()
                .orElse(null);
    }

    /**
     * Ids this pool still owns after shutdown. Used only to log them.
     * An empty worker list is not a query: an {@code IN ()} is invalid SQL.
     */
    public List<UUID> findRunningIdsForWorkers(Collection<UUID> workerIds) {
        if (workerIds == null || workerIds.isEmpty()) {
            return List.of();
        }
        return jdbcClient.sql("""
                SELECT id
                FROM tasks
                WHERE status = 'RUNNING'
                  AND worker_id IN (:workerIds)
                ORDER BY id
                """)
                .param("workerIds", workerIds)
                .query((rs, rowNum) -> rs.getObject("id", UUID.class))
                .list();
    }

    public Optional<Task> findById(UUID id) {
        return jdbcClient.sql("""
                SELECT
                %s
                FROM tasks
                WHERE id = :id
                """.formatted(TASK_COLUMNS))
                .param("id", id)
                .query(TASK_ROW_MAPPER)
                .optional();
    }

    /**
     * Moves the lease forward and records a heartbeat. The guard is the
     * task, {@code RUNNING}, the worker, and the attempt from claim. A past
     * {@code lease_expires_at} still matches: expiry is not a new owner.
     * {@code updated_at}, {@code attempt}, {@code status}, and
     * {@code worker_id} stay as they were. Zero rows is {@code false}.
     */
    public boolean extendLease(UUID id, UUID workerId, int attempt, long leaseMicros) {
        return jdbcClient.sql("""
                UPDATE tasks
                SET lease_expires_at = NOW() + (:leaseMicros * INTERVAL '1 microsecond'),
                    last_heartbeat_at = NOW()
                WHERE id = :id
                  AND status = 'RUNNING'
                  AND worker_id = :workerId
                  AND attempt = :attempt
                RETURNING id
                """)
                .param("id", id)
                .param("workerId", workerId)
                .param("attempt", attempt)
                .param("leaseMicros", leaseMicros)
                .query((rs, rowNum) -> rs.getObject("id", UUID.class))
                .optional()
                .isPresent();
    }

    /**
     * One update per transition. The {@code WHERE} clause is what makes the
     * transition legal, including the attempt the worker claimed. An empty
     * result means the row did not change; the service names that outcome
     * and does not try the update again.
     */
    public Optional<Task> completeTask(UUID id, UUID workerId, int attempt, String result) {
        return jdbcClient.sql("""
                UPDATE tasks
                SET status = 'COMPLETED',
                    result = CAST(:result AS jsonb),
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE id = :id
                  AND status = 'RUNNING'
                  AND worker_id = :workerId
                  AND attempt = :attempt
                RETURNING
                %s
                """.formatted(TASK_COLUMNS))
                .param("id", id)
                .param("workerId", workerId)
                .param("attempt", attempt)
                .param("result", result)
                .query(TASK_ROW_MAPPER)
                .optional();
    }

    public Optional<Task> failTask(UUID id, UUID workerId, int attempt, String error) {
        return jdbcClient.sql("""
                UPDATE tasks
                SET status = 'FAILED',
                    error = left(:error, 4000),
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE id = :id
                  AND status = 'RUNNING'
                  AND worker_id = :workerId
                  AND attempt = :attempt
                RETURNING
                %s
                """.formatted(TASK_COLUMNS))
                .param("id", id)
                .param("workerId", workerId)
                .param("attempt", attempt)
                .param("error", error)
                .query(TASK_ROW_MAPPER)
                .optional();
    }

    public Optional<Task> cancelTask(UUID id) {
        return jdbcClient.sql("""
                UPDATE tasks
                SET status = 'CANCELLED',
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE id = :id
                  AND status = 'PENDING'
                RETURNING
                %s
                """.formatted(TASK_COLUMNS))
                .param("id", id)
                .query(TASK_ROW_MAPPER)
                .optional();
    }

    private static String taskColumns(String alias) {
        String prefix = alias == null ? "" : alias + ".";
        return Stream.of(
                "id",
                "task_type",
                "status",
                "payload::text AS payload",
                "result::text AS result",
                "error",
                "created_at",
                "updated_at",
                "claimed_at",
                "finished_at",
                "worker_id",
                "attempt",
                "lease_expires_at",
                "last_heartbeat_at"
        ).map(column -> prefix + column).collect(Collectors.joining(", "));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
