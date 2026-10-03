package durable_workflow_engine.task;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public class TaskRepository {

    private final JdbcClient jdbcClient;

    public TaskRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    public void createTask(UUID id, String status, String payload) {
        jdbcClient.sql("""
                INSERT INTO tasks (id, status, payload)
                VALUES (:id, :status, CAST(:payload AS jsonb))
                """)
                .param("id", id)
                .param("status", status)
                .param("payload", payload)
                .update();
    }

    public Task claimTask(UUID workerId) {
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
                worker_id = :workerId
            FROM next_task
            WHERE t.id = next_task.id
            RETURNING
                t.id,
                t.status,
                t.payload,
                t.created_at,
                t.claimed_at,
                t.worker_id
            """)
            .param("workerId", workerId)
            .query(Task.class)
            .optional()
            .orElse(null);
    }
}