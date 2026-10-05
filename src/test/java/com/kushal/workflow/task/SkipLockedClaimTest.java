package com.kushal.workflow.task;

import com.kushal.workflow.support.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Proves that {@link TaskService#claimTask} uses {@code SKIP LOCKED} rather than
 * waiting on a row lock.
 *
 * <p>{@code FOR UPDATE} locks the selected rows until the transaction commits or
 * rolls back. Another transaction that also uses {@code FOR UPDATE} waits for
 * that lock. {@code SKIP LOCKED} changes the wait into a skip: locked rows are
 * ignored and the query continues with the next matching row, or returns nothing.
 *
 * <p>The lock is taken on a raw JDBC connection and left uncommitted. The claim
 * runs through Spring on a different pooled connection, with
 * {@code statement_timeout} set to two seconds. If {@code SKIP LOCKED} is
 * removed, that claim blocks on the lock and PostgreSQL cancels it. The
 * {@link Timeout} on each test is only a backstop for a timeout set on the
 * wrong connection.
 *
 * <p>{@link ConcurrentTaskClaimTest} cannot prove this. Waiting on a lock also
 * produces unique claims. This test forces one known row to be locked before
 * the claim starts, so a waiting query and a skipping query behave differently.
 */
@SpringBootTest
class SkipLockedClaimTest extends PostgresIntegrationTest {

    private static final String STATEMENT_TIMEOUT = "2s";

    @Autowired
    private TaskService taskService;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void claimSkipsLockedOlderTask() throws Exception {
        UUID olderId = taskService.createTask("NOOP", "{\"test\":\"skip-locked-older\"}").id();
        UUID newerId = taskService.createTask("NOOP", "{\"test\":\"skip-locked-newer\"}").id();
        List<UUID> createdIds = List.of(olderId, newerId);
        setCreatedAt(olderId, OffsetDateTime.parse("2020-01-01T00:00:00Z"));
        setCreatedAt(newerId, OffsetDateTime.parse("2020-01-01T00:00:01Z"));

        Connection lock = dataSource.getConnection();
        lock.setAutoCommit(false);
        try {
            lockTask(lock, olderId);
            int lockPid = backendPid(lock);

            UUID workerId = UUID.randomUUID();
            Task claimed = claimWithStatementTimeout(workerId, lockPid);

            assertNotNull(claimed, "claim should skip the locked row and return the next pending task");
            assertEquals(newerId, claimed.id());
            assertEquals(TaskStatus.RUNNING, claimed.status());
            assertEquals(workerId, claimed.workerId());
            assertEquals("PENDING", statusOf(olderId));
        } finally {
            releaseLock(lock);
            deleteTasks(createdIds);
        }
    }

    /**
     * Locks every pending row, including the one this test inserts. The claim
     * query scans the whole table, so an unlocked leftover row would be claimed
     * instead. The lock transaction rolls back and does not change those rows.
     */
    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void claimReturnsNullWhenPendingTasksAreLocked() throws Exception {
        UUID onlyId = taskService.createTask("NOOP", "{\"test\":\"skip-locked-only\"}").id();
        List<UUID> createdIds = List.of(onlyId);

        Connection lock = dataSource.getConnection();
        lock.setAutoCommit(false);
        try {
            lockPendingTasks(lock);
            int lockPid = backendPid(lock);

            Task claimed = claimWithStatementTimeout(UUID.randomUUID(), lockPid);

            assertNull(claimed, "claim should return null instead of waiting for the locked task");
            assertEquals("PENDING", statusOf(onlyId));
        } finally {
            releaseLock(lock);
            deleteTasks(createdIds);
        }
    }

    private Task claimWithStatementTimeout(UUID workerId, int lockPid) {
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        return transaction.execute(status -> {
            jdbcClient.sql("SET LOCAL statement_timeout = '" + STATEMENT_TIMEOUT + "'").update();
            Integer claimPid = jdbcClient.sql("SELECT pg_backend_pid()").query(Integer.class).single();
            assertNotEquals(lockPid, claimPid,
                    "the claim must run on a different database connection from the lock");
            return taskService.claimTask(workerId);
        });
    }

    private void lockTask(Connection connection, UUID id) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT id
                FROM tasks
                WHERE id = ?
                FOR UPDATE
                """)) {
            statement.setObject(1, id);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("task to lock was not visible: " + id);
                }
            }
        }
    }

    private void lockPendingTasks(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("""
                     SELECT id
                     FROM tasks
                     WHERE status = 'PENDING'
                     FOR UPDATE
                     """)) {
            if (!result.next()) {
                throw new IllegalStateException("expected at least one pending task to lock");
            }
        }
    }

    private void setCreatedAt(UUID id, OffsetDateTime createdAt) {
        jdbcClient.sql("""
                        UPDATE tasks
                        SET created_at = :createdAt
                        WHERE id = :id
                        """)
                .param("createdAt", createdAt)
                .param("id", id)
                .update();
    }

    private String statusOf(UUID id) {
        return jdbcClient.sql("SELECT status FROM tasks WHERE id = :id")
                .param("id", id)
                .query(String.class)
                .single();
    }

    private int backendPid(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT pg_backend_pid()")) {
            result.next();
            return result.getInt(1);
        }
    }

    private void releaseLock(Connection connection) throws SQLException {
        try {
            connection.rollback();
        } finally {
            connection.close();
        }
    }

    private void deleteTasks(List<UUID> ids) {
        for (UUID id : ids) {
            jdbcClient.sql("DELETE FROM tasks WHERE id = :id")
                    .param("id", id)
                    .update();
        }
    }
}
