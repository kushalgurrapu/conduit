package com.kushal.workflow.task;

import com.kushal.workflow.support.PostgresContainerSupport;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Applies V5 to a database that stopped at V4. The inserts are legal V4 rows:
 * {@code task_type} is required, a pending row has no owner, and a terminal
 * row has {@code finished_at}.
 */
class V5MigrationTest extends PostgresContainerSupport {

    private static final String DATABASE = "v5_migration";

    @Test
    void v5BackfillsRunningLeasesAndReplacesTheOwnerCheck() throws Exception {
        recreateDatabase(DATABASE);
        String url = jdbcUrl(DATABASE);

        Flyway.configure()
                .dataSource(url, postgres.getUsername(), postgres.getPassword())
                .locations("classpath:db/migration")
                .target("4")
                .load()
                .migrate();

        UUID pendingId = UUID.randomUUID();
        UUID runningId = UUID.randomUUID();
        UUID completedId = UUID.randomUUID();
        UUID runningWorker = UUID.randomUUID();
        Instant runningClaimedAt = Instant.parse("2020-02-02T00:00:00Z");
        Instant completedAt = Instant.parse("2020-03-01T00:00:00Z");

        try (Connection connection = open(url)) {
            insert(connection, pendingId, "PENDING", "NOOP",
                    Instant.parse("2020-01-01T00:00:00Z"),
                    null,
                    null,
                    Instant.parse("2020-01-03T00:00:00Z"),
                    null,
                    null,
                    null);
            insert(connection, runningId, "RUNNING", "NOOP",
                    Instant.parse("2020-02-01T00:00:00Z"),
                    runningClaimedAt,
                    runningWorker,
                    Instant.parse("2020-02-03T00:00:00Z"),
                    null,
                    null,
                    null);
            insert(connection, completedId, "COMPLETED", "NOOP",
                    Instant.parse("2020-01-01T00:00:00Z"),
                    Instant.parse("2020-02-01T00:00:00Z"),
                    UUID.randomUUID(),
                    completedAt,
                    completedAt,
                    "{\"done\":true}",
                    null);
        }

        Flyway.configure()
                .dataSource(url, postgres.getUsername(), postgres.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();

        try (Connection connection = open(url)) {
            assertEquals(Set.of(
                    "tasks_finished_at_iff_terminal_check",
                    "tasks_pending_has_no_owner_check",
                    "tasks_running_has_owner_check",
                    "tasks_status_check",
                    "tasks_task_type_format_check"
            ), checkConstraintNames(connection));
            String definition = constraintDefinition(connection, "tasks_running_has_owner_check");
            assertTrue(definition.contains("worker_id"), definition);
            assertTrue(definition.contains("claimed_at"), definition);
            assertTrue(definition.contains("lease_expires_at"), definition);
            assertEquals(1, constraintCount(connection, "tasks_running_has_owner_check"));

            String index = indexDefinition(connection, "idx_tasks_running_lease");
            assertTrue(index.contains("lease_expires_at"), index);
            assertTrue(index.contains("RUNNING"), index);

            Row pending = read(connection, pendingId);
            assertEquals(0, pending.attempt());
            assertNull(pending.leaseExpiresAt());
            assertNull(pending.lastHeartbeatAt());

            Row running = read(connection, runningId);
            assertEquals(0, running.attempt());
            assertEquals(runningClaimedAt, running.leaseExpiresAt());
            assertNull(running.lastHeartbeatAt());
            assertEquals("RUNNING", running.status());

            Row completed = read(connection, completedId);
            assertEquals(0, completed.attempt());
            assertNull(completed.leaseExpiresAt());
            assertEquals(completedAt, completed.finishedAt());

            SQLException rejected = assertThrows(SQLException.class, () -> insert(
                    connection,
                    UUID.randomUUID(),
                    "RUNNING",
                    "NOOP",
                    Instant.parse("2020-04-01T00:00:00Z"),
                    Instant.parse("2020-04-02T00:00:00Z"),
                    UUID.randomUUID(),
                    Instant.parse("2020-04-03T00:00:00Z"),
                    null,
                    null,
                    null));
            assertEquals("23514", rejected.getSQLState());
        }
    }

    private static void recreateDatabase(String name) throws SQLException {
        try (Connection admin = open(jdbcUrl("postgres"));
             Statement statement = admin.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)");
            statement.execute("CREATE DATABASE " + name);
        }
    }

    private static Connection open(String url) throws SQLException {
        Connection connection = DriverManager.getConnection(
                url, postgres.getUsername(), postgres.getPassword());
        connection.setAutoCommit(true);
        return connection;
    }

    private static String jdbcUrl(String database) {
        return "jdbc:postgresql://" + postgres.getHost() + ":"
                + postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT) + "/" + database;
    }

    private static void insert(
            Connection connection,
            UUID id,
            String status,
            String taskType,
            Instant createdAt,
            Instant claimedAt,
            UUID workerId,
            Instant updatedAt,
            Instant finishedAt,
            String result,
            String error
    ) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO tasks (
                    id, task_type, status, payload, result, error,
                    created_at, claimed_at, worker_id, updated_at, finished_at
                )
                VALUES (?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), ?, ?, ?, ?, ?, ?)
                """)) {
            statement.setObject(1, id);
            statement.setString(2, taskType);
            statement.setString(3, status);
            statement.setString(4, "{}");
            if (result == null) {
                statement.setNull(5, Types.VARCHAR);
            } else {
                statement.setString(5, result);
            }
            if (error == null) {
                statement.setNull(6, Types.VARCHAR);
            } else {
                statement.setString(6, error);
            }
            statement.setObject(7, offset(createdAt));
            if (claimedAt == null) {
                statement.setNull(8, Types.TIMESTAMP_WITH_TIMEZONE);
            } else {
                statement.setObject(8, offset(claimedAt));
            }
            if (workerId == null) {
                statement.setNull(9, Types.OTHER);
            } else {
                statement.setObject(9, workerId);
            }
            statement.setObject(10, offset(updatedAt));
            if (finishedAt == null) {
                statement.setNull(11, Types.TIMESTAMP_WITH_TIMEZONE);
            } else {
                statement.setObject(11, offset(finishedAt));
            }
            statement.executeUpdate();
        }
    }

    private static Set<String> checkConstraintNames(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'tasks'::regclass
                  AND contype = 'c'
                """);
             ResultSet result = statement.executeQuery()) {
            Set<String> names = new HashSet<>();
            while (result.next()) {
                names.add(result.getString("conname"));
            }
            return names;
        }
    }

    private static String constraintDefinition(Connection connection, String name) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conrelid = 'tasks'::regclass
                  AND conname = ?
                """)) {
            statement.setString(1, name);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new AssertionError("missing constraint " + name);
                }
                return result.getString(1);
            }
        }
    }

    private static int constraintCount(Connection connection, String name) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT count(*)
                FROM pg_constraint
                WHERE conrelid = 'tasks'::regclass
                  AND conname = ?
                """)) {
            statement.setString(1, name);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getInt(1);
            }
        }
    }

    private static String indexDefinition(Connection connection, String name) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT indexdef
                FROM pg_indexes
                WHERE tablename = 'tasks'
                  AND indexname = ?
                """)) {
            statement.setString(1, name);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new AssertionError("missing index " + name);
                }
                return result.getString(1);
            }
        }
    }

    private static Row read(Connection connection, UUID id) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT status, attempt, lease_expires_at, last_heartbeat_at, finished_at
                FROM tasks
                WHERE id = ?
                """)) {
            statement.setObject(1, id);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new AssertionError("missing task " + id);
                }
                return new Row(
                        result.getString("status"),
                        result.getInt("attempt"),
                        instant(result, "lease_expires_at"),
                        instant(result, "last_heartbeat_at"),
                        instant(result, "finished_at")
                );
            }
        }
    }

    private static OffsetDateTime offset(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Instant instant(ResultSet result, String column) throws SQLException {
        OffsetDateTime value = result.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private record Row(
            String status,
            int attempt,
            Instant leaseExpiresAt,
            Instant lastHeartbeatAt,
            Instant finishedAt
    ) {
    }
}
