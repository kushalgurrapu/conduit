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

/**
 * Applies V4 to a database that stopped at V3, on a second database so the
 * shared Testcontainers database is never pinned with {@code spring.flyway.target}.
 *
 * <p>The pending row copies the V1 artifact: {@code claimed_at} was filled in
 * even though nobody claimed it. The running row has an owner, so the new
 * check can succeed. The completed row has no {@code finished_at} yet, which
 * is what every pre-V4 terminal row looks like.
 */
class V4MigrationTest extends PostgresContainerSupport {

    private static final String DATABASE = "v4_migration";

    @Test
    void v4BackfillsV3RowsAndAddsNamedChecks() throws Exception {
        recreateDatabase(DATABASE);
        String url = jdbcUrl(DATABASE);

        Flyway.configure()
                .dataSource(url, postgres.getUsername(), postgres.getPassword())
                .locations("classpath:db/migration")
                .target("3")
                .load()
                .migrate();

        UUID pendingId = UUID.randomUUID();
        UUID runningId = UUID.randomUUID();
        UUID completedId = UUID.randomUUID();
        UUID runningWorkerId = UUID.randomUUID();
        Instant pendingClaimedAt = Instant.parse("2020-01-02T00:00:00Z");
        Instant runningClaimedAt = Instant.parse("2020-02-02T00:00:00Z");
        Instant completedCreatedAt = Instant.parse("2020-01-01T00:00:00Z");
        Instant completedClaimedAt = Instant.parse("2020-02-01T00:00:00Z");
        Instant completedUpdatedAt = Instant.parse("2020-03-01T00:00:00Z");

        try (Connection connection = open(url)) {
            insert(connection, pendingId, "PENDING",
                    Instant.parse("2020-01-01T00:00:00Z"),
                    pendingClaimedAt,
                    null,
                    Instant.parse("2020-01-03T00:00:00Z"));
            insert(connection, runningId, "RUNNING",
                    Instant.parse("2020-02-01T00:00:00Z"),
                    runningClaimedAt,
                    runningWorkerId,
                    Instant.parse("2020-02-03T00:00:00Z"));
            insert(connection, completedId, "COMPLETED",
                    completedCreatedAt,
                    completedClaimedAt,
                    UUID.randomUUID(),
                    completedUpdatedAt);
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
            assertNull(columnDefault(connection, "task_type"));

            Row pending = read(connection, pendingId);
            assertEquals("NOOP", pending.taskType());
            assertEquals("PENDING", pending.status());
            assertNull(pending.workerId());
            assertNull(pending.claimedAt());
            assertNull(pending.finishedAt());

            Row running = read(connection, runningId);
            assertEquals("NOOP", running.taskType());
            assertEquals("RUNNING", running.status());
            assertEquals(runningWorkerId, running.workerId());
            assertEquals(runningClaimedAt, running.claimedAt());
            assertNull(running.finishedAt());

            Row completed = read(connection, completedId);
            assertEquals("NOOP", completed.taskType());
            assertEquals("COMPLETED", completed.status());
            assertEquals(completedUpdatedAt, completed.finishedAt());
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
            Instant createdAt,
            Instant claimedAt,
            UUID workerId,
            Instant updatedAt
    ) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO tasks (
                    id, status, payload, created_at, claimed_at, worker_id, updated_at
                )
                VALUES (?, ?, CAST(? AS jsonb), ?, ?, ?, ?)
                """)) {
            statement.setObject(1, id);
            statement.setString(2, status);
            statement.setString(3, "{}");
            statement.setObject(4, OffsetDateTime.ofInstant(createdAt, ZoneOffset.UTC));
            statement.setObject(5, OffsetDateTime.ofInstant(claimedAt, ZoneOffset.UTC));
            if (workerId == null) {
                statement.setNull(6, Types.OTHER);
            } else {
                statement.setObject(6, workerId);
            }
            statement.setObject(7, OffsetDateTime.ofInstant(updatedAt, ZoneOffset.UTC));
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

    private static String columnDefault(Connection connection, String column) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT column_default
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'tasks'
                  AND column_name = ?
                """)) {
            statement.setString(1, column);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getString("column_default");
            }
        }
    }

    private static Row read(Connection connection, UUID id) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT task_type, status, claimed_at, finished_at, worker_id
                FROM tasks
                WHERE id = ?
                """)) {
            statement.setObject(1, id);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new AssertionError("missing task " + id);
                }
                return new Row(
                        result.getString("task_type"),
                        result.getString("status"),
                        instant(result, "claimed_at"),
                        instant(result, "finished_at"),
                        result.getObject("worker_id", UUID.class)
                );
            }
        }
    }

    private static Instant instant(ResultSet result, String column) throws SQLException {
        OffsetDateTime value = result.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private record Row(
            String taskType,
            String status,
            Instant claimedAt,
            Instant finishedAt,
            UUID workerId
    ) {
    }
}
