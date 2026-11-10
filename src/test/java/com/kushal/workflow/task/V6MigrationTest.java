package com.kushal.workflow.task;

import com.kushal.workflow.support.PostgresContainerSupport;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Applies V6 to a database that stopped at V5. The inserts are legal V5 rows:
 * a pending row has no owner, a terminal row has {@code finished_at}, and a
 * running row has a worker, {@code claimed_at}, and a lease. Only the two
 * running rows are backfilled as open attempts.
 */
class V6MigrationTest extends PostgresContainerSupport {

    private static final String DATABASE = "v6_migration";
    private static final String NEGATIVE_DATABASE = "v6_negative";

    private static final Set<String> V5_TASK_CHECKS = Set.of(
            "tasks_finished_at_iff_terminal_check",
            "tasks_pending_has_no_owner_check",
            "tasks_running_has_owner_check",
            "tasks_status_check",
            "tasks_task_type_format_check");

    @Test
    void v6AddsTheAttemptCheckAndBackfillsRunningRows() throws Exception {
        recreateDatabase(DATABASE);
        String url = jdbcUrl(DATABASE);
        migrate(url, "5");

        UUID pendingId = UUID.randomUUID();
        UUID legacyRunningId = UUID.randomUUID();
        UUID runningId = UUID.randomUUID();
        UUID completedId = UUID.randomUUID();
        UUID failedId = UUID.randomUUID();
        UUID cancelledId = UUID.randomUUID();
        UUID legacyWorker = UUID.randomUUID();
        UUID runningWorker = UUID.randomUUID();
        Instant legacyClaimedAt = Instant.parse("2020-02-02T00:00:00Z");
        Instant runningClaimedAt = Instant.parse("2020-03-02T00:00:00Z");
        Instant created = Instant.parse("2020-01-01T00:00:00Z");
        Instant finished = Instant.parse("2020-04-01T00:00:00Z");

        try (Connection connection = open(url)) {
            insert(connection, pendingId, "PENDING", created, null, null, null, 0, null);
            insert(connection, legacyRunningId, "RUNNING", created, legacyClaimedAt, legacyWorker,
                    null, 0, legacyClaimedAt);
            insert(connection, runningId, "RUNNING", created, runningClaimedAt, runningWorker,
                    null, 2, Instant.parse("2100-01-01T00:00:00Z"));
            insert(connection, completedId, "COMPLETED", created, runningClaimedAt, UUID.randomUUID(),
                    finished, 1, runningClaimedAt);
            insert(connection, failedId, "FAILED", created, runningClaimedAt, UUID.randomUUID(),
                    finished, 1, runningClaimedAt);
            insert(connection, cancelledId, "CANCELLED", created, null, null, finished, 0, null);
        }

        migrate(url, "6");

        try (Connection connection = open(url)) {
            Set<String> expectedTaskChecks = new HashSet<>(V5_TASK_CHECKS);
            expectedTaskChecks.add("tasks_attempt_nonnegative_check");
            assertEquals(expectedTaskChecks, checkConstraintNames(connection, "tasks"));
            assertEquals("CHECK ((attempt >= 0))",
                    constraintDefinitions(connection, "tasks").get("tasks_attempt_nonnegative_check"));

            SQLException negativeAttempt = assertThrows(SQLException.class, () -> execute(connection,
                    "UPDATE tasks SET attempt = -1 WHERE id = '" + pendingId + "'"));
            assertEquals("23514", negativeAttempt.getSQLState());

            Map<String, String> definitions = constraintDefinitions(connection, "task_attempts");
            assertEquals(Set.of(
                    "task_attempts_pkey",
                    "task_attempts_task_id_fkey",
                    "task_attempts_attempt_check",
                    "task_attempts_outcome_check",
                    "task_attempts_ended_iff_outcome_check"
            ), definitions.keySet());
            assertEquals("PRIMARY KEY (task_id, attempt)", definitions.get("task_attempts_pkey"));
            assertEquals("FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE",
                    definitions.get("task_attempts_task_id_fkey"));
            assertEquals("CHECK ((attempt >= 0))", definitions.get("task_attempts_attempt_check"));
            String outcomeCheck = definitions.get("task_attempts_outcome_check");
            assertTrue(outcomeCheck.contains("'COMPLETED'"), outcomeCheck);
            assertTrue(outcomeCheck.contains("'FAILED'"), outcomeCheck);
            assertTrue(outcomeCheck.contains("'LEASE_EXPIRED'"), outcomeCheck);
            assertEquals("CHECK (((outcome IS NULL) = (ended_at IS NULL)))",
                    definitions.get("task_attempts_ended_iff_outcome_check"));

            assertEquals(List.of(
                    new AttemptRow(legacyRunningId, 0, legacyWorker, legacyClaimedAt, null, null, null),
                    new AttemptRow(runningId, 2, runningWorker, runningClaimedAt, null, null, null)
            ), attempts(connection));

            Instant ended = Instant.parse("2020-05-01T00:00:00Z");
            assertInsertFails(connection, "23514",
                    pendingId, -1, UUID.randomUUID(), created, null, null);
            assertInsertFails(connection, "23514",
                    pendingId, 1, UUID.randomUUID(), created, ended, "CANCELLED");
            assertInsertFails(connection, "23514",
                    pendingId, 1, UUID.randomUUID(), created, null, "COMPLETED");
            assertInsertFails(connection, "23514",
                    pendingId, 1, UUID.randomUUID(), created, ended, null);
            assertInsertFails(connection, "23505",
                    legacyRunningId, 0, UUID.randomUUID(), created, null, null);
            assertInsertFails(connection, "23503",
                    UUID.randomUUID(), 1, UUID.randomUUID(), created, null, null);

            execute(connection, "DELETE FROM tasks WHERE id = '" + runningId + "'");
            assertEquals(List.of(
                    new AttemptRow(legacyRunningId, 0, legacyWorker, legacyClaimedAt, null, null, null)
            ), attempts(connection));
        }
    }

    /** PostgreSQL DDL is transactional, so the failed check rolls the whole of V6 back. */
    @Test
    void negativeAttemptFailsTheMigration() throws Exception {
        recreateDatabase(NEGATIVE_DATABASE);
        String url = jdbcUrl(NEGATIVE_DATABASE);
        migrate(url, "5");

        UUID pendingId = UUID.randomUUID();
        try (Connection connection = open(url)) {
            insert(connection, pendingId, "PENDING", Instant.parse("2020-01-01T00:00:00Z"),
                    null, null, null, 0, null);
            execute(connection, "UPDATE tasks SET attempt = -1 WHERE id = '" + pendingId + "'");
        }

        assertThrows(FlywayException.class, () -> migrate(url, "6"));

        try (Connection connection = open(url)) {
            assertFalse(tableExists(connection, "task_attempts"));
            assertEquals(V5_TASK_CHECKS, checkConstraintNames(connection, "tasks"));
        }
    }

    private static void migrate(String url, String target) {
        Flyway.configure()
                .dataSource(url, postgres.getUsername(), postgres.getPassword())
                .locations("classpath:db/migration")
                .target(target)
                .load()
                .migrate();
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

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static void insert(
            Connection connection,
            UUID id,
            String status,
            Instant createdAt,
            Instant claimedAt,
            UUID workerId,
            Instant finishedAt,
            int attempt,
            Instant leaseExpiresAt
    ) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO tasks (
                    id, task_type, status, payload, created_at, updated_at,
                    claimed_at, worker_id, finished_at, attempt, lease_expires_at
                )
                VALUES (?, 'NOOP', ?, CAST('{}' AS jsonb), ?, ?, ?, ?, ?, ?, ?)
                """)) {
            statement.setObject(1, id);
            statement.setString(2, status);
            statement.setObject(3, offset(createdAt));
            statement.setObject(4, offset(createdAt));
            setTimestamp(statement, 5, claimedAt);
            setUuid(statement, 6, workerId);
            setTimestamp(statement, 7, finishedAt);
            statement.setInt(8, attempt);
            setTimestamp(statement, 9, leaseExpiresAt);
            statement.executeUpdate();
        }
    }

    private static void assertInsertFails(
            Connection connection,
            String sqlState,
            UUID taskId,
            int attempt,
            UUID workerId,
            Instant startedAt,
            Instant endedAt,
            String outcome
    ) throws SQLException {
        List<AttemptRow> before = attempts(connection);
        SQLException rejected = assertThrows(SQLException.class, () -> {
            try (PreparedStatement statement = connection.prepareStatement("""
                    INSERT INTO task_attempts (task_id, attempt, worker_id, started_at, ended_at, outcome)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """)) {
                statement.setObject(1, taskId);
                statement.setInt(2, attempt);
                statement.setObject(3, workerId);
                statement.setObject(4, offset(startedAt));
                setTimestamp(statement, 5, endedAt);
                if (outcome == null) {
                    statement.setNull(6, Types.VARCHAR);
                } else {
                    statement.setString(6, outcome);
                }
                statement.executeUpdate();
            }
        });
        assertEquals(sqlState, rejected.getSQLState(), rejected.getMessage());
        assertEquals(before, attempts(connection));
    }

    private static Set<String> checkConstraintNames(Connection connection, String table) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = CAST(? AS regclass)
                  AND contype = 'c'
                """)) {
            statement.setString(1, table);
            try (ResultSet result = statement.executeQuery()) {
                Set<String> names = new HashSet<>();
                while (result.next()) {
                    names.add(result.getString("conname"));
                }
                return names;
            }
        }
    }

    private static Map<String, String> constraintDefinitions(Connection connection, String table)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT conname, pg_get_constraintdef(oid) AS definition
                FROM pg_constraint
                WHERE conrelid = CAST(? AS regclass)
                """)) {
            statement.setString(1, table);
            try (ResultSet result = statement.executeQuery()) {
                Map<String, String> definitions = new HashMap<>();
                while (result.next()) {
                    definitions.put(result.getString("conname"), result.getString("definition"));
                }
                return definitions;
            }
        }
    }

    private static boolean tableExists(Connection connection, String table) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT to_regclass(?) IS NOT NULL")) {
            statement.setString(1, table);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getBoolean(1);
            }
        }
    }

    private static List<AttemptRow> attempts(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("""
                     SELECT task_id, attempt, worker_id, started_at, ended_at, outcome, error
                     FROM task_attempts
                     ORDER BY attempt, task_id
                     """)) {
            List<AttemptRow> rows = new ArrayList<>();
            while (result.next()) {
                rows.add(new AttemptRow(
                        result.getObject("task_id", UUID.class),
                        result.getInt("attempt"),
                        result.getObject("worker_id", UUID.class),
                        instant(result, "started_at"),
                        instant(result, "ended_at"),
                        result.getString("outcome"),
                        result.getString("error")));
            }
            return rows;
        }
    }

    private static void setTimestamp(PreparedStatement statement, int index, Instant value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
        } else {
            statement.setObject(index, offset(value));
        }
    }

    private static void setUuid(PreparedStatement statement, int index, UUID value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.OTHER);
        } else {
            statement.setObject(index, value);
        }
    }

    private static OffsetDateTime offset(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Instant instant(ResultSet result, String column) throws SQLException {
        OffsetDateTime value = result.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private record AttemptRow(
            UUID taskId,
            int attempt,
            UUID workerId,
            Instant startedAt,
            Instant endedAt,
            String outcome,
            String error
    ) {
    }
}
