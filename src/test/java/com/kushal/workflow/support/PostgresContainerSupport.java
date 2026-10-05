package com.kushal.workflow.support;

import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * One PostgreSQL container for the whole test JVM.
 *
 * <p>The server is started with {@code fsync=off}, a short {@code lock_timeout},
 * and {@code idle_in_transaction_session_timeout}. Those settings make tests
 * faster and stop a forgotten lock from hanging the suite. They also mean this
 * container must not be used to claim that a crash is durable: a write can be
 * acknowledged before it reaches disk.
 *
 * <p>Spring tests extend {@link PostgresIntegrationTest}. A test that must not
 * start the application, such as a Flyway upgrade of a second database, extends
 * this class directly.
 */
public abstract class PostgresContainerSupport {

    @ServiceConnection
    protected static final PostgreSQLContainer postgres =
            new PostgreSQLContainer("postgres:16-alpine")
                    .withCommand(
                            "postgres",
                            "-c", "fsync=off",
                            "-c", "lock_timeout=10s",
                            "-c", "idle_in_transaction_session_timeout=30s");

    static {
        postgres.start();
    }
}
