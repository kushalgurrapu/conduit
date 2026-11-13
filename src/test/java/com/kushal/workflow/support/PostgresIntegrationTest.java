package com.kushal.workflow.support;

import com.kushal.workflow.worker.LeaseReaper;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Spring tests share {@link PostgresContainerSupport}'s container.
 *
 * <p>{@code engine.worker.enabled=false} keeps the worker pool from
 * claiming rows while a test is looking at them. {@code DELETE FROM tasks}
 * drops rows from earlier tests without taking the stronger locks of
 * {@code TRUNCATE}.
 *
 * <p>{@code engine.reaper.enabled=false} keeps the lease reaper from
 * requeuing fixtures with expired leases. Every cached context shares one
 * database, so a reaper running in any of them would reach every other
 * test's rows. Before each test, the reaper must not be running unless
 * the context enables it. A context that does must be closed after its
 * class.
 */
@TestPropertySource(properties = {
        "engine.worker.enabled=false",
        "engine.reaper.enabled=false",
        "spring.datasource.hikari.minimum-idle=1"
})
public abstract class PostgresIntegrationTest extends PostgresContainerSupport {

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private ObjectProvider<LeaseReaper> leaseReaper;

    @Autowired
    private Environment environment;

    @BeforeEach
    void reaperIsOffUnlessEnabled() {
        if (environment.getProperty("engine.reaper.enabled", Boolean.class, true)) {
            return;
        }
        LeaseReaper reaper = leaseReaper.getIfAvailable();
        if (reaper != null) {
            assertFalse(reaper.isRunning(), "the lease reaper is running in a context that disables it");
        }
    }

    @BeforeEach
    void deleteAllTasks() {
        jdbcClient.sql("DELETE FROM tasks").update();
    }
}
