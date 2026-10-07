package com.kushal.workflow.support;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

/**
 * Spring tests share {@link PostgresContainerSupport}'s container.
 *
 * <p>{@code engine.worker.enabled=false} keeps the worker pool from
 * claiming rows while a test is looking at them. {@code DELETE FROM tasks}
 * drops rows from earlier tests without taking the stronger locks of
 * {@code TRUNCATE}.
 */
@TestPropertySource(properties = {
        "engine.worker.enabled=false",
        "spring.datasource.hikari.minimum-idle=1"
})
public abstract class PostgresIntegrationTest extends PostgresContainerSupport {

    @Autowired
    private JdbcClient jdbcClient;

    @BeforeEach
    void deleteAllTasks() {
        jdbcClient.sql("DELETE FROM tasks").update();
    }
}
