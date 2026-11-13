package com.kushal.workflow.worker;

import com.kushal.workflow.support.PostgresIntegrationTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * The recovery tests' context: a one-second lease with a 300 ms heartbeat,
 * so natural expiry takes about a second. The reaper bean stays off; tests
 * build their own reapers and executors.
 *
 * <p>Every subclass must get this same cached context. A subclass must not
 * declare a nested {@code @Configuration} or {@code @TestConfiguration},
 * {@code @Import}, {@code @MockitoBean}, or properties of its own. Any of
 * them makes a new context, with its own connection pool.
 */
@SpringBootTest
@TestPropertySource(properties = {
        "engine.lease.duration=1s",
        "engine.lease.heartbeat-interval=300ms"
})
abstract class RecoveryContextTest extends PostgresIntegrationTest {
}
