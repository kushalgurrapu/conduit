package com.kushal.workflow.worker;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * {@code engine.worker.*}. Tests turn the pool off. The main application
 * leaves it on.
 *
 * <p>{@code shutdown-timeout} is how long {@link WorkerPool#stop()} waits
 * before interrupting a handler. It is not a limit on how long a handler
 * may run while the process is up. It has to be shorter than
 * {@code spring.lifecycle.timeout-per-shutdown-phase} so Spring does not
 * abandon the shutdown phase first.
 */
@Validated
@ConfigurationProperties(prefix = "engine.worker")
public class WorkerProperties {

    private boolean enabled = true;

    @Min(1)
    private int concurrency = 4;

    @NotNull
    private Duration pollInterval = Duration.ofSeconds(1);

    @NotNull
    private Duration errorBackoff = Duration.ofSeconds(1);

    @NotNull
    private Duration shutdownTimeout = Duration.ofSeconds(20);

    public void validate(Duration phaseTimeout) {
        if (concurrency < 1) {
            throw new IllegalStateException("engine.worker.concurrency must be at least 1");
        }
        requirePositive(pollInterval, "poll-interval");
        requirePositive(errorBackoff, "error-backoff");
        requirePositive(shutdownTimeout, "shutdown-timeout");
        if (phaseTimeout == null || phaseTimeout.isZero() || phaseTimeout.isNegative()) {
            throw new IllegalStateException(
                    "spring.lifecycle.timeout-per-shutdown-phase must be positive");
        }
        if (shutdownTimeout.compareTo(phaseTimeout) >= 0) {
            throw new IllegalStateException(
                    "engine.worker.shutdown-timeout (" + shutdownTimeout
                            + ") must be less than spring.lifecycle.timeout-per-shutdown-phase ("
                            + phaseTimeout + ")");
        }
    }

    private static void requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalStateException("engine.worker." + name + " must be positive");
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public Duration getErrorBackoff() {
        return errorBackoff;
    }

    public void setErrorBackoff(Duration errorBackoff) {
        this.errorBackoff = errorBackoff;
    }

    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    public void setShutdownTimeout(Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }
}
