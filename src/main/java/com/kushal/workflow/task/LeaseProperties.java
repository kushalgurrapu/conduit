package com.kushal.workflow.task;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * {@code engine.lease.*}. {@link #leaseMicros()} is the duration claim
 * writes into {@code lease_expires_at}. Lease extension uses the same
 * conversion.
 *
 * <p>Both values are at least one microsecond, and
 * {@code heartbeat-interval} is strictly shorter than {@code duration}.
 * One third of the lease is a starting heuristic, not a startup rule.
 */
@ConfigurationProperties(prefix = "engine.lease")
public class LeaseProperties {

    private Duration duration = Duration.ofSeconds(30);

    private Duration heartbeatInterval = Duration.ofSeconds(10);

    public void validate() {
        requireAtLeastOneMicrosecond(duration, "duration");
        requireAtLeastOneMicrosecond(heartbeatInterval, "heartbeat-interval");
        if (heartbeatInterval.compareTo(duration) >= 0) {
            throw new IllegalStateException(
                    "engine.lease.heartbeat-interval (" + heartbeatInterval
                            + ") must be shorter than duration (" + duration + ")");
        }
    }

    /**
     * Whole microseconds, truncating any remainder shorter than one.
     * {@link #validate()} rejects a duration that would become zero.
     */
    public long leaseMicros() {
        return duration.toNanos() / 1_000L;
    }

    private static void requireAtLeastOneMicrosecond(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative() || value.toNanos() < 1_000L) {
            throw new IllegalStateException(
                    "engine.lease." + name + " must be at least 1 microsecond");
        }
    }

    public Duration getDuration() {
        return duration;
    }

    public void setDuration(Duration duration) {
        this.duration = duration;
    }

    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }
}
