package com.kushal.workflow.worker;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * {@code engine.reaper.*}. Independent of {@code engine.worker.enabled}, so
 * an API-only process still reaps. Tests turn the reaper off.
 *
 * <p>{@link #validate()} runs whether or not the reaper is enabled, so a bad
 * value fails startup in every process. {@code interval} is at least 100 ms,
 * and {@code batch-size} is 1 to 1000 inclusive.
 */
@ConfigurationProperties(prefix = "engine.reaper")
public class ReaperProperties {

    static final Duration MIN_INTERVAL = Duration.ofMillis(100);
    static final int MAX_BATCH_SIZE = 1000;

    private boolean enabled = true;

    private Duration interval = Duration.ofSeconds(10);

    private int batchSize = 100;

    public void validate() {
        if (interval == null || interval.compareTo(MIN_INTERVAL) < 0) {
            throw new IllegalStateException(
                    "engine.reaper.interval (" + interval + ") must be at least " + MIN_INTERVAL);
        }
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalStateException(
                    "engine.reaper.batch-size (" + batchSize + ") must be between 1 and " + MAX_BATCH_SIZE);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Duration getInterval() {
        return interval;
    }

    public void setInterval(Duration interval) {
        this.interval = interval;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
