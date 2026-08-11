package com.flowforge.history;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * One event as the Python engine publishes it.
 *
 * <p>Unknown fields are ignored on purpose: the producer adds keys to event
 * payloads as node types grow, and a projection that refuses to parse an event
 * because it learned a new field would turn a producer-side addition into a
 * consumer-side outage.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RunEvent(
        String type,
        long seq,
        @JsonProperty("run") String runId,
        double at,
        String node,
        String status,
        Map<String, Object> stats) {

    /** Terminal events carry the run's final status and its stats. */
    public boolean isTerminal() {
        return "run.completed".equals(type) || "run.failed".equals(type) || "run.paused".equals(type);
    }

    public String terminalStatus() {
        if (status != null) {
            return status;
        }
        return switch (type == null ? "" : type) {
            case "run.completed" -> "completed";
            case "run.failed" -> "failed";
            case "run.paused" -> "paused";
            default -> null;
        };
    }

    public Integer nodesExecuted() {
        return statInt("nodes_executed");
    }

    public Double totalMs() {
        if (stats == null || !(stats.get("total_ms") instanceof Number number)) {
            return null;
        }
        return number.doubleValue();
    }

    private Integer statInt(String key) {
        if (stats == null || !(stats.get(key) instanceof Number number)) {
            return null;
        }
        return number.intValue();
    }
}
