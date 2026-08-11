package com.flowforge.history;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** The questions the Python API cannot answer, because it keeps no history. */
@RestController
public class RunController {

    private final RunRepository runs;
    private final RunEventConsumer consumer;

    public RunController(RunRepository runs, RunEventConsumer consumer) {
        this.runs = runs;
        this.consumer = consumer;
    }

    /** Liveness, and enough state for the container healthcheck to be meaningful. */
    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of(
                "status", "ok",
                "events_applied", consumer.applied(),
                "duplicates_skipped", consumer.duplicates(),
                "events_rejected", consumer.rejected());
    }

    @GetMapping("/history/runs")
    public Map<String, Object> list(
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "50") int limit) {
        // Math.clamp would be neater but is Java 21; this targets 17.
        int capped = Math.min(Math.max(limit, 1), 500);
        List<Map<String, Object>> found = runs.listRuns(status, capped);
        return Map.of("runs", found, "count", found.size());
    }

    @GetMapping("/history/runs/{runId}")
    public ResponseEntity<Map<String, Object>> one(@PathVariable String runId) {
        List<Map<String, Object>> found = runs.findRun(runId);
        if (found.isEmpty()) {
            return ResponseEntity.status(404).body(Map.of("error", "unknown run " + runId));
        }
        return ResponseEntity.ok(found.get(0));
    }

    /** Every event for a run, in emit order — the timeline the engine streamed. */
    @GetMapping("/history/runs/{runId}/timeline")
    public ResponseEntity<Map<String, Object>> timeline(@PathVariable String runId) {
        List<Map<String, Object>> events = runs.timeline(runId);
        if (events.isEmpty()) {
            return ResponseEntity.status(404).body(Map.of("error", "no events for run " + runId));
        }
        return ResponseEntity.ok(Map.of("run", runId, "events", events, "count", events.size()));
    }

    @GetMapping("/history/summary")
    public Map<String, Object> summary() {
        return runs.summary();
    }
}
