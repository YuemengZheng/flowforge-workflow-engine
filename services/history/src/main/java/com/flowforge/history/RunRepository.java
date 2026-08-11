package com.flowforge.history;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * The two tables this service owns, and the queries over them.
 *
 * <p>Writes are idempotent. Kafka gives at-least-once delivery, so the same event
 * can arrive twice — on a rebalance, or when this service is restarted and replays
 * from an earlier offset. Every insert is therefore keyed on
 * {@code (run_id, seq)}, which the engine guarantees is unique within a run, and a
 * duplicate is discarded rather than counted twice. A projection that double-counts
 * on redelivery is worse than one that is briefly behind.
 */
@Repository
public class RunRepository {

    private final JdbcTemplate jdbc;

    public RunRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @return true when the event was new, false when it was a duplicate. */
    public boolean recordEvent(RunEvent event) {
        int inserted = jdbc.update(
                """
                INSERT INTO run_events (run_id, seq, type, node, at_epoch)
                SELECT ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM run_events WHERE run_id = ? AND seq = ?
                )
                """,
                event.runId(), event.seq(), event.type(), event.node(), event.at(),
                event.runId(), event.seq());
        return inserted > 0;
    }

    /** Upsert the run summary. The first event opens it; a terminal one closes it. */
    public void openRun(String runId, double startedAt) {
        jdbc.update(
                """
                INSERT INTO runs (run_id, status, started_at)
                SELECT ?, 'running', ?
                WHERE NOT EXISTS (SELECT 1 FROM runs WHERE run_id = ?)
                """,
                runId, startedAt, runId);
    }

    public void closeRun(String runId, String status, Double totalMs, Integer nodesExecuted, double finishedAt) {
        jdbc.update(
                """
                UPDATE runs
                   SET status = ?, total_ms = ?, nodes_executed = ?, finished_at = ?
                 WHERE run_id = ?
                """,
                status, totalMs, nodesExecuted, finishedAt, runId);
    }

    public void countNode(String runId) {
        jdbc.update("UPDATE runs SET nodes_seen = nodes_seen + 1 WHERE run_id = ?", runId);
    }

    // ------------------------------------------------------------------ reads

    public List<Map<String, Object>> listRuns(String status, int limit) {
        if (status == null || status.isBlank()) {
            return jdbc.queryForList(
                    "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", limit);
        }
        return jdbc.queryForList(
                "SELECT * FROM runs WHERE status = ? ORDER BY started_at DESC LIMIT ?",
                status, limit);
    }

    public List<Map<String, Object>> findRun(String runId) {
        return jdbc.queryForList("SELECT * FROM runs WHERE run_id = ?", runId);
    }

    public List<Map<String, Object>> timeline(String runId) {
        return jdbc.queryForList(
                "SELECT seq, type, node, at_epoch FROM run_events WHERE run_id = ? ORDER BY seq",
                runId);
    }

    public Map<String, Object> summary() {
        return jdbc.queryForMap(
                """
                SELECT COUNT(*) AS runs,
                       COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0) AS completed,
                       COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed,
                       COALESCE(SUM(CASE WHEN status = 'paused' THEN 1 ELSE 0 END), 0) AS paused
                  FROM runs
                """);
    }

    public int eventCount() {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM run_events", Integer.class);
        return count == null ? 0 : count;
    }
}
