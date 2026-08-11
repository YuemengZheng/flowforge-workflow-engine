package com.flowforge.history;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The projection, against an in-memory database and no broker.
 *
 * <p>Kafka itself is not started: what needs testing here is the fold — that
 * events become rows, and that a redelivery does not double-count. Redelivery is
 * not a hypothetical, it is what at-least-once means.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:h2:mem:projection;DB_CLOSE_DELAY=-1;MODE=MySQL",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.kafka.bootstrap-servers=localhost:1",
        "spring.kafka.listener.auto-startup=false"
})
class ProjectionTest {

    @Autowired RunEventConsumer consumer;
    @Autowired RunRepository runs;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper json;

    @BeforeEach
    void clean() {
        jdbc.update("DELETE FROM run_events");
        jdbc.update("DELETE FROM runs");
    }

    private RunEvent event(String type, long seq, String runId) {
        return new RunEvent(type, seq, runId, 1_700_000_000.0 + seq, null, null, null);
    }

    @Test
    void a_run_is_opened_by_its_first_event() {
        consumer.apply(event("run.started", 1, "r1"));

        Map<String, Object> run = runs.findRun("r1").get(0);
        assertThat(run.get("STATUS")).isEqualTo("running");
        assertThat(runs.eventCount()).isEqualTo(1);
    }

    @Test
    void a_terminal_event_closes_the_run_with_its_stats() {
        consumer.apply(event("run.started", 1, "r2"));
        consumer.apply(new RunEvent("run.completed", 9, "r2", 1_700_000_009.0, null, "completed",
                Map.of("total_ms", 12.5, "nodes_executed", 7)));

        Map<String, Object> run = runs.findRun("r2").get(0);
        assertThat(run.get("STATUS")).isEqualTo("completed");
        assertThat(((Number) run.get("TOTAL_MS")).doubleValue()).isEqualTo(12.5);
        assertThat(((Number) run.get("NODES_EXECUTED")).intValue()).isEqualTo(7);
    }

    @Test
    void a_redelivered_event_is_not_counted_twice() {
        RunEvent completed = event("node.completed", 4, "r3");
        consumer.apply(event("run.started", 1, "r3"));
        consumer.apply(completed);
        consumer.apply(completed);   // at-least-once: this will happen
        consumer.apply(completed);

        assertThat(consumer.duplicates()).isGreaterThanOrEqualTo(2);
        assertThat(runs.eventCount()).isEqualTo(2);
        Map<String, Object> run = runs.findRun("r3").get(0);
        assertThat(((Number) run.get("NODES_SEEN")).intValue()).isEqualTo(1);
    }

    @Test
    void node_completions_are_counted_per_run() {
        consumer.apply(event("run.started", 1, "r4"));
        consumer.apply(event("node.completed", 2, "r4"));
        consumer.apply(event("node.completed", 3, "r4"));
        consumer.apply(event("node.skipped", 4, "r4"));

        Map<String, Object> run = runs.findRun("r4").get(0);
        assertThat(((Number) run.get("NODES_SEEN")).intValue()).isEqualTo(2);
    }

    @Test
    void the_timeline_comes_back_in_emit_order() {
        for (long seq : new long[]{3, 1, 2}) {
            consumer.apply(event("node.completed", seq, "r5"));
        }

        List<Map<String, Object>> timeline = runs.timeline("r5");
        assertThat(timeline).extracting(row -> ((Number) row.get("SEQ")).longValue())
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    void two_runs_do_not_mix() {
        consumer.apply(event("run.started", 1, "a"));
        consumer.apply(event("node.completed", 2, "a"));
        consumer.apply(event("run.started", 1, "b"));

        assertThat(runs.timeline("a")).hasSize(2);
        assertThat(runs.timeline("b")).hasSize(1);
        assertThat(runs.listRuns(null, 10)).hasSize(2);
    }

    @Test
    void an_event_the_producer_grew_a_field_for_still_parses() throws Exception {
        // Forward compatibility: the engine adds payload keys as node types grow.
        String payload = """
                {"type":"node.completed","seq":2,"run":"r6","at":1700000002.0,
                 "node":"a","ms":1.5,"attempts":1,"outputs":{"x":1},"brand_new":true}
                """;
        RunEvent parsed = json.readValue(payload, RunEvent.class);
        consumer.apply(parsed);

        assertThat(parsed.runId()).isEqualTo("r6");
        assertThat(parsed.node()).isEqualTo("a");
        assertThat(runs.eventCount()).isEqualTo(1);
    }

    @Test
    void an_unparseable_record_is_skipped_not_fatal() {
        consumer.onMessage("{not json");
        consumer.onMessage("{\"type\":\"run.started\"}");   // no run id

        assertThat(consumer.rejected()).isEqualTo(2);
        assertThat(runs.eventCount()).isZero();
    }

    @Test
    void the_summary_counts_by_status() {
        consumer.apply(new RunEvent("run.completed", 1, "done", 1.0, null, "completed", Map.of()));
        consumer.apply(new RunEvent("run.failed", 1, "bad", 2.0, null, "failed", Map.of()));

        Map<String, Object> summary = runs.summary();
        assertThat(((Number) summary.get("RUNS")).intValue()).isEqualTo(2);
        assertThat(((Number) summary.get("COMPLETED")).intValue()).isEqualTo(1);
        assertThat(((Number) summary.get("FAILED")).intValue()).isEqualTo(1);
    }
}
