package com.flowforge.history;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Folds the event topic into the run tables.
 *
 * <p>Deserialisation is done here rather than by a typed Kafka deserialiser so a
 * single unparseable record cannot wedge the partition: a poison message is
 * counted and skipped, because the alternative is a consumer that stops making
 * progress for every later event because of one bad one.
 */
@Component
public class RunEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(RunEventConsumer.class);

    private final RunRepository runs;
    private final ObjectMapper json;
    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();

    public RunEventConsumer(RunRepository runs, ObjectMapper json) {
        this.runs = runs;
        this.json = json;
    }

    @KafkaListener(topics = "${flowforge.topic:flowforge.events}", groupId = "${flowforge.group:flowforge-history}")
    @Transactional
    public void onMessage(String payload) {
        RunEvent event;
        try {
            event = json.readValue(payload, RunEvent.class);
        } catch (Exception exc) {
            rejected.incrementAndGet();
            log.warn("skipping unparseable event: {}", exc.getMessage());
            return;
        }
        if (event.runId() == null || event.runId().isBlank() || event.type() == null) {
            rejected.incrementAndGet();
            return;
        }
        apply(event);
    }

    void apply(RunEvent event) {
        // Idempotence first: if this event has been seen, nothing else may run,
        // or a redelivery would inflate the counters it already contributed to.
        if (!runs.recordEvent(event)) {
            duplicates.incrementAndGet();
            return;
        }
        runs.openRun(event.runId(), event.at());
        if ("node.completed".equals(event.type())) {
            runs.countNode(event.runId());
        }
        if (event.isTerminal()) {
            runs.closeRun(
                    event.runId(),
                    event.terminalStatus(),
                    event.totalMs(),
                    event.nodesExecuted(),
                    event.at());
        }
        applied.incrementAndGet();
    }

    public long applied() {
        return applied.get();
    }

    public long duplicates() {
        return duplicates.get();
    }

    public long rejected() {
        return rejected.get();
    }
}
