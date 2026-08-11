package com.flowforge.history;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Run history, projected from the engine's Kafka event stream.
 *
 * <p>This service exists because the Python API deliberately does not answer
 * historical questions. Its {@code GET /runs} lists runs that are <em>paused</em>
 * — that is the checkpoint store's job, and a checkpoint is deleted the moment a
 * run finishes. Nothing in the system could say what ran yesterday, how long a
 * node took, or which workflow fails most often.
 *
 * <p>The events needed to answer all of that are already on the topic. So this is
 * a read model: consume {@code flowforge.events}, fold it into two tables, and
 * serve queries. The engine stays uninvolved — it publishes and forgets, and a
 * projection that falls behind or is rebuilt from the topic costs nothing on the
 * hot path.
 */
@SpringBootApplication
public class HistoryApplication {

    public static void main(String[] args) {
        SpringApplication.run(HistoryApplication.class, args);
    }
}
