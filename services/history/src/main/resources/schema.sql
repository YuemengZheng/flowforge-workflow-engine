-- Portable on purpose: MySQL in the container, an in-memory database in the
-- repository tests, and the same DDL for both.
CREATE TABLE IF NOT EXISTS runs (
    run_id         VARCHAR(191) NOT NULL PRIMARY KEY,
    status         VARCHAR(32)  NOT NULL,
    started_at     DOUBLE       NOT NULL,
    finished_at    DOUBLE       NULL,
    total_ms       DOUBLE       NULL,
    nodes_executed INT          NULL,
    nodes_seen     INT          NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS run_events (
    run_id   VARCHAR(191) NOT NULL,
    seq      BIGINT       NOT NULL,
    type     VARCHAR(64)  NOT NULL,
    node     VARCHAR(191) NULL,
    at_epoch DOUBLE       NOT NULL,
    PRIMARY KEY (run_id, seq)
);
