-- S4 Development agent: the spine records the migrations (M1-M5) it applied.
-- The loyalty feature tables exist only on dev branches.

CREATE TABLE migration_log (
    migration_id INT NOT NULL,
    name         VARCHAR(40) NOT NULL,
    applied_step INT NOT NULL,
    applied_at   TIMESTAMP,
    PRIMARY KEY (migration_id)
);
