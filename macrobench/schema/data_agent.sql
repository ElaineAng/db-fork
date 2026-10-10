-- S6 Data agent: derived (warehouse) tables maintained by ingestion batches,
-- and the batch manifest whose row is the batch sentinel.

CREATE TABLE daily_revenue (
    day      DATE NOT NULL,
    w_id     INT NOT NULL,
    revenue  DECIMAL(14, 2) NOT NULL,
    orders   INT NOT NULL,
    PRIMARY KEY (day, w_id)
);

CREATE TABLE customer_summary (
    c_w_id       INT NOT NULL,
    c_d_id       INT NOT NULL,
    c_id         INT NOT NULL,
    order_count  INT NOT NULL,
    total_amount DECIMAL(14, 2) NOT NULL,
    last_order   TIMESTAMP,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

CREATE TABLE nation_sales (
    n_nationkey INT NOT NULL,
    day         DATE NOT NULL,
    revenue     DECIMAL(14, 2) NOT NULL,
    PRIMARY KEY (n_nationkey, day)
);

CREATE TABLE batch_manifest (
    batch_id    INT NOT NULL,
    branch_name VARCHAR(64) NOT NULL,
    days_back   INT NOT NULL,
    rows_loaded INT NOT NULL,
    status      VARCHAR(16) NOT NULL,
    created_at  TIMESTAMP,
    PRIMARY KEY (batch_id)
);
