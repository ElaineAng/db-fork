-- ============================================================
-- CH-benCHmark Schema for PostgreSQL
-- Combines TPC-C (OLTP) tables with TPC-H dimension tables
-- (region, nation, supplier) for HTAP workloads.
--
-- Reference: Cole et al., "The Mixed Workload CH-benCHmark"
--            DBTest 2011.
-- ============================================================

-- Drop existing tables (reverse dependency order)
DROP TABLE IF EXISTS order_line;
DROP TABLE IF EXISTS new_order;
DROP TABLE IF EXISTS history;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS stock;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS district ;
DROP TABLE IF EXISTS warehouse ;
DROP TABLE IF EXISTS item ;
DROP TABLE IF EXISTS supplier ;
DROP TABLE IF EXISTS nation ;
DROP TABLE IF EXISTS region ;

-- ============================================================
-- TPC-H Dimension Tables (analytical layer)
-- ============================================================

-- REGION table
CREATE TABLE region (
    r_regionkey  INTEGER NOT NULL,
    r_name       CHAR(55) NOT NULL,
    r_comment    VARCHAR(152),
    PRIMARY KEY (r_regionkey)
);

-- NATION table
CREATE TABLE nation (
    n_nationkey  INTEGER NOT NULL,
    n_name       CHAR(25) NOT NULL,
    n_regionkey  INTEGER NOT NULL,
    n_comment    VARCHAR(152),
    PRIMARY KEY (n_nationkey)
);

-- SUPPLIER table
CREATE TABLE supplier (
    su_suppkey    INTEGER NOT NULL,
    su_name       CHAR(25) NOT NULL,
    su_address    VARCHAR(40) NOT NULL,
    su_nationkey  INTEGER NOT NULL,
    su_phone      CHAR(15) NOT NULL,
    su_acctbal    DECIMAL(12, 2) NOT NULL,
    su_comment    VARCHAR(101) NOT NULL,
    PRIMARY KEY (su_suppkey)
);

-- ============================================================
-- TPC-C Core Tables (transactional layer)
-- ============================================================

-- WAREHOUSE table
CREATE TABLE warehouse (
    w_id        SMALLINT NOT NULL,
    w_name      VARCHAR(10),
    w_street_1  VARCHAR(20),
    w_street_2  VARCHAR(20),
    w_city      VARCHAR(20),
    w_state     CHAR(2),
    w_zip       CHAR(9),
    w_tax       DECIMAL(4, 4),
    w_ytd       DECIMAL(12, 2),
    PRIMARY KEY (w_id)
);

-- DISTRICT table
CREATE TABLE district (
    d_id         SMALLINT NOT NULL,
    d_w_id       SMALLINT NOT NULL,
    d_name       VARCHAR(10),
    d_street_1   VARCHAR(20),
    d_street_2   VARCHAR(20),
    d_city       VARCHAR(20),
    d_state      CHAR(2),
    d_zip        CHAR(9),
    d_tax        DECIMAL(4, 4),
    d_ytd        DECIMAL(12, 2),
    d_next_o_id  INT,
    PRIMARY KEY (d_w_id, d_id)
);

-- CUSTOMER table
CREATE TABLE customer (
    c_id           INT NOT NULL,
    c_d_id         SMALLINT NOT NULL,
    c_w_id         SMALLINT NOT NULL,
    c_first        VARCHAR(16),
    c_middle       CHAR(2),
    c_last         VARCHAR(16),
    c_street_1     VARCHAR(20),
    c_street_2     VARCHAR(20),
    c_city         VARCHAR(20),
    c_state        CHAR(2),
    c_zip          CHAR(9),
    c_phone        CHAR(16),
    c_since        TIMESTAMP,
    c_credit       CHAR(2),
    c_credit_lim   DECIMAL(12, 2),
    c_discount     DECIMAL(4, 4),
    c_balance      DECIMAL(12, 2),
    c_ytd_payment  FLOAT,
    c_payment_cnt  SMALLINT,
    c_delivery_cnt SMALLINT,
    c_data         VARCHAR(500),
    c_n_nationkey  INTEGER,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

-- HISTORY table
CREATE TABLE history (
    h_c_id   INT,
    h_c_d_id SMALLINT,
    h_c_w_id SMALLINT,
    h_d_id   SMALLINT,
    h_w_id   SMALLINT,
    h_date   TIMESTAMP,
    h_amount DECIMAL(6, 2),
    h_data   VARCHAR(24)
);

-- ITEM table
CREATE TABLE item (
    i_id     INT NOT NULL,
    i_im_id  INT,
    i_name   VARCHAR(24),
    i_price  DECIMAL(5, 2),
    i_data   VARCHAR(50),
    PRIMARY KEY (i_id)
);

-- STOCK table
CREATE TABLE stock (
    s_i_id       INT NOT NULL,
    s_w_id       SMALLINT NOT NULL,
    s_quantity   SMALLINT,
    s_dist_01    CHAR(24),
    s_dist_02    CHAR(24),
    s_dist_03    CHAR(24),
    s_dist_04    CHAR(24),
    s_dist_05    CHAR(24),
    s_dist_06    CHAR(24),
    s_dist_07    CHAR(24),
    s_dist_08    CHAR(24),
    s_dist_09    CHAR(24),
    s_dist_10    CHAR(24),
    s_ytd        DECIMAL(8, 0),
    s_order_cnt  SMALLINT,
    s_remote_cnt SMALLINT,
    s_data       VARCHAR(50),
    s_su_suppkey INTEGER,
    PRIMARY KEY (s_w_id, s_i_id)
);

-- ORDERS table
CREATE TABLE orders (
    o_id         INT NOT NULL,
    o_d_id       SMALLINT NOT NULL,
    o_w_id       SMALLINT NOT NULL,
    o_c_id       INT,
    o_entry_d    TIMESTAMP,
    o_carrier_id SMALLINT,
    o_ol_cnt     SMALLINT,
    o_all_local  SMALLINT,
    PRIMARY KEY (o_w_id, o_d_id, o_id)
);

-- NEW_ORDER table
CREATE TABLE new_order (
    no_o_id  INT NOT NULL,
    no_d_id  SMALLINT NOT NULL,
    no_w_id  SMALLINT NOT NULL,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
);

-- ORDER_LINE table
CREATE TABLE order_line (
    ol_o_id        INT NOT NULL,
    ol_d_id        SMALLINT NOT NULL,
    ol_w_id        SMALLINT NOT NULL,
    ol_number      SMALLINT NOT NULL,
    ol_i_id        INT,
    ol_supply_w_id SMALLINT,
    ol_delivery_d  TIMESTAMP,
    ol_quantity    SMALLINT,
    ol_amount      DECIMAL(6, 2),
    ol_dist_info   CHAR(24),
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

-- ============================================================
-- Foreign Key Constraints
-- ============================================================

-- TPC-H dimension FKs
ALTER TABLE nation ADD FOREIGN KEY (n_regionkey)
    REFERENCES region (r_regionkey) ON DELETE CASCADE;
ALTER TABLE supplier ADD FOREIGN KEY (su_nationkey)
    REFERENCES nation (n_nationkey) ON DELETE CASCADE;

-- TPC-C FKs
ALTER TABLE district ADD FOREIGN KEY (d_w_id)
    REFERENCES warehouse (w_id) ON DELETE CASCADE;
ALTER TABLE customer ADD FOREIGN KEY (c_w_id, c_d_id)
    REFERENCES district (d_w_id, d_id) ON DELETE CASCADE;
ALTER TABLE history ADD FOREIGN KEY (h_c_w_id, h_c_d_id, h_c_id)
    REFERENCES customer (c_w_id, c_d_id, c_id) ON DELETE CASCADE;
ALTER TABLE history ADD FOREIGN KEY (h_w_id, h_d_id)
    REFERENCES district (d_w_id, d_id) ON DELETE CASCADE;
ALTER TABLE stock ADD FOREIGN KEY (s_w_id)
    REFERENCES warehouse (w_id) ON DELETE CASCADE;
ALTER TABLE stock ADD FOREIGN KEY (s_i_id)
    REFERENCES item (i_id) ON DELETE CASCADE;
ALTER TABLE orders ADD FOREIGN KEY (o_w_id, o_d_id, o_c_id)
    REFERENCES customer (c_w_id, c_d_id, c_id) ON DELETE CASCADE;
ALTER TABLE new_order ADD FOREIGN KEY (no_w_id, no_d_id, no_o_id)
    REFERENCES orders (o_w_id, o_d_id, o_id) ON DELETE CASCADE;
ALTER TABLE order_line ADD FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id)
    REFERENCES orders (o_w_id, o_d_id, o_id) ON DELETE CASCADE;
ALTER TABLE order_line ADD FOREIGN KEY (ol_supply_w_id, ol_i_id)
    REFERENCES stock (s_w_id, s_i_id) ON DELETE CASCADE;

-- CH-benCHmark cross-schema FK (denormalized in stock and customer)
ALTER TABLE customer ADD FOREIGN KEY (c_n_nationkey)
    REFERENCES nation (n_nationkey) ON DELETE CASCADE;
