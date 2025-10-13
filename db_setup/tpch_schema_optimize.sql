-- Indexes on the largest table: LINEITEM
CREATE INDEX idx_lineitem_orderkey ON lineitem (l_orderkey);
CREATE INDEX idx_lineitem_part_supp ON lineitem (l_partkey, l_suppkey);

-- Indexes on ORDERS
CREATE INDEX idx_orders_custkey ON orders (o_custkey);
CREATE INDEX idx_orders_orderdate ON orders (o_orderdate);

-- Indexes on PARTSUPP
CREATE INDEX idx_partsupp_partkey ON partsupp (ps_partkey);
CREATE INDEX idx_partsupp_suppkey ON partsupp (ps_suppkey);

-- Indexes for joins involving CUSTOMER, SUPPLIER, NATION, and REGION
CREATE INDEX idx_customer_nationkey ON customer (c_nationkey);
CREATE INDEX idx_supplier_nationkey ON supplier (s_nationkey);
CREATE INDEX idx_nation_regionkey ON nation (n_regionkey);