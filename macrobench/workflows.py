"""Per-workflow SQL operations for the macrobenchmark.

Each workflow class provides concrete SQL statements for the three phases
of an automaton step: mutate (DDL + DML), evaluate (read queries), and
compare (cross-branch queries run during background passes).

SQL statements are derived from Figure 2 in the paper (Section 3.3).
The CH-benCHmark schema tables used:
  warehouse, district, customer, item, stock, orders, new_order, order_line

All SQL is written for maximum compatibility across backends (Dolt, Neon,
KPG, Xata).  Specifically we avoid:
  - FILTER (WHERE ...) — use CASE WHEN instead
  - percentile_cont / WITHIN GROUP — use simple aggregates
  - CREATE TABLE ... AS SELECT — use CREATE TABLE + INSERT
  - NOT VALID on constraints
  - IF NOT EXISTS on ALTER TABLE ADD COLUMN
"""

from abc import ABC, abstractmethod
from typing import Optional


class WorkflowOps(ABC):
    """Abstract base class for workflow-specific SQL operations."""

    @abstractmethod
    def mutate_ddl(self, step_id: int) -> list[str]:
        """Return DDL statements for this step.

        Args:
            step_id: Global step number (can be used to vary statements).

        Returns:
            List of SQL DDL strings.
        """

    @abstractmethod
    def mutate_dml(self, step_id: int, rng) -> list[str]:
        """Return DML statements for this step.

        Args:
            step_id: Global step number.
            rng: random.Random instance for generating random values.

        Returns:
            List of SQL DML strings.
        """

    @abstractmethod
    def evaluate(self) -> list[str]:
        """Return evaluation queries for this step.

        Returns:
            List of SQL SELECT strings.
        """

    @abstractmethod
    def compare(self) -> list[str]:
        """Return per-branch comparison queries for background passes.

        These queries are executed on each alive branch individually
        (the runner connects to each branch in turn).

        Returns:
            List of SQL SELECT strings.
        """


class SoftwareDevOps(WorkflowOps):
    """Software development workflow: DDL-heavy schema migrations.

    Mutate: CREATE TABLE, ALTER TABLE, seed data via INSERT...SELECT.
    Evaluate: FK integrity checks, application query simulation.
    Compare: Run the same test queries on sibling branches.
    """

    def mutate_ddl(self, step_id: int) -> list[str]:
        # Use step_id to create unique table/column names per step to avoid
        # conflicts across steps on the same branch.
        suffix = step_id % 1000
        return [
            f"""CREATE TABLE IF NOT EXISTS customer_preferences_{suffix} (
                id SERIAL PRIMARY KEY,
                c_w_id INT NOT NULL,
                c_d_id INT NOT NULL,
                c_id INT NOT NULL,
                theme VARCHAR(20) DEFAULT 'light',
                language VARCHAR(10) DEFAULT 'en',
                notifications BOOL DEFAULT true
            );""",
            f"ALTER TABLE customer ADD COLUMN "
            f"c_display_name_{suffix} VARCHAR(100);",
        ]

    def mutate_dml(self, step_id: int, rng) -> list[str]:
        suffix = step_id % 1000
        w_id = rng.randint(1, 10)
        d_id = rng.randint(1, 10)
        return [
            f"""INSERT INTO customer_preferences_{suffix}
                (c_w_id, c_d_id, c_id, theme, language, notifications)
                SELECT c_w_id, c_d_id, c_id, 'light', 'en', true
                FROM customer
                WHERE c_w_id = {w_id} AND c_d_id = {d_id}
                ON CONFLICT DO NOTHING;""",
        ]

    def evaluate(self) -> list[str]:
        return [
            # Schema sanity: count tables created by DDL migrations
            """SELECT COUNT(*) FROM information_schema.tables
               WHERE table_schema = 'public';""",
            # Column count check: how many columns on customer table
            """SELECT COUNT(*) FROM information_schema.columns
               WHERE table_name = 'customer';""",
            # Application query simulation: customer lookup
            """SELECT c.c_id, c.c_first, c.c_last
               FROM customer c
               WHERE c.c_w_id = 1 AND c.c_d_id = 1
               LIMIT 10;""",
            # FK-like integrity check: orphan orders
            """SELECT COUNT(*) FROM orders o
               WHERE NOT EXISTS (
                   SELECT 1 FROM customer c
                   WHERE c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id
                     AND c.c_id = o.o_c_id
               );""",
        ]

    def compare(self) -> list[str]:
        return [
            # Same integrity check used as "test suite" on each branch
            """SELECT COUNT(*) FROM information_schema.tables
               WHERE table_schema = 'public';""",
            """SELECT COUNT(*) FROM customer;""",
        ]


class FailureReproOps(WorkflowOps):
    """Failure reproduction: transaction replay + constraint modification.

    Mutate: INSERT/UPDATE with edge-case values, ALTER constraints.
    Evaluate: Corruption checks (negative amounts, orphan rows).
    Compare: Audit trail capture (count mutations per branch).
    """

    def mutate_ddl(self, step_id: int) -> list[str]:
        suffix = step_id % 1000
        # Toggle constraint: drop then re-add to test hypothesis
        if step_id % 2 == 0:
            return [
                f"ALTER TABLE order_line DROP CONSTRAINT IF EXISTS "
                f"ol_amount_check_{suffix};",
            ]
        else:
            return [
                f"ALTER TABLE order_line ADD CONSTRAINT "
                f"ol_amount_check_{suffix} CHECK (ol_amount >= 0);",
            ]

    def mutate_dml(self, step_id: int, rng) -> list[str]:
        stmts = []
        # Replay suspect transaction sequence with edge-case values
        w_id = rng.randint(1, 10)
        d_id = rng.randint(1, 10)
        o_id = 3000 + step_id

        stmts.append(
            f"""INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,
                o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
                VALUES ({o_id}, {d_id}, {w_id}, 1, NULL, 5, 1,
                        EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT)
                ON CONFLICT DO NOTHING;"""
        )

        # Inject edge-case: negative quantity in order_line
        for i in range(min(5, step_id % 10 + 1)):
            ol_num = i + 1
            amount = rng.choice([-1, -5, 0, 1, 100])
            stmts.append(
                f"""INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id,
                    ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d,
                    ol_quantity, ol_amount, ol_dist_info)
                    VALUES ({o_id}, {d_id}, {w_id}, {ol_num}, 1, {w_id},
                            NULL, 5, {amount}, 'dist_info')
                    ON CONFLICT DO NOTHING;"""
            )

        # Update existing rows with suspect values
        stmts.append(
            f"""UPDATE order_line SET ol_amount = -1
                WHERE ol_o_id = {o_id} AND ol_d_id = {d_id}
                AND ol_w_id = {w_id} AND ol_number = 1;"""
        )

        return stmts

    def evaluate(self) -> list[str]:
        return [
            # Check for negative amounts (corruption indicator)
            "SELECT COUNT(*) FROM order_line WHERE ol_amount < 0;",
            # Orphan order lines
            """SELECT COUNT(*) FROM order_line ol
               WHERE NOT EXISTS (
                   SELECT 1 FROM orders o
                   WHERE o.o_id = ol.ol_o_id
                     AND o.o_d_id = ol.ol_d_id
                     AND o.o_w_id = ol.ol_w_id
               );""",
            # Constraint violations
            """SELECT COUNT(*) FROM order_line
               WHERE ol_quantity <= 0;""",
        ]

    def compare(self) -> list[str]:
        return [
            # Audit trail: count of mutations (negative amounts injected)
            "SELECT COUNT(*) FROM order_line WHERE ol_amount < 0;",
            "SELECT COUNT(*) FROM orders;",
        ]


class DataCurationOps(WorkflowOps):
    """Data curation: bulk DML / CTAS for feature synthesis.

    Mutate: CREATE TABLE + INSERT...SELECT with aggregation, bulk rewrite.
    Evaluate: Null rate, distribution stats, full-scan validation.
    Compare: Metric aggregation across branches.
    """

    def mutate_ddl(self, step_id: int) -> list[str]:
        suffix = step_id % 1000
        # Split CTAS into CREATE TABLE + INSERT...SELECT for Dolt compat.
        return [
            f"""CREATE TABLE IF NOT EXISTS customer_features_{suffix} (
                c_w_id SMALLINT,
                c_d_id SMALLINT,
                c_id INT,
                total_spend DECIMAL(12, 2),
                order_cnt BIGINT,
                last_order BIGINT
            );""",
            f"""INSERT INTO customer_features_{suffix}
                (c_w_id, c_d_id, c_id, total_spend, order_cnt, last_order)
                SELECT c.c_w_id, c.c_d_id, c.c_id,
                    COALESCE(SUM(ol.ol_amount), 0),
                    COUNT(DISTINCT o.o_id),
                    MAX(o.o_entry_d)
                FROM customer c
                LEFT JOIN orders o
                    ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id
                       AND c.c_id = o.o_c_id
                LEFT JOIN order_line ol
                    ON o.o_id = ol.ol_o_id AND o.o_d_id = ol.ol_d_id
                       AND o.o_w_id = ol.ol_w_id
                WHERE c.c_w_id = {(step_id % 10) + 1}
                GROUP BY c.c_w_id, c.c_d_id, c.c_id
                ON CONFLICT DO NOTHING;""",
        ]

    def mutate_dml(self, step_id: int, rng) -> list[str]:
        suffix = step_id % 1000
        # Apply imputation: set NULL total_spend to 0 (Dolt doesn't
        # support subqueries in UPDATE SET, so we avoid rolling_avg).
        return [
            f"""UPDATE customer_features_{suffix}
                SET total_spend = 0
                WHERE total_spend IS NULL;""",
        ]

    def evaluate(self) -> list[str]:
        return [
            # Customer count (base table scan)
            "SELECT COUNT(*) FROM customer;",
            # Distribution stats on order_line amounts
            """SELECT MIN(ol_amount), MAX(ol_amount),
                      AVG(ol_amount), STDDEV(ol_amount)
               FROM order_line;""",
            # Order-customer join aggregation
            """SELECT COUNT(DISTINCT o.o_id) AS total_orders,
                      COUNT(DISTINCT o.o_c_id) AS unique_customers
               FROM orders o;""",
        ]

    def compare(self) -> list[str]:
        return [
            # Per-branch quality metrics (CASE instead of FILTER)
            """SELECT AVG(ol_amount) AS avg_spend,
                      COUNT(*) AS row_count
               FROM order_line;""",
        ]


class WhatIfOps(WorkflowOps):
    """What-if analysis (MCTS): lightweight DML + analytical reward queries.

    Mutate: UPDATE stock assignments (no DDL).
    Evaluate: Revenue and low-stock analytical queries.
    Compare: Cross-branch reward aggregation.
    """

    def mutate_ddl(self, step_id: int) -> list[str]:
        # What-if has M_DDL = 0
        return []

    def mutate_dml(self, step_id: int, rng) -> list[str]:
        # Reassign products to different warehouses
        target_w_id = rng.randint(1, 10)
        i_id_start = rng.randint(1, 90000)
        i_id_end = i_id_start + rng.randint(100, 1000)
        return [
            f"""UPDATE stock SET s_w_id = {target_w_id}
                WHERE s_i_id BETWEEN {i_id_start} AND {i_id_end}
                AND s_w_id != {target_w_id};""",
        ]

    def evaluate(self) -> list[str]:
        return [
            # Analytical reward query: revenue and low-stock
            # Using CASE instead of FILTER for Dolt compatibility
            """SELECT w.w_id,
                      SUM(ol.ol_amount) AS revenue,
                      COUNT(DISTINCT CASE WHEN s.s_quantity < 10
                            THEN s.s_i_id END) AS low_stock_items
               FROM order_line ol
               JOIN stock s ON ol.ol_i_id = s.s_i_id
                           AND ol.ol_supply_w_id = s.s_w_id
               JOIN warehouse w ON s.s_w_id = w.w_id
               GROUP BY w.w_id
               ORDER BY revenue DESC
               LIMIT 10;""",
        ]

    def compare(self) -> list[str]:
        return [
            # Same reward query used for ranking across branches
            """SELECT SUM(ol.ol_amount) AS total_revenue,
                      COUNT(DISTINCT CASE WHEN s.s_quantity < 10
                            THEN s.s_i_id END) AS low_stock
               FROM order_line ol
               JOIN stock s ON ol.ol_i_id = s.s_i_id
                           AND ol.ol_supply_w_id = s.s_w_id;""",
        ]


class SimulationOps(WorkflowOps):
    """Simulation: sequential DML per time step (no DDL).

    Mutate: INSERT orders, UPDATE stock quantities, trigger replenishment.
    Evaluate: Stockout count, total fulfillment cost.
    Compare: Cross-branch aggregation (mean, VaR, max).
    """

    def mutate_ddl(self, step_id: int) -> list[str]:
        # Simulation has M_DDL = 0
        return []

    def mutate_dml(self, step_id: int, rng) -> list[str]:
        stmts = []
        w_id = rng.randint(1, 10)
        d_id = rng.randint(1, 10)
        o_id = 5000 + step_id

        # Insert new order
        stmts.append(
            f"""INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,
                o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
                VALUES ({o_id}, {d_id}, {w_id}, {rng.randint(1, 3000)},
                        NULL, {rng.randint(1, 15)}, 1,
                        EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT)
                ON CONFLICT DO NOTHING;"""
        )

        # Insert order lines
        for ol_num in range(1, rng.randint(2, 6)):
            i_id = rng.randint(1, 100000)
            qty = rng.randint(1, 10)
            stmts.append(
                f"""INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id,
                    ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d,
                    ol_quantity, ol_amount, ol_dist_info)
                    VALUES ({o_id}, {d_id}, {w_id}, {ol_num}, {i_id},
                            {w_id}, NULL, {qty},
                            {round(rng.uniform(1, 500), 2)}, 'dist_info')
                    ON CONFLICT DO NOTHING;"""
            )

        # Decrement stock
        stmts.append(
            f"""UPDATE stock SET s_quantity = s_quantity - {rng.randint(1, 5)}
                WHERE s_i_id = {rng.randint(1, 100000)}
                AND s_w_id = {w_id}
                AND s_quantity > 5;"""
        )

        # Replenishment trigger: restock items below threshold
        stmts.append(
            f"""UPDATE stock SET s_quantity = s_quantity + 100
                WHERE s_quantity < 10 AND s_w_id = {w_id};"""
        )

        return stmts

    def evaluate(self) -> list[str]:
        return [
            # Stockout count and fulfillment cost (CASE instead of FILTER)
            """SELECT SUM(CASE WHEN s_quantity = 0 THEN 1 ELSE 0 END) AS stockouts,
                      AVG(s_quantity) AS avg_stock
               FROM stock;""",
            """SELECT SUM(ol_amount) AS total_cost,
                      COUNT(*) AS total_lines
               FROM order_line;""",
        ]

    def compare(self) -> list[str]:
        return [
            # Per-branch outcome metrics for cross-branch aggregation
            """SELECT SUM(CASE WHEN s_quantity = 0 THEN 1 ELSE 0 END) AS stockouts,
                      AVG(s_quantity) AS avg_stock,
                      MIN(s_quantity) AS min_stock
               FROM stock;""",
            """SELECT SUM(ol_amount) AS total_cost
               FROM order_line;""",
        ]


def get_workflow_ops(workflow_type) -> WorkflowOps:
    """Factory function to get the workflow operations for a given type.

    Args:
        workflow_type: WorkflowType enum value from task_pb2.

    Returns:
        An instance of the corresponding WorkflowOps subclass.
    """
    # Import here to avoid circular dependency at module level.
    from macrobench import task_pb2 as tp

    mapping = {
        tp.WorkflowType.SOFTWARE_DEV: SoftwareDevOps,
        tp.WorkflowType.FAILURE_REPRO: FailureReproOps,
        tp.WorkflowType.DATA_CURATION: DataCurationOps,
        tp.WorkflowType.WHAT_IF: WhatIfOps,
        tp.WorkflowType.SIMULATION: SimulationOps,
    }

    cls = mapping.get(workflow_type)
    if cls is None:
        raise ValueError(f"Unknown workflow type: {workflow_type}")
    return cls()
