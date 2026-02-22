"""Per-workflow SQL operations for the macrobenchmark.

Each workflow class provides concrete SQL statements for the phases
of a step: mutate (DDL + DML), evaluate (read queries), and
compare (cross-branch queries spread across steps).

SQL statements are derived from Figure 1 in the paper (Section 3.3).
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


class WorkflowOps(ABC):
    """Abstract base class for workflow-specific SQL operations."""

    @abstractmethod
    def mutate_ddl(self, step_id: int) -> list[str]:
        """Return DDL statements for this step.

        Args:
            step_id: Step number within this thread.

        Returns:
            List of SQL DDL strings.
        """

    @abstractmethod
    def mutate_dml(self, step_id: int, rng, thread_id: int = 0) -> list[str]:
        """Return DML statements for this step.

        Args:
            step_id: Step number within this thread.
            rng: random.Random instance for generating random values.
            thread_id: Worker thread ID (used by some workflows to
                       differentiate strategies).

        Returns:
            List of SQL DML strings.
        """

    @abstractmethod
    def evaluate(self) -> list[str]:
        """Return per-branch evaluation queries for this step.

        Returns:
            List of SQL SELECT strings.
        """

    @abstractmethod
    def compare(self) -> list[str]:
        """Return cross-branch comparison queries.

        These queries are executed on each committed leaf branch
        individually (the runner connects to each branch in turn)
        during cross-branch query passes.

        Returns:
            List of SQL SELECT strings.  Empty list if C=--- for
            this workflow.
        """


class SoftwareDevOps(WorkflowOps):
    """Software development workflow: DDL-heavy schema migrations.

    Mutate: ALTER TABLE to add columns, UPDATE to backfill.
    Evaluate: Null check + distribution query.
    Compare: Distribution query across branches.
    """

    def mutate_ddl(self, step_id: int) -> list[str]:
        # M_s=1 DDL per step.  We provide two options so the runner
        # can pick up to schema_changes of them.
        return [
            "ALTER TABLE customer ADD COLUMN loyalty_tier VARCHAR(8);",
            "ALTER TABLE customer ADD COLUMN c_credit_limit DECIMAL(10,2);",
        ]

    def mutate_dml(self, step_id: int, rng, thread_id: int = 0) -> list[str]:
        # M_d=1 DML per step: backfill the loyalty_tier column.
        return [
            """UPDATE customer SET loyalty_tier = CASE
                   WHEN c_ytd_payment > 9000 THEN 'Gold'
                   WHEN c_ytd_payment > 5000 THEN 'Silver'
                   ELSE 'Bronze'
               END;""",
        ]

    def evaluate(self) -> list[str]:
        # Q_v=2
        return [
            "SELECT COUNT(*) FROM customer WHERE loyalty_tier IS NULL;",
            """SELECT loyalty_tier, COUNT(*), AVG(c_ytd_payment)
               FROM customer GROUP BY loyalty_tier;""",
        ]

    def compare(self) -> list[str]:
        # C=1 cross-branch query
        return [
            """SELECT loyalty_tier, COUNT(*), AVG(c_ytd_payment)
               FROM customer GROUP BY loyalty_tier;""",
        ]


class FailureReproOps(WorkflowOps):
    """Failure reproduction: transaction replay + constraint modification.

    Mutate: INSERT/UPDATE/DELETE/ALTER replaying a recorded transaction log.
    Evaluate: Invariant check (order line count mismatch).
    Compare: No cross-branch queries (C=---).
    """

    def mutate_ddl(self, step_id: int) -> list[str]:
        # Part of the mixed DDL+DML replay.  Up to M_s=5 schema changes.
        suffix = step_id % 1000
        stmts = [
            "ALTER TABLE order_line ADD COLUMN "
            f"ol_discount_{suffix} DECIMAL(5,2);",
        ]
        # Additional DDL ops to fill M_s=5
        if step_id % 2 == 0:
            stmts.append(
                f"ALTER TABLE order_line DROP COLUMN ol_discount_{suffix};"
            )
        stmts.append(
            f"ALTER TABLE order_line ADD CONSTRAINT "
            f"ol_amount_check_{suffix} CHECK (ol_amount >= 0);"
        )
        stmts.append(
            f"ALTER TABLE order_line DROP CONSTRAINT IF EXISTS "
            f"ol_amount_check_{suffix};"
        )
        stmts.append(
            f"ALTER TABLE orders ADD COLUMN "
            f"o_flag_{suffix} BOOLEAN DEFAULT false;"
        )
        return stmts

    def mutate_dml(self, step_id: int, rng, thread_id: int = 0) -> list[str]:
        # M_d=45 DML statements: replay recorded transaction prefix.
        w_id = rng.randint(1, 10)
        d_id = rng.randint(1, 10)
        o_id = 3000 + step_id
        stmts = []

        # Insert order
        stmts.append(
            f"""INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,
                o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
                VALUES ({o_id}, {d_id}, {w_id}, 42, NULL, 5, 1,
                        CURRENT_TIMESTAMP)
                ON CONFLICT DO NOTHING;"""
        )

        # Update customer balance
        stmts.append(
            f"""UPDATE customer SET c_balance = c_balance - 72.50
                WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = 42;"""
        )

        # Delete order lines (replay of cleanup)
        stmts.append(
            f"""DELETE FROM order_line
                WHERE ol_o_id = {o_id} AND ol_d_id = {d_id}
                AND ol_w_id = {w_id};"""
        )

        # Pad with order line inserts to reach ~45 DML
        for i in range(42):
            ol_num = i + 1
            amount = rng.choice([-1, 0, 1, 50, 100])
            stmts.append(
                f"""INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id,
                    ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d,
                    ol_quantity, ol_amount, ol_dist_info)
                    VALUES ({o_id}, {d_id}, {w_id}, {ol_num}, 1, {w_id},
                            NULL, 5, {amount}, 'dist_info')
                    ON CONFLICT DO NOTHING;"""
            )

        return stmts

    def evaluate(self) -> list[str]:
        # Q_v=1: invariant check — order line count mismatch
        return [
            """SELECT o_id FROM orders o
               WHERE o_ol_cnt <> (
                   SELECT COUNT(*) FROM order_line ol
                   WHERE ol.ol_o_id = o.o_id
                     AND ol.ol_d_id = o.o_d_id
                     AND ol.ol_w_id = o.o_w_id
               );""",
        ]

    def compare(self) -> list[str]:
        # C=--- (no cross-branch queries for failure repro)
        return []


class DataCleaningOps(WorkflowOps):
    """Data cleaning: multiple strategies on overlapping data regions.

    Each worker thread applies a different cleaning strategy on its branch.
    Mutate: UPDATE (impute) or DELETE (drop nulls) on customer.c_balance.
    Evaluate: Per-branch correctness check (negative balance count).
    Compare: Cross-branch quality ranking (null count + spread).
    """

    def mutate_ddl(self, step_id: int) -> list[str]:
        # M_s=1 — data cleaning may add a flag column
        return [
            "ALTER TABLE customer ADD COLUMN c_cleaned BOOLEAN DEFAULT false;",
        ]

    def mutate_dml(self, step_id: int, rng, thread_id: int = 0) -> list[str]:
        # M_d=1: strategy depends on thread_id
        if thread_id % 2 == 0:
            # Strategy A: impute missing c_balance with 0 (Dolt does not
            # support correlated subqueries in UPDATE SET).
            return [
                "UPDATE customer SET c_balance = 0 WHERE c_balance IS NULL;",
            ]
        else:
            # Strategy B: drop rows with null c_balance
            return [
                "DELETE FROM customer WHERE c_balance IS NULL;",
            ]

    def evaluate(self) -> list[str]:
        # Q_v=1: per-branch correctness check
        return [
            """SELECT COUNT(CASE WHEN c_balance < 0 THEN 1 END) AS invalid
               FROM customer;""",
        ]

    def compare(self) -> list[str]:
        # C=2: cross-branch quality ranking
        return [
            """SELECT COUNT(CASE WHEN c_balance IS NULL THEN 1 END) AS nulls,
                      MAX(c_ytd_payment) - MIN(c_ytd_payment) AS spread
               FROM customer;""",
        ]


class MctsOps(WorkflowOps):
    """Monte Carlo Tree Search: lightweight DML + analytical reward queries.

    Mutate: UPDATE stock assignments (no DDL).
    Evaluate: Revenue query (total fulfillment cost).
    Compare: No cross-branch queries (C=---).
    """

    def mutate_ddl(self, step_id: int) -> list[str]:
        # M_s=--- (no DDL)
        return []

    def mutate_dml(self, step_id: int, rng, thread_id: int = 0) -> list[str]:
        # M_d=1: reassign stock quantity
        w_id = rng.randint(1, 10)
        i_id = rng.randint(1, 100000)
        qty = rng.randint(1, 10)
        return [
            f"""UPDATE stock SET s_quantity = s_quantity - {qty}
                WHERE s_w_id = {w_id} AND s_i_id = {i_id}
                AND s_quantity >= {qty};""",
        ]

    def evaluate(self) -> list[str]:
        # Q_v=1: reward query — total fulfillment cost
        return [
            """SELECT SUM(ol_amount) AS total_cost
               FROM order_line ol
               JOIN warehouse w ON ol.ol_supply_w_id = w.w_id;""",
        ]

    def compare(self) -> list[str]:
        # C=--- (no cross-branch queries)
        return []


class SimulationOps(WorkflowOps):
    """MC Simulation: sequential DML per time step (no DDL).

    Mutate: INSERT orders, UPDATE stock (decrement + replenish), ~50 stmts.
    Evaluate: Stockout count + total fulfillment cost.
    Compare: Same metrics for cross-branch aggregation.
    """

    def mutate_ddl(self, step_id: int) -> list[str]:
        # M_s=--- (no DDL)
        return []

    def mutate_dml(self, step_id: int, rng, thread_id: int = 0) -> list[str]:
        # M_d=50: ~30 rounds of (insert order + decrement stock + replenish)
        stmts = []
        w_id = rng.randint(1, 10)

        for day in range(16):
            d_id = rng.randint(1, 10)
            o_id = 5000 + thread_id * 1000 + step_id * 100 + day

            # Demand arrives: insert order
            stmts.append(
                f"""INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,
                    o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
                    VALUES ({o_id}, {d_id}, {w_id}, {rng.randint(1, 3000)},
                            NULL, 5, 1, CURRENT_TIMESTAMP)
                    ON CONFLICT DO NOTHING;"""
            )

            # Fulfill: decrement stock
            i_id = rng.randint(1, 100000)
            qty = rng.randint(1, 5)
            stmts.append(
                f"""UPDATE stock SET s_quantity = s_quantity - {qty}
                    WHERE s_i_id = {i_id} AND s_w_id = {w_id}
                    AND s_quantity >= {qty};"""
            )

            # Replenish: restock items below threshold
            stmts.append(
                f"""UPDATE stock SET s_quantity = s_quantity + 100
                    WHERE s_quantity < 10 AND s_w_id = {w_id};"""
            )

        # Remaining stmts to reach ~50 (16*3 = 48, add 2 more)
        stmts.append(
            f"""INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id,
                ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d,
                ol_quantity, ol_amount, ol_dist_info)
                VALUES (5000 + {thread_id}, 1, {w_id}, 1,
                        {rng.randint(1, 100000)}, {w_id}, NULL, 3,
                        {round(rng.uniform(1, 500), 2)}, 'dist_info')
                ON CONFLICT DO NOTHING;"""
        )
        stmts.append(
            f"""UPDATE stock SET s_quantity = s_quantity - 1
                WHERE s_i_id = {rng.randint(1, 100000)}
                AND s_w_id = {w_id} AND s_quantity > 0;"""
        )

        return stmts

    def evaluate(self) -> list[str]:
        # Q_v=1: per-branch outcome metrics
        return [
            """SELECT SUM(CASE WHEN s_quantity = 0 THEN 1 ELSE 0 END) AS stockouts,
                      SUM(ol.ol_amount) AS total_cost
               FROM stock s
               JOIN order_line ol ON s.s_i_id = ol.ol_i_id;""",
        ]

    def compare(self) -> list[str]:
        # C=1: cross-branch aggregation (per-branch portion)
        return [
            """SELECT SUM(CASE WHEN s_quantity = 0 THEN 1 ELSE 0 END) AS stockouts,
                      SUM(ol.ol_amount) AS total_cost
               FROM stock s
               JOIN order_line ol ON s.s_i_id = ol.ol_i_id;""",
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
        tp.WorkflowType.DATA_CLEANING: DataCleaningOps,
        tp.WorkflowType.MCTS: MctsOps,
        tp.WorkflowType.SIMULATION: SimulationOps,
    }

    cls = mapping.get(workflow_type)
    if cls is None:
        raise ValueError(f"Unknown workflow type: {workflow_type}")
    return cls()
