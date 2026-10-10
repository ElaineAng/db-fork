"""S4: Development agent.

The spine is production: TPC-C traffic from the spine load, periodic
checkpoints, and a schema migration from the catalog every
``spine_migration_interval`` dev steps. Dev branches are created from the
spine (``concurrent_branches`` alive at once, ``dev_branches`` in total).
Each implements the loyalty feature: loyalty_tier and loyalty_event tables,
customer.c_tier, and a chunked backfill of customer rows (a share
``key_overlap`` of which also rewrites c_data, which Payment transactions
on the spine rewrite too). The branch is hot for ``dev_phase_steps``, then
quiet for ``review_phase_steps`` with occasional modifications, rebases
onto the spine once or twice (conflicts resolved in the branch's favour),
and is deleted without merging.

Sentinels: loyalty_tier rows exist only on dev branches; item 100001+i is
inserted on the spine after branch i exists and must be visible on the
branch only after a rebase.
"""

import threading
import time

from macrobench.scenarios.base import Scenario, Worker, register
from macrobench.datagen.ch import DISTRICTS_PER_WAREHOUSE

SPINE_SENTINEL_BASE = 100_000
REVIEW_IDLE_SEC = 0.05
SPINE_COMMIT_PERIOD_SEC = 2.0

# Spine migration catalog M1-M5: (name, target table, ddl(k)).
MIGRATIONS = [
    ("M1_customer_segment", "customer",
     lambda k: f"ALTER TABLE customer ADD COLUMN c_segment_{k} VARCHAR(8)"),
    ("M2_stock_reorder", "stock",
     lambda k: f"ALTER TABLE stock ADD COLUMN s_reorder_{k} INT"),
    ("M3_item_category", "item",
     lambda k: f"ALTER TABLE item ADD COLUMN i_category_{k} VARCHAR(16) DEFAULT 'general'"),
    ("M4_promo_table", "promo",
     lambda k: f"CREATE TABLE promo_{k} (promo_id INT NOT NULL, i_id INT, "
               f"discount DECIMAL(4,2), PRIMARY KEY (promo_id))"),
    ("M5_district_manager", "district",
     lambda k: f"ALTER TABLE district ADD COLUMN d_manager_{k} VARCHAR(16)"),
]


@register
class DevAgentScenario(Scenario):
    key = "dev_agent"
    name = "S4 development agent"
    uses_spine_load = True
    spine_touches_c_data = True

    def validate(self):
        p = self.params
        if p.dev_branches <= 0 or p.dev_phase_steps <= 0:
            raise ValueError("dev_agent needs dev_branches and dev_phase_steps > 0")

    def total_units(self) -> int:
        p = self.params
        return p.dev_branches * (p.dev_phase_steps + p.review_phase_steps)

    # ------------------------------------------------------------------

    def run(self):
        ctx, p = self.ctx, self.params
        self._spine_lock = threading.Lock()
        self._dev_steps = 0
        self._migrations = 0
        self._counter_lock = threading.Lock()
        done = threading.Event()
        prod = threading.Thread(target=self._production_loop, args=(done,),
                                daemon=True, name="production")
        prod.start()
        try:
            ctx.run_workers(list(range(1, p.dev_branches + 1)), self._dev_branch,
                            threads=max(1, p.concurrent_branches or ctx.worker_threads))
        finally:
            done.set()
            prod.join(60)
        ctx.add_metric("dev_steps", self._dev_steps)
        ctx.add_metric("spine_migrations", self._migrations)

    def _production_loop(self, done: threading.Event):
        """Spine checkpoints and migrations while dev branches work."""
        import dblib.result_collector as rc
        rc.set_current_thread_id(800)
        ctx, p = self.ctx, self.params
        suite = ctx.new_suite()
        rng = ctx.worker_rng(800)
        last_commit = time.time()
        next_migration_at = p.spine_migration_interval or 0
        try:
            while not done.is_set():
                with self._counter_lock:
                    steps = self._dev_steps
                if next_migration_at and steps >= next_migration_at:
                    self._migrate(suite, rng)
                    next_migration_at += p.spine_migration_interval
                    last_commit = time.time()
                elif time.time() - last_commit >= SPINE_COMMIT_PERIOD_SEC:
                    self._spine_commit(suite, "production checkpoint")
                    last_commit = time.time()
                done.wait(0.2)
        finally:
            ctx.close_suite(suite)

    def _spine_commit(self, suite, message, label="spine_commit"):
        with self._spine_lock, self.quiesced():
            return suite.commit(self.ctx.spine, message, label=label)

    def _migrate(self, suite, rng):
        ctx, p = self.ctx, self.params
        self._migrations += 1
        k = self._migrations
        if rng.random() < p.key_overlap:
            name, table, ddl = MIGRATIONS[0]        # targets customer
        else:
            name, table, ddl = rng.choice(MIGRATIONS[1:])
        stmt = ddl(k)

        def script(db):
            db.sql(stmt)
            db.sql("INSERT INTO migration_log (migration_id, name, applied_step, applied_at) "
                   "VALUES (%s, %s, %s, CURRENT_TIMESTAMP)", (k, name, self._dev_steps))
        with self._spine_lock, self.quiesced():
            res = suite.exec(script, refs=[ctx.spine], label="spine_migration")[0]
            suite.commit(ctx.spine, f"migration {name} #{k}", label="migration_commit")
        if not res.ok:
            ctx.note(f"migration {name}: {res.status_name} {res.error[:100]}")

    # ------------------------------------------------------------------

    def _dev_branch(self, w: Worker, i: int):
        ctx, p, inv, suite, rng = self.ctx, self.params, self.ctx.invariants, w.suite, w.rng
        branch = f"dev_{i}"
        res = ctx.branch(suite, branch, ctx.spine, label="dev_branch")
        if not res.ok:
            ctx.note(f"dev branch {branch}: {res.status_name} {res.error}")
            ctx.tick(p.dev_phase_steps + p.review_phase_steps)
            return None
        # Spine sentinel: inserted on the spine after the branch exists.
        sentinel_id = SPINE_SENTINEL_BASE + i
        with self._spine_lock, self.quiesced():
            suite.exec([("INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) "
                         "VALUES (%s, 1, %s, 1.00, 'spine sentinel')",
                         (sentinel_id, f"spine_sentinel_{i}"))],
                       refs=[ctx.spine], label="spine_sentinel")
            suite.commit(ctx.spine, f"spine sentinel {i}", label="spine_commit")

        rebase_steps = {p.dev_phase_steps}
        if p.rebases_per_branch >= 2 and p.dev_phase_steps >= 2:
            rebase_steps.add(p.dev_phase_steps // 2)
        rebases_done = 0
        chunk = 0

        def sentinel_visible():
            r = w.exec([("SELECT COUNT(*) FROM item WHERE i_id = %s", (sentinel_id,))],
                       branch, label="invariant")
            return int(r.rows[0][0]) if r.rows else None

        # Dev phase.
        for step in range(1, p.dev_phase_steps + 1):
            ctx.check_stop()
            if step == 1:
                w.exec(self._feature_ddl(i), branch, label="feature_ddl")
            else:
                w.exec(self._backfill_script(i, step, chunk, rng), branch, label="backfill")
                chunk += ctx.statements_per_step
            committed = ctx.should_commit(step - 1, last_step=(step == p.dev_phase_steps))
            if committed:
                w.commit(branch, f"dev {i} step {step}", label="dev_commit")
            with self._counter_lock:
                self._dev_steps += 1
            if step in rebase_steps:
                if not committed:  # a rebase needs a clean working set
                    w.commit(branch, f"dev {i} step {step} (pre-rebase)", label="dev_commit")
                if rebases_done == 0:
                    inv.expect(f"S4.2a spine sentinel invisible on {branch} before rebase",
                               sentinel_visible(), 0)
                with self._spine_lock:
                    rb = ctx.retry(lambda: suite.rebase(branch, ctx.spine, on_conflict="theirs", label="dev_rebase"))
                rebases_done += 1
                if rb.ok and isinstance(rb.value, dict):
                    ctx.bump_metric("rebase_conflicts", int(rb.value.get("conflicts", 0)))
                elif rb.failed:
                    ctx.bump_metric("rebase_failed")
                    ctx.note(f"{branch} rebase: {rb.error[:120]}")
                if rebases_done == 1:
                    if rb.unsupported:
                        inv.not_applicable(f"S4.2b spine sentinel visible on {branch} after rebase",
                                           "rebase unsupported")
                    else:
                        inv.expect(f"S4.2b spine sentinel visible on {branch} after rebase",
                                   sentinel_visible(), 1, f"rebase {rb.status_name}")
            ctx.tick()

        # Review phase: quiet, occasional modification.
        modified = False
        for step in range(1, p.review_phase_steps + 1):
            ctx.check_stop()
            if rng.random() < p.review_modification_prob:
                w.exec([("UPDATE loyalty_tier SET min_payment = min_payment + 1 WHERE tier_id = 2", None),
                        ("SELECT COUNT(*) FROM loyalty_event", None)],
                       branch, label="review_edit")
                modified = True
                if ctx.should_commit(step - 1):
                    w.commit(branch, f"dev {i} review {step}", label="dev_commit")
                    modified = False
            else:
                time.sleep(REVIEW_IDLE_SEC)
            ctx.tick()
        if modified:
            w.commit(branch, f"dev {i} review end", label="dev_commit")

        # Invariant: feature data never leaks into production.
        # MySQL-protocol backends list every database's tables in
        # information_schema, so count only the current one there; the
        # dialect probe is untimed so its miss on Postgres is not a
        # recorded failure.
        r = suite.exec([("SELECT COUNT(*) FROM information_schema.tables "
                         "WHERE table_name = %s AND table_schema = DATABASE()",
                         ("loyalty_tier",))], refs=[ctx.spine], label="invariant", timed=False)[0]
        if not r.ok:
            r = suite.exec([("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s",
                             ("loyalty_tier",))], refs=[ctx.spine], label="invariant")[0]
        if r.ok and r.rows:
            tables = int(r.rows[0][0])
        else:
            # No information_schema: probe the table itself (untimed, since a
            # missing table is the expected outcome, not a failed op).
            probe = suite.exec(["SELECT COUNT(*) FROM loyalty_tier"], refs=[ctx.spine],
                               timed=False)[0]
            tables = 0 if probe.failed else 1
        inv.expect(f"S4.1 loyalty_tier never on the spine (dev {i})", tables, 0,
                   "loyalty_tier tables on the spine")

        for _ in range(max(0, ctx.branch_ops.retention_steps)):
            time.sleep(REVIEW_IDLE_SEC)
        ctx.delete(suite, branch, label="dev_delete")
        return rebases_done

    def _feature_ddl(self, i: int):
        def script(db):
            db.sql("CREATE TABLE loyalty_tier (tier_id INT NOT NULL, name VARCHAR(16) NOT NULL, "
                   "min_payment DECIMAL(12,2) NOT NULL, PRIMARY KEY (tier_id))")
            db.sql("CREATE TABLE loyalty_event (event_id INT NOT NULL, c_w_id INT, c_d_id INT, "
                   "c_id INT, tier_id INT, created_at TIMESTAMP, PRIMARY KEY (event_id))")
            db.sql("ALTER TABLE customer ADD COLUMN c_tier VARCHAR(8)")
            db.sql("INSERT INTO loyalty_tier (tier_id, name, min_payment) VALUES "
                   "(1, 'bronze', 0), (2, 'silver', 5000), (3, 'gold', 9000)")
        return script

    def _backfill_script(self, i: int, step: int, chunk: int, rng):
        ctx = self.ctx
        n = ctx.statements_per_step
        rows = ctx.rows_per_write
        customers = ctx.scale.customers_per_district
        per_district = max(1, (customers + rows - 1) // rows)
        overlap = self.params.key_overlap

        def script(db):
            for k in range(n):
                idx = chunk + k
                w_id = 1 + (idx // (per_district * DISTRICTS_PER_WAREHOUSE)) % ctx.scale.warehouses
                d_id = 1 + (idx // per_district) % DISTRICTS_PER_WAREHOUSE
                lo = 1 + (idx % per_district) * rows
                hi = min(customers, lo + rows - 1)
                if rng.random() < overlap:
                    db.sql("UPDATE customer SET c_tier = CASE WHEN c_ytd_payment > 9000 THEN 'gold' "
                           "WHEN c_ytd_payment > 5000 THEN 'silver' ELSE 'bronze' END, "
                           "c_data = 'loyalty backfill' "
                           "WHERE c_w_id = %s AND c_d_id = %s AND c_id BETWEEN %s AND %s",
                           (w_id, d_id, lo, hi))
                else:
                    db.sql("UPDATE customer SET c_tier = CASE WHEN c_ytd_payment > 9000 THEN 'gold' "
                           "WHEN c_ytd_payment > 5000 THEN 'silver' ELSE 'bronze' END "
                           "WHERE c_w_id = %s AND c_d_id = %s AND c_id BETWEEN %s AND %s",
                           (w_id, d_id, lo, hi))
                db.sql("INSERT INTO loyalty_event (event_id, c_w_id, c_d_id, c_id, tier_id, created_at) "
                       "VALUES (%s, %s, %s, %s, 1, CURRENT_TIMESTAMP)",
                       (i * 1_000_000 + step * 1000 + k, w_id, d_id, lo))
            db.sql("SELECT c_tier, COUNT(*) FROM customer GROUP BY c_tier")
        return script
