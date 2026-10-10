"""S5: Operations agent.

The spine is production under TPC-C load with dense commits. A bad
deployment ``fault_commit_offset`` commits before the end injects a fault
from the catalog and writes the deploy_log row that is the head sentinel.
Investigation branches are created at ``investigation_branches`` commit
points among the last ``history_offset`` commits; each creates
incident_finding, evaluates the TPC-C consistency conditions, diffs
against the corrupted head and commits its findings. The spine is then
reset hard to the latest commit at which every condition held, and the
investigation branches are deleted.

Sentinels: the bad deploy's deploy_log row is present at the head, absent
on a branch created from an earlier commit, and absent after the reset.
"""

import time
from datetime import datetime

from macrobench import faults, tpcc
from macrobench.scenarios.base import Scenario, Worker, register

COMMIT_PERIOD_SEC = 0.1
BAD_VERSION = "v-bad-deploy"


@register
class OpsAgentScenario(Scenario):
    key = "ops_agent"
    name = "S5 operations agent"
    uses_spine_load = True

    def validate(self):
        p = self.params
        if p.spine_commits <= 0 or p.investigation_branches <= 0:
            raise ValueError("ops_agent needs spine_commits and investigation_branches > 0")
        if p.fault_commit_offset <= 0 or p.fault_commit_offset >= p.spine_commits:
            raise ValueError("ops_agent.fault_commit_offset must be in (0, spine_commits)")

    def total_units(self) -> int:
        return self.params.spine_commits + self.params.investigation_branches + 1

    # ------------------------------------------------------------------

    def run(self):
        ctx, p, inv = self.ctx, self.params, self.ctx.invariants
        suite = ctx.new_suite()
        rng = ctx.rng
        commits = []   # (k, hash or None, iso timestamp)
        fault, fw, fd = faults.pick_fault(rng, ctx.scale.warehouses)
        fault_k = p.spine_commits - p.fault_commit_offset
        try:
            # 1. Production with dense commits; the bad deploy lands at fault_k.
            for k in range(p.spine_commits):
                ctx.check_stop()
                if self.spine_load is None:
                    suite.exec(lambda db: tpcc.run_transaction(
                        tpcc.pick_transaction(rng), db, rng, ctx.scale),
                        refs=[ctx.spine], label="spine")
                else:
                    time.sleep(COMMIT_PERIOD_SEC)
                with self.quiesced():
                    if k == fault_k:
                        suite.exec(self._deploy_script(k, fault, fw, fd, bad=True),
                                   refs=[ctx.spine], label="bad_deploy")
                    elif k % 5 == 0:
                        suite.exec(self._deploy_script(k, None, fw, fd, bad=False),
                                   refs=[ctx.spine], label="deploy")
                    c = suite.commit(ctx.spine, f"prod commit {k}", label="spine_commit")
                commits.append((k, c.value if (c.ok and c.value) else None,
                                datetime.now().isoformat(timespec="seconds")))
                ctx.tick()

            if self.spine_load is not None and not p.load_during_investigation:
                self.spine_load.stop()

            # 2. Investigation branches at commit points near the head.
            t_start = time.time()
            window = commits[-(p.history_offset + 1):-1] if p.history_offset > 0 else commits[:-1]
            if not window:
                window = commits[:-1] or commits
            n = min(p.investigation_branches, len(window))
            picks = [window[int(i * (len(window) - 1) / max(1, n - 1))] for i in range(n)] if n > 1 else [window[-1]]
            picks = sorted(set(picks))
            findings = ctx.run_workers(picks, lambda w, c: self._investigate(w, c, fault_k))
            good = [f for f in findings if isinstance(f, dict) and f["all_hold"]]
            if good:
                target = max(good, key=lambda f: f["k"])
            else:
                target = min((f for f in findings if isinstance(f, dict)), key=lambda f: f["k"], default=None)

            # 3. Reset the spine (PITR).
            if target is None:
                inv.not_applicable("S5.2 head sentinel gone after reset", "no investigation results")
                reset = None
            else:
                to = target["hash"] or target["timestamp"]
                with self.quiesced():
                    reset = ctx.retry(lambda: suite.reset(ctx.spine, to, label="pitr_reset"))
                ctx.add_metric("reset_status", reset.status_name)
                ctx.add_metric("reset_target_commit", target["k"])
                ctx.add_metric("fault_commit", fault_k)
                if reset.unsupported:
                    inv.not_applicable("S5.2 head sentinel gone after reset", "reset unsupported")
                else:
                    r = suite.exec([("SELECT COUNT(*) FROM deploy_log WHERE version = %s", (BAD_VERSION,))],
                                   refs=[ctx.spine], label="invariant")[0]
                    present = int(r.rows[0][0]) if r.rows else None
                    if target["k"] < fault_k:
                        inv.expect("S5.2 head sentinel gone after reset", present, 0,
                                   f"reset {reset.status_name} to commit {target['k']}")
                    else:
                        inv.not_applicable("S5.2 head sentinel gone after reset",
                                           f"selected commit {target['k']} is after the bad deploy {fault_k}")
            ctx.add_metric("time_to_recovery_sec", round(time.time() - t_start, 3))
            ctx.tick()

            # 4. Delete the investigation branches.
            for f in findings:
                if isinstance(f, dict):
                    ctx.delete(suite, f["branch"], label="investigation_delete")
                    ctx.tick()
        finally:
            ctx.close_suite(suite)

    # ------------------------------------------------------------------

    def _deploy_script(self, k, fault, fw, fd, bad):
        def script(db):
            if bad:
                fault.inject(db, fw, fd)
                db.sql("INSERT INTO deploy_log (deploy_id, version, deployed_at, note) "
                       "VALUES (%s, %s, CURRENT_TIMESTAMP, %s)",
                       (k, BAD_VERSION, f"sentinel: {fault.name} in w{fw} d{fd}"))
            else:
                db.sql("INSERT INTO deploy_log (deploy_id, version, deployed_at, note) "
                       "VALUES (%s, %s, CURRENT_TIMESTAMP, 'routine')", (k, f"v{k}"))
        return script

    def _investigate(self, w: Worker, commit, fault_k) -> dict:
        ctx, inv, suite = self.ctx, self.ctx.invariants, w.suite
        k, h, ts = commit
        branch = f"inv_{k}"
        ref = f"{ctx.spine}@{h}" if h else ctx.spine
        res = ctx.branch(suite, branch, ref, label="investigation_branch")
        if not res.ok:
            ctx.note(f"investigation branch {branch}: {res.status_name} {res.error}")
            ctx.tick()
            return None

        def script(db):
            db.sql("CREATE TABLE incident_finding (finding_id INT NOT NULL, branch_name VARCHAR(64), "
                   "commit_hash VARCHAR(64), condition_name VARCHAR(8), violations INT, "
                   "PRIMARY KEY (finding_id))")
            violations = faults.check_consistency(db)
            for i, (cond, v) in enumerate(violations.items()):
                db.sql("INSERT INTO incident_finding (finding_id, branch_name, commit_hash, "
                       "condition_name, violations) VALUES (%s, %s, %s, %s, %s)",
                       (k * 100 + i, branch, h or "", cond, v))
            rows = db.sql("SELECT COUNT(*) FROM deploy_log WHERE version = %s", (BAD_VERSION,))
            return {"violations": violations, "bad_deploy_rows": int(rows[0][0]) if rows else None}
        r = w.exec(script, branch, label="investigate")
        value = r.value if (r.ok and isinstance(r.value, dict)) else {"violations": {}, "bad_deploy_rows": None}
        ctx.retry(lambda: suite.diff(ctx.spine, branch, label="investigate_diff"))
        w.commit(branch, f"findings at commit {k}", label="findings_commit")
        if k < fault_k:
            if h is None:
                inv.not_applicable(f"S5.1 head sentinel absent on branch from commit {k}",
                                   "backend has no commits; branch is at the head")
            else:
                inv.expect(f"S5.1 head sentinel absent on branch from commit {k}",
                           value["bad_deploy_rows"], 0)
        ctx.tick()
        return {"k": k, "hash": h, "timestamp": ts, "branch": branch,
                "all_hold": faults.all_hold(value["violations"]),
                "violations": value["violations"]}
