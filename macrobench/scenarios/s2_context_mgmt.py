"""S2: Agent context management.

A single spine records an assistant's interactions (turn and tool_call
rows). Every ``compaction_interval`` steps the spine is committed and
``fanout`` candidate branches try a curation strategy each (A: summarize
old turns, B: replace large tool results by references, C: deduplicate
turns), commit, and read their curated context through a view. A
cross-branch policy check picks one candidate and the spine fast-forwards
to it. At the end of the run the strategy chosen at ``rejected_cycle`` is
rejected: its compaction commit is reverted on the spine, the spine is
rebased onto the retained alternative candidate and fast-forwarded again.
Candidates are deleted ``retention_steps`` interaction steps after their
compaction point (the alternative kept for the rejection is deleted last).

The rebase replays every spine commit since the compaction point onto the
alternative, including the rejected compaction commit and its revert;
these cancel out, so the result equals replaying the work commits only.
Conflict-freedom is a schema property: candidates rewrite old rows and add
notes, views and indexes; the spine only appends new turn/tool_call rows.

Sentinels: the ``note`` row with kind 'compaction' naming the candidate.
"""

from macrobench.scenarios.base import Scenario, register

STRATEGIES = ["summarize", "reference", "dedup"]
INDEX_COLUMNS = {"summarize": "(step)", "reference": "(session_id, step)",
                 "dedup": "(role, step)"}
LETTERS = "ABCDEFGHIJ"


@register
class ContextMgmtScenario(Scenario):
    key = "context_mgmt"
    name = "S2 context management"

    def validate(self):
        p = self.params
        if p.compaction_interval <= 0 or p.fanout <= 0 or p.compaction_cycles <= 0:
            raise ValueError("context_mgmt needs compaction_interval, fanout and compaction_cycles > 0")
        if p.fanout > len(LETTERS):
            raise ValueError("context_mgmt fanout must be <= 10")

    def total_units(self) -> int:
        p = self.params
        return p.compaction_cycles * (p.compaction_interval + 1) + 1

    # ------------------------------------------------------------------

    def run(self):
        ctx, p = self.ctx, self.params
        suite = ctx.new_suite()
        rng = ctx.rng
        self._turn_id = 0
        self._call_id = 0
        self._note_id = 0
        step = 0
        pending_delete = []    # (branch, delete_at_step)
        chosen = {}            # cycle -> dict(branch, commit, strategy)
        alternatives = {}      # cycle -> dict(branch, commit, strategy)
        retention = max(0, ctx.branch_ops.retention_steps)
        ff_count = 0

        try:
            for cycle in range(1, p.compaction_cycles + 1):
                # Interactions on the spine.
                for k in range(p.compaction_interval):
                    ctx.check_stop()
                    suite.exec(self._interaction_script(step, rng), refs=[ctx.spine],
                               label="interaction")
                    if ctx.should_commit(k):
                        suite.commit(ctx.spine, f"work step {step}", label="work_commit")
                    step += 1
                    pending_delete = self._retire(suite, pending_delete, step)
                    ctx.tick()

                # Compaction point.
                ctx.check_stop()
                suite.commit(ctx.spine, f"pre-compaction {cycle}", label="pre_compaction_commit")
                cands = []
                for j in range(p.fanout):
                    name = f"cand_{cycle}_{LETTERS[j]}"
                    strategy = STRATEGIES[j % len(STRATEGIES)]
                    res = ctx.branch(suite, name, ctx.spine, label="candidate_branch")
                    if not res.ok:
                        ctx.note(f"candidate {name}: {res.status_name} {res.error}")
                        continue
                    suite.exec(self._curation_script(name, strategy, step, rng),
                               refs=[name], label="curation")
                    c = suite.commit(name, f"compaction {cycle} {strategy} ({name})",
                                     label="curation_commit")
                    suite.exec([f"SELECT COUNT(*), SUM(token_count) FROM context_v_{name}"],
                               refs=[name], label="curated_context")
                    cands.append({"branch": name, "commit": c.value if c.ok else None,
                                  "strategy": strategy})
                if not cands:
                    ctx.tick()
                    continue

                pick = self._policy_check(suite, cands)
                if cycle == p.rejected_cycle and len(cands) >= 2:
                    pick, alt = 1, 0
                    alternatives[cycle] = cands[alt]
                sel = cands[pick]
                chosen[cycle] = sel
                m = suite.merge(ctx.spine, sel["branch"],
                                message=f"fast-forward to {sel['branch']}", label="fast_forward")
                if m.ok:
                    ff = bool(isinstance(m.value, dict) and m.value.get("fast_forward"))
                    ff_count += int(ff)
                    if not ff:
                        ctx.bump_metric("non_ff_merges")
                for c in cands:
                    if c is not alternatives.get(cycle):
                        pending_delete.append((c["branch"], step + retention))
                ctx.tick()

            # Rejection of a previous compaction.
            self._reject(suite, chosen, alternatives, step)
            ctx.tick()
        finally:
            for branch, _ in pending_delete:
                ctx.delete(suite, branch, label="candidate_delete")
            for c in alternatives.values():
                ctx.delete(suite, c["branch"], label="candidate_delete")
            ctx.add_metric("interaction_steps", step)
            ctx.add_metric("fast_forwards", ff_count)
            ctx.close_suite(suite)

    # ------------------------------------------------------------------

    def _retire(self, suite, pending, step):
        keep = []
        for branch, at in pending:
            if step >= at:
                self.ctx.delete(suite, branch, label="candidate_delete")
            else:
                keep.append((branch, at))
        return keep

    def _interaction_script(self, step, rng):
        ctx = self.ctx
        n = ctx.statements_per_step
        writes = max(1, round(n * ctx.data_ops.write_fraction)) if n > 1 else 1
        reads = max(0, n - writes)
        blob = "x" * (10 * ctx.rows_per_write)
        topic = rng.randint(1, 20)  # repeats -> duplicates for strategy C

        def script(db):
            for k in range(writes):
                self._turn_id += 1
                db.sql("INSERT INTO turn (turn_id, session_id, step, role, content, token_count, created_at) "
                       "VALUES (%s, 1, %s, 'user', %s, %s, CURRENT_TIMESTAMP)",
                       (self._turn_id, step, f"question about order {topic}", 12))
                self._turn_id += 1
                db.sql("INSERT INTO turn (turn_id, session_id, step, role, content, token_count, created_at) "
                       "VALUES (%s, 1, %s, 'assistant', %s, %s, CURRENT_TIMESTAMP)",
                       (self._turn_id, step, f"answer about order {topic} at step {step}", 40))
                self._call_id += 1
                db.sql("INSERT INTO tool_call (call_id, turn_id, tool, args, result, result_size) "
                       "VALUES (%s, %s, 'lookup_order', %s, %s, %s)",
                       (self._call_id, self._turn_id, f'{{"order": {topic}}}', blob, len(blob)))
            for _ in range(reads):
                db.sql("SELECT turn_id, role, content FROM turn ORDER BY turn_id DESC LIMIT 20")
        return script

    def _curation_script(self, name, strategy, step, rng):
        cutoff = max(0, step - 2)   # "older" = before the last two steps

        def script(db):
            if strategy == "summarize":
                db.sql("UPDATE turn SET content = 'summarized', token_count = 2 WHERE step < %s",
                       (cutoff,))
                self._note_id += 1
                db.sql("INSERT INTO note (note_id, kind, candidate, content, created_at) "
                       "VALUES (%s, 'summary', %s, %s, CURRENT_TIMESTAMP)",
                       (self._note_id, name, f"summary of steps before {cutoff}"))
            elif strategy == "reference":
                rows = db.sql("SELECT c.call_id FROM tool_call c JOIN turn t ON t.turn_id = c.turn_id "
                              "WHERE t.step < %s AND c.result_size > 100", (cutoff,))
                for (call_id,) in rows or []:
                    db.sql("UPDATE tool_call SET result = %s, result_size = 8 WHERE call_id = %s",
                           (f"ref:{call_id}", call_id))
            else:  # dedup
                rows = db.sql("SELECT role, content, MIN(turn_id), COUNT(*) FROM turn "
                              "WHERE step < %s GROUP BY role, content HAVING COUNT(*) > 1", (cutoff,))
                for role, content, keep, _cnt in rows or []:
                    db.sql("DELETE FROM turn WHERE role = %s AND content = %s AND turn_id <> %s "
                           "AND step < %s", (role, content, keep, cutoff))
            self._note_id += 1
            db.sql("INSERT INTO note (note_id, kind, candidate, content, created_at) "
                   "VALUES (%s, 'compaction', %s, %s, CURRENT_TIMESTAMP)",
                   (self._note_id, name, f"strategy {strategy}"))
            db.sql(f"CREATE VIEW context_v_{name} AS SELECT turn_id, step, role, content, token_count "
                   f"FROM turn WHERE step >= {cutoff}")
            # One index per strategy, each on its own column set: Dolt
            # cannot merge two indexes covering the same columns, and a
            # strategy chosen earlier may already have put its index on
            # the spine (then the CREATE fails and is ignored).
            ddl = f"idx_turn_{strategy} ON turn {INDEX_COLUMNS[strategy]}"
            try:
                db.sql(f"CREATE INDEX IF NOT EXISTS {ddl}")
            except Exception:
                try:  # MySQL dialects lack IF NOT EXISTS
                    db.sql(f"CREATE INDEX {ddl}")
                except Exception:
                    pass
        return script

    def _policy_check(self, suite, cands) -> int:
        """Cross-branch read of each candidate's context size; returns the
        index of the smallest."""
        refs = [c["branch"] for c in cands]

        def script_for(multi):
            if multi:
                def script(db):
                    out = {}
                    for ref in refs:
                        rows = db.sql(f"SELECT COUNT(*), SUM(token_count) FROM {db.table(ref, 'turn')}")
                        out[ref] = float(rows[0][1] or 0) if rows else 0.0
                    return out
                return script

            def one(db):
                rows = db.sql("SELECT COUNT(*), SUM(token_count) FROM turn")
                return float(rows[0][1] or 0) if rows else 0.0
            return one

        results = self.ctx.cross_branch_exec(suite, script_for, refs, label="policy_check")
        sizes = {}
        if len(results) == 1 and isinstance(results[0].value, dict):
            sizes = results[0].value
        else:
            for ref, res in zip(refs, results):
                sizes[ref] = res.value if (res.ok and res.value is not None) else float("inf")
        best = min(range(len(refs)), key=lambda i: sizes.get(refs[i], float("inf")))
        return best

    def _reject(self, suite, chosen, alternatives, step):
        ctx, inv, p = self.ctx, self.ctx.invariants, self.params
        cycle = p.rejected_cycle
        if cycle not in chosen or cycle not in alternatives:
            inv.not_applicable("S2.1 rejected candidate's sentinel removed after revert",
                               "no rejected cycle configured or fewer than 2 candidates")
            inv.not_applicable("S2.2 alternative's sentinel present after rebase+merge", "same")
            return
        rejected, alt = chosen[cycle], alternatives[cycle]

        def present(candidate):
            res = suite.exec([("SELECT COUNT(*) FROM note WHERE kind = 'compaction' AND candidate = %s",
                               (candidate,))], refs=[ctx.spine], label="invariant")[0]
            return int(res.rows[0][0]) if res.rows else None

        alt_before = present(alt["branch"])
        if rejected["commit"] is None:
            inv.not_applicable("S2.1 rejected candidate's sentinel removed after revert",
                               "backend has no commits to revert")
        else:
            r = suite.revert(ctx.spine, rejected["commit"], label="revert_compaction")
            if r.unsupported:
                inv.not_applicable("S2.1 rejected candidate's sentinel removed after revert",
                                   "revert unsupported")
            else:
                inv.expect("S2.1 rejected candidate's sentinel removed after revert",
                           present(rejected["branch"]), 0, f"revert {r.status_name} {r.error}")
        rb = suite.rebase(ctx.spine, alt["branch"], on_conflict="theirs", label="rebase_onto_alternative")
        if rb.ok and isinstance(rb.value, dict):
            ctx.add_metric("rebase_conflicts", rb.value.get("conflicts", 0))
        m = suite.merge(ctx.spine, alt["branch"], message="fast-forward to alternative",
                        label="fast_forward")
        if rb.unsupported and m.unsupported:
            inv.not_applicable("S2.2 alternative's sentinel present after rebase+merge",
                               "rebase and merge unsupported")
        else:
            inv.check("S2.2 alternative's sentinel present after rebase+merge",
                      alt_before == 0 and present(alt["branch"]) == 1,
                      f"before={alt_before} after={present(alt['branch'])} "
                      f"(rebase {rb.status_name}, merge {m.status_name})")
