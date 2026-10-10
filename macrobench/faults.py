"""Fault catalog and TPC-C consistency conditions, shared by S1 (RL
environment) and S5 (operations agent).

A fault is a scripted write that violates one TPC-C consistency condition
in one district; its repair is the inverse write. Both run through a
Session (``db.sql``) so they are recorded like any other data op. The
conditions are evaluated as "violation count" queries that return 0 when
the condition holds; ``qualify`` lets the caller rewrite table names for a
cross-branch (multi-ref) read.
"""

from dataclasses import dataclass
from typing import Callable

Qualifier = Callable[[str], str]


def _ident(table: str) -> str:
    return table


# --------------------------------------------------------------------------
# Consistency conditions (TPC-C clause 3.3.2), as violation counts
# --------------------------------------------------------------------------


def dec(expr: str) -> str:
    """Money comparisons go through DECIMAL(14,2) on both sides: Doltgres
    hands SUM(decimal) back as a float and its decimal-float comparison is
    wrong, while Postgres and MySQL accept the cast harmlessly."""
    return f"CAST({expr} AS DECIMAL(14,2))"


def consistency_queries(q: Qualifier = _ident) -> dict:
    W, D, O, NO, OL, H = (q("warehouse"), q("district"), q("orders"),
                          q("new_order"), q("order_line"), q("history"))
    DEC = "CAST_DEC"
    out = {
        # C1: W_YTD = sum(D_YTD). Both sides are cast because Doltgres
        # returns SUM(decimal) as a float and compares it wrongly.
        "C1": f"""SELECT COUNT(*) FROM {W} w WHERE {DEC}(w.w_ytd) <>
                  (SELECT {DEC}(SUM(d.d_ytd)) FROM {D} d WHERE d.d_w_id = w.w_id)""",
        # C2: D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
        "C2": f"""SELECT COUNT(*) FROM {D} d WHERE d.d_next_o_id - 1 <>
                  (SELECT MAX(o.o_id) FROM {O} o
                   WHERE o.o_w_id = d.d_w_id AND o.o_d_id = d.d_id)
                  OR d.d_next_o_id - 1 <>
                  (SELECT MAX(n.no_o_id) FROM {NO} n
                   WHERE n.no_w_id = d.d_w_id AND n.no_d_id = d.d_id)""",
        # C3: max(NO_O_ID) - min(NO_O_ID) + 1 = count(new_order) per district
        "C3": f"""SELECT COUNT(*) FROM (
                    SELECT no_w_id, no_d_id,
                           MAX(no_o_id) - MIN(no_o_id) + 1 AS span, COUNT(*) AS cnt
                    FROM {NO} GROUP BY no_w_id, no_d_id) x
                  WHERE x.span <> x.cnt""",
        # C4: sum(O_OL_CNT) = count(order_line) per district
        "C4": f"""SELECT COUNT(*) FROM
                    (SELECT o_w_id, o_d_id, SUM(o_ol_cnt) AS s FROM {O}
                     GROUP BY o_w_id, o_d_id) o
                  JOIN
                    (SELECT ol_w_id, ol_d_id, COUNT(*) AS c FROM {OL}
                     GROUP BY ol_w_id, ol_d_id) ol
                  ON o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id
                  WHERE o.s <> ol.c""",
        # C5: O_CARRIER_ID is NULL iff the order is in new_order
        "C5": f"""SELECT COUNT(*) FROM {O} o LEFT JOIN {NO} n
                  ON n.no_w_id = o.o_w_id AND n.no_d_id = o.o_d_id AND n.no_o_id = o.o_id
                  WHERE (o.o_carrier_id IS NULL AND n.no_o_id IS NULL)
                     OR (o.o_carrier_id IS NOT NULL AND n.no_o_id IS NOT NULL)""",
        # C7: OL_DELIVERY_D is NULL iff O_CARRIER_ID is NULL
        "C7": f"""SELECT COUNT(*) FROM {OL} ol JOIN {O} o
                  ON o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id AND o.o_id = ol.ol_o_id
                  WHERE (ol.ol_delivery_d IS NULL AND o.o_carrier_id IS NOT NULL)
                     OR (ol.ol_delivery_d IS NOT NULL AND o.o_carrier_id IS NULL)""",
        # C8: W_YTD = sum(H_AMOUNT) per warehouse
        "C8": f"""SELECT COUNT(*) FROM {W} w WHERE {DEC}(w.w_ytd) <>
                  (SELECT {DEC}(SUM(h.h_amount)) FROM {H} h WHERE h.h_w_id = w.w_id)""",
        # C9: D_YTD = sum(H_AMOUNT) per district
        "C9": f"""SELECT COUNT(*) FROM {D} d WHERE {DEC}(d.d_ytd) <>
                  (SELECT {DEC}(SUM(h.h_amount)) FROM {H} h
                   WHERE h.h_w_id = d.d_w_id AND h.h_d_id = d.d_id)""",
    }
    return {k: _expand_dec(v) for k, v in out.items()}


def _expand_dec(sql: str) -> str:
    """Replace CAST_DEC(expr) with CAST(expr AS DECIMAL(14,2))."""
    out = ""
    i = 0
    while True:
        j = sql.find("CAST_DEC(", i)
        if j < 0:
            return out + sql[i:]
        out += sql[i:j]
        k = j + len("CAST_DEC(")
        depth = 1
        while depth:
            if sql[k] == "(":
                depth += 1
            elif sql[k] == ")":
                depth -= 1
            k += 1
        out += dec(sql[j + len("CAST_DEC("):k - 1])
        i = k


CONDITIONS = tuple(consistency_queries().keys())


def check_consistency(db, q: Qualifier = _ident, conditions=CONDITIONS) -> dict:
    """{condition: violation_count} on the session's branch (or on the
    qualified tables). A query that fails counts as -1."""
    out = {}
    for name, sql in consistency_queries(q).items():
        if name not in conditions:
            continue
        try:
            rows = db.sql(sql)
            out[name] = int(rows[0][0]) if rows else 0
        except Exception:  # recorded as a FAILED statement row already
            out[name] = -1
    return out


def all_hold(violations: dict) -> bool:
    return bool(violations) and all(v == 0 for v in violations.values())


# --------------------------------------------------------------------------
# Fault catalog
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Fault:
    fault_id: int
    name: str
    conditions: tuple  # the conditions it violates
    inject: Callable   # inject(db, w, d) -> dict (state the repair needs)
    repair: Callable   # repair(db, w, d, state) -> None


def _district_ytd_inject(db, w, d):
    db.sql("UPDATE district SET d_ytd = d_ytd + 100 WHERE d_w_id = %s AND d_id = %s", (w, d))
    return {}


def _district_ytd_repair(db, w, d, state):
    db.sql("UPDATE district SET d_ytd = d_ytd - 100 WHERE d_w_id = %s AND d_id = %s", (w, d))


def _lost_new_order_inject(db, w, d):
    rows = db.sql(
        "SELECT MIN(no_o_id) FROM new_order WHERE no_w_id = %s AND no_d_id = %s", (w, d)
    )
    lo = rows[0][0] if rows and rows[0][0] is not None else None
    if lo is None:
        return {"o_id": None}
    o_id = lo + 1
    db.sql("DELETE FROM new_order WHERE no_w_id = %s AND no_d_id = %s AND no_o_id = %s",
           (w, d, o_id))
    return {"o_id": o_id}


def _lost_new_order_repair(db, w, d, state):
    if state.get("o_id") is None:
        return
    # Idempotent: a rollout forked from a step after the repair would
    # otherwise re-insert the row and fail on the primary key.
    present = db.sql("SELECT 1 FROM new_order WHERE no_w_id = %s AND no_d_id = %s AND no_o_id = %s",
                     (w, d, state["o_id"]))
    if not present:
        db.sql("INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (%s, %s, %s)",
               (state["o_id"], d, w))


def _ol_cnt_inject(db, w, d):
    rows = db.sql("SELECT MIN(o_id) FROM orders WHERE o_w_id = %s AND o_d_id = %s", (w, d))
    o_id = rows[0][0] if rows else None
    if o_id is None:
        return {"o_id": None}
    db.sql("UPDATE orders SET o_ol_cnt = o_ol_cnt + 1 "
           "WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s", (w, d, o_id))
    return {"o_id": o_id}


def _ol_cnt_repair(db, w, d, state):
    if state.get("o_id") is not None:
        db.sql("UPDATE orders SET o_ol_cnt = o_ol_cnt - 1 "
               "WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s", (w, d, state["o_id"]))


def _carrier_inject(db, w, d):
    rows = db.sql("SELECT MIN(o_id), MIN(o_carrier_id) FROM orders "
                  "WHERE o_w_id = %s AND o_d_id = %s AND o_carrier_id IS NOT NULL", (w, d))
    o_id = rows[0][0] if rows else None
    if o_id is None:
        return {"o_id": None}
    carrier = db.sql("SELECT o_carrier_id FROM orders WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s",
                     (w, d, o_id))[0][0]
    db.sql("UPDATE orders SET o_carrier_id = NULL WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s",
           (w, d, o_id))
    return {"o_id": o_id, "carrier": carrier}


def _carrier_repair(db, w, d, state):
    if state.get("o_id") is not None:
        db.sql("UPDATE orders SET o_carrier_id = %s WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s",
               (state["carrier"], w, d, state["o_id"]))


def _history_inject(db, w, d):
    db.sql("UPDATE history SET h_amount = h_amount * 2 "
           "WHERE h_w_id = %s AND h_d_id = %s AND h_c_id = 1", (w, d))
    return {}


def _history_repair(db, w, d, state):
    db.sql("UPDATE history SET h_amount = h_amount / 2 "
           "WHERE h_w_id = %s AND h_d_id = %s AND h_c_id = 1", (w, d))


FAULTS = {
    1: Fault(1, "district_ytd_drift", ("C1", "C9"), _district_ytd_inject, _district_ytd_repair),
    2: Fault(2, "lost_new_order", ("C3", "C5"), _lost_new_order_inject, _lost_new_order_repair),
    3: Fault(3, "order_line_count_drift", ("C4",), _ol_cnt_inject, _ol_cnt_repair),
    4: Fault(4, "carrier_cleared", ("C5", "C7"), _carrier_inject, _carrier_repair),
    5: Fault(5, "history_amount_doubled", ("C8", "C9"), _history_inject, _history_repair),
}


def pick_fault(rng, w_max: int) -> tuple:
    """(Fault, w_id, d_id) chosen at random."""
    fault = FAULTS[rng.randint(1, len(FAULTS))]
    return fault, rng.randint(1, w_max), rng.randint(1, 10)
