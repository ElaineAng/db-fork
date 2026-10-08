"""TPC-C style transactions and CH analytical queries as exec() scripts,
plus the background spine load that S4/S5/S6 run.

Every transaction is a function ``fn(db, rng, scale, **kw)`` that issues
its statements through ``db.sql`` (autocommit, one row per statement) and
keeps the TPC-C consistency conditions true at its end. The spine load
runs them from ``spine_clients`` threads, each with its own DBToolSuite on
the spine branch, until the scenario finishes.

``SpineLoad.pause()`` lets a scenario take a consistent commit of the
spine: it waits for in-flight transactions to finish and holds new ones
back until the block exits, so the pausing thread sees no transaction in
flight. Pausers have priority over clients, so a busy load cannot starve
the scenario.
"""

import random
import time
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta

from macrobench.datagen.ch import CHScale, DISTRICTS_PER_WAREHOUSE

# --------------------------------------------------------------------------
# Transactions
# --------------------------------------------------------------------------


def _now():
    return datetime.now().replace(microsecond=0)


def new_order(db, rng, scale: CHScale, w=None, d=None, c=None, n_items=None):
    w = w or rng.randint(1, scale.warehouses)
    d = d or rng.randint(1, DISTRICTS_PER_WAREHOUSE)
    c = c or rng.randint(1, scale.customers_per_district)
    n_items = n_items or rng.randint(5, 15)
    # Increment first: the row lock serialises concurrent new_orders on the
    # district under read-committed backends, and the read then sees our
    # own increment, so two clients never take the same order id.
    db.sql("UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = %s AND d_id = %s",
           (w, d))
    rows = db.sql("SELECT d_next_o_id, d_tax FROM district WHERE d_w_id = %s AND d_id = %s",
                  (w, d))
    o_id = int(rows[0][0]) - 1
    entry = _now()
    db.sql("INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, "
           "o_ol_cnt, o_all_local) VALUES (%s, %s, %s, %s, %s, NULL, %s, 1)",
           (o_id, d, w, c, entry, n_items))
    db.sql("INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (%s, %s, %s)",
           (o_id, d, w))
    total = 0.0
    for n in range(1, n_items + 1):
        i_id = rng.randint(1, scale.items)
        qty = rng.randint(1, 10)
        price = db.sql("SELECT i_price FROM item WHERE i_id = %s", (i_id,))
        price = float(price[0][0]) if price else 1.0
        db.sql("UPDATE stock SET s_quantity = CASE WHEN s_quantity - %s >= 10 "
               "THEN s_quantity - %s ELSE s_quantity - %s + 91 END, "
               "s_ytd = s_ytd + %s, s_order_cnt = s_order_cnt + 1 "
               "WHERE s_w_id = %s AND s_i_id = %s", (qty, qty, qty, qty, w, i_id))
        amount = round(qty * price, 2)
        total += amount
        db.sql("INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, "
               "ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) "
               "VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, %s, %s)",
               (o_id, d, w, n, i_id, w, qty, amount, "x" * 24))
    return {"o_id": o_id, "w": w, "d": d, "total": round(total, 2)}


def payment(db, rng, scale: CHScale, w=None, d=None, c=None, amount=None,
            touch_c_data=False):
    w = w or rng.randint(1, scale.warehouses)
    d = d or rng.randint(1, DISTRICTS_PER_WAREHOUSE)
    c = c or rng.randint(1, scale.customers_per_district)
    amount = amount or round(rng.uniform(1, 5000), 2)
    db.sql("UPDATE warehouse SET w_ytd = w_ytd + %s WHERE w_id = %s", (amount, w))
    db.sql("UPDATE district SET d_ytd = d_ytd + %s WHERE d_w_id = %s AND d_id = %s",
           (amount, w, d))
    if touch_c_data:
        db.sql("UPDATE customer SET c_balance = c_balance - %s, "
               "c_ytd_payment = c_ytd_payment + %s, c_payment_cnt = c_payment_cnt + 1, "
               "c_data = %s WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s",
               (amount, amount, f"payment {amount} at {_now()}", w, d, c))
    else:
        db.sql("UPDATE customer SET c_balance = c_balance - %s, "
               "c_ytd_payment = c_ytd_payment + %s, c_payment_cnt = c_payment_cnt + 1 "
               "WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s", (amount, amount, w, d, c))
    db.sql("INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, "
           "h_amount, h_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
           (c, d, w, d, w, _now(), amount, "payment"))
    return {"w": w, "d": d, "c": c, "amount": amount}


def delivery(db, rng, scale: CHScale, w=None, carrier=None):
    w = w or rng.randint(1, scale.warehouses)
    carrier = carrier or rng.randint(1, 10)
    delivered = 0
    for d in range(1, DISTRICTS_PER_WAREHOUSE + 1):
        rows = db.sql("SELECT MIN(no_o_id) FROM new_order WHERE no_w_id = %s AND no_d_id = %s",
                      (w, d))
        o_id = rows[0][0] if rows else None
        if o_id is None:
            continue
        o_id = int(o_id)
        db.sql("DELETE FROM new_order WHERE no_w_id = %s AND no_d_id = %s AND no_o_id = %s",
               (w, d, o_id))
        db.sql("UPDATE orders SET o_carrier_id = %s WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s",
               (carrier, w, d, o_id))
        db.sql("UPDATE order_line SET ol_delivery_d = %s "
               "WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s", (_now(), w, d, o_id))
        rows = db.sql("SELECT o_c_id FROM orders WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s",
                      (w, d, o_id))
        c = rows[0][0]
        rows = db.sql("SELECT SUM(ol_amount) FROM order_line "
                      "WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s", (w, d, o_id))
        total = rows[0][0] or 0
        db.sql("UPDATE customer SET c_balance = c_balance + %s, c_delivery_cnt = c_delivery_cnt + 1 "
               "WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s", (total, w, d, c))
        delivered += 1
    return {"w": w, "delivered": delivered}


def order_status(db, rng, scale: CHScale, w=None, d=None, c=None):
    w = w or rng.randint(1, scale.warehouses)
    d = d or rng.randint(1, DISTRICTS_PER_WAREHOUSE)
    c = c or rng.randint(1, scale.customers_per_district)
    db.sql("SELECT c_first, c_middle, c_last, c_balance FROM customer "
           "WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s", (w, d, c))
    rows = db.sql("SELECT MAX(o_id) FROM orders WHERE o_w_id = %s AND o_d_id = %s AND o_c_id = %s",
                  (w, d, c))
    o_id = rows[0][0] if rows else None
    if o_id is not None:
        db.sql("SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d "
               "FROM order_line WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s",
               (w, d, o_id))
    return {"w": w, "d": d, "c": c, "o_id": o_id}


def stock_level(db, rng, scale: CHScale, w=None, d=None, threshold=None):
    w = w or rng.randint(1, scale.warehouses)
    d = d or rng.randint(1, DISTRICTS_PER_WAREHOUSE)
    threshold = threshold or rng.randint(10, 20)
    rows = db.sql("SELECT d_next_o_id FROM district WHERE d_w_id = %s AND d_id = %s", (w, d))
    nxt = int(rows[0][0])
    # Written as a semi-join: Doltgres plans the equivalent
    # order_line JOIN stock as a scan of stock (90s+ at W=1), while the IN
    # form takes milliseconds.
    rows = db.sql("SELECT COUNT(*) FROM stock s WHERE s.s_w_id = %s AND s.s_quantity < %s "
                  "AND s.s_i_id IN (SELECT ol_i_id FROM order_line "
                  "WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id >= %s AND ol_o_id < %s)",
                  (w, threshold, w, d, nxt - 20, nxt))
    return {"w": w, "d": d, "low_stock": int(rows[0][0]) if rows else 0}


TRANSACTIONS = {
    "new_order": new_order,
    "payment": payment,
    "delivery": delivery,
    "order_status": order_status,
    "stock_level": stock_level,
}
# TPC-C mix
TXN_MIX = [("new_order", 0.45), ("payment", 0.43), ("delivery", 0.04),
           ("order_status", 0.04), ("stock_level", 0.04)]


# --------------------------------------------------------------------------
# CH analytical queries (a dialect-neutral subset)
# --------------------------------------------------------------------------


def ch_q1(db, since):
    return db.sql(
        "SELECT ol_number, SUM(ol_quantity), SUM(ol_amount), AVG(ol_quantity), "
        "AVG(ol_amount), COUNT(*) FROM order_line WHERE ol_delivery_d > %s "
        "GROUP BY ol_number ORDER BY ol_number", (since,))


def ch_q3(db, since):
    return db.sql(
        "SELECT ol.ol_o_id, ol.ol_w_id, ol.ol_d_id, SUM(ol.ol_amount) AS revenue, o.o_entry_d "
        "FROM customer c JOIN new_order no ON no.no_w_id = c.c_w_id AND no.no_d_id = c.c_d_id "
        "JOIN orders o ON o.o_w_id = no.no_w_id AND o.o_d_id = no.no_d_id AND o.o_id = no.no_o_id "
        "AND o.o_c_id = c.c_id "
        "JOIN order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id "
        "WHERE c.c_state >= 'A' AND o.o_entry_d > %s "
        "GROUP BY ol.ol_o_id, ol.ol_w_id, ol.ol_d_id, o.o_entry_d "
        "ORDER BY revenue DESC, o.o_entry_d LIMIT 100", (since,))


def ch_q6(db, since, until):
    return db.sql(
        "SELECT SUM(ol_amount) FROM order_line WHERE ol_delivery_d >= %s "
        "AND ol_delivery_d < %s AND ol_quantity BETWEEN 1 AND 100000", (since, until))


def ch_q12(db, since):
    return db.sql(
        "SELECT o.o_ol_cnt, "
        "SUM(CASE WHEN o.o_carrier_id = 1 OR o.o_carrier_id = 2 THEN 1 ELSE 0 END) AS high_line, "
        "SUM(CASE WHEN o.o_carrier_id <> 1 AND o.o_carrier_id <> 2 THEN 1 ELSE 0 END) AS low_line "
        "FROM orders o JOIN order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id "
        "AND ol.ol_o_id = o.o_id WHERE o.o_entry_d <= ol.ol_delivery_d AND ol.ol_delivery_d > %s "
        "GROUP BY o.o_ol_cnt ORDER BY o.o_ol_cnt", (since,))


def ch_q18(db, since):
    return db.sql(
        "SELECT c.c_last, c.c_id, o.o_id, o.o_entry_d, o.o_ol_cnt, SUM(ol.ol_amount) AS total "
        "FROM customer c JOIN orders o ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id "
        "JOIN order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id "
        "WHERE o.o_entry_d > %s "
        "GROUP BY c.c_last, c.c_id, o.o_id, o.o_entry_d, o.o_ol_cnt "
        "HAVING SUM(ol.ol_amount) > 200 ORDER BY total DESC, o.o_entry_d LIMIT 100", (since,))


def analytical(db, rng, scale: CHScale, days=7):
    """One CH query chosen at random over the last ``days`` days."""
    since = _now() - timedelta(days=days)
    which = rng.choice(["q1", "q3", "q6", "q12", "q18"])
    if which == "q1":
        rows = ch_q1(db, since)
    elif which == "q3":
        rows = ch_q3(db, since)
    elif which == "q6":
        rows = ch_q6(db, since, _now())
    elif which == "q12":
        rows = ch_q12(db, since)
    else:
        rows = ch_q18(db, since)
    return {"query": which, "rows": len(rows or [])}


def pick_transaction(rng, analytical_fraction: float = 0.0):
    """Name of the next spine transaction."""
    if analytical_fraction > 0 and rng.random() < analytical_fraction:
        return "analytical"
    x = rng.random()
    acc = 0.0
    for name, share in TXN_MIX:
        acc += share
        if x < acc:
            return name
    return TXN_MIX[-1][0]


TXN_RETRIES = 5


def run_transaction(name, db, rng, scale: CHScale, retries: int = TXN_RETRIES, **kw):
    """Run one TPC-C transaction atomically (``db.transaction()``) and retry
    it when the backend rejects it, e.g. a serialization failure or a
    duplicate order id raced by another spine client. A failed attempt
    rolls back, so concurrent clients never leave the district counters
    half-updated and the TPC-C consistency conditions keep holding."""
    if name == "analytical":
        return analytical(db, rng, scale)
    fn = TRANSACTIONS[name]
    last = None
    for attempt in range(max(1, retries)):
        try:
            with db.transaction():
                return fn(db, rng, scale, **kw)
        except Exception as e:  # recorded as FAILED statement rows already
            last = e
            # Jittered backoff: W=1 makes the warehouse row a hot spot, so
            # commits of concurrent payments often collide.
            time.sleep(random.uniform(0.0, 0.02) * (attempt + 1))
    raise last


# --------------------------------------------------------------------------
# Background spine load
# --------------------------------------------------------------------------


class Quiescer:
    """Pause/resume gate between load clients and a pausing thread.

    Clients wrap each transaction in ``with q.transaction():``; a pauser
    uses ``with q.pause():`` and gets the block only once every in-flight
    transaction has finished. New transactions wait while a pause is
    requested, so pausers are never starved. pause() may be nested on the
    same thread.
    """

    def __init__(self):
        self._cond = threading.Condition()
        self._active = 0
        self._pausers = 0
        self._local = threading.local()

    @contextmanager
    def transaction(self):
        with self._cond:
            while self._pausers > 0:
                self._cond.wait()
            self._active += 1
        try:
            yield
        finally:
            with self._cond:
                self._active -= 1
                self._cond.notify_all()

    @contextmanager
    def pause(self):
        depth = getattr(self._local, "depth", 0)
        if depth == 0:
            with self._cond:
                self._pausers += 1
                while self._active > 0:
                    self._cond.wait()
        self._local.depth = depth + 1
        try:
            yield
        finally:
            self._local.depth = depth
            if depth == 0:
                with self._cond:
                    self._pausers -= 1
                    self._cond.notify_all()


class SpineLoad:
    """``clients`` threads running the TPC-C/CH mix on ``spine_ref``.

    ``suite_factory()`` returns a fresh DBToolSuite per thread. Each
    transaction is one exec() labelled "spine" and runs inside the
    Quiescer, so ``with load.pause():`` yields a moment with no
    transaction in flight.
    """

    def __init__(self, suite_factory, spine_ref: str, scale: CHScale,
                 clients: int, analytical_fraction: float = 0.0,
                 txn_limit: int = 0, seed: int = 42, touch_c_data: bool = False,
                 on_error=None):
        self.suite_factory = suite_factory
        self.spine_ref = spine_ref
        self.scale = scale
        self.clients = max(0, clients)
        self.analytical_fraction = analytical_fraction
        self.txn_limit = txn_limit
        self.seed = seed
        self.touch_c_data = touch_c_data
        self.on_error = on_error
        self.quiescer = Quiescer()
        self._stop = threading.Event()
        self._threads = []
        self.counts = {}
        self.failures = 0
        self._lock = threading.Lock()

    def start(self):
        for i in range(self.clients):
            t = threading.Thread(target=self._run, args=(i,), daemon=True,
                                 name=f"spine-{i}")
            t.start()
            self._threads.append(t)
        return self

    def stop(self, timeout: float = 60.0):
        self._stop.set()
        for t in self._threads:
            t.join(timeout)
        return self

    @property
    def running(self) -> bool:
        return any(t.is_alive() for t in self._threads)

    def pause(self):
        """Context manager: no spine transaction runs inside the block."""
        return self.quiescer.pause()

    def _run(self, index: int):
        import dblib.result_collector as rc
        rc.set_current_thread_id(1000 + index)
        rng = random.Random(self.seed * 1000 + index)
        suite = self.suite_factory()
        done = 0
        try:
            while not self._stop.is_set():
                if self.txn_limit and done >= self.txn_limit:
                    break
                name = pick_transaction(rng, self.analytical_fraction)
                kw = {"touch_c_data": True} if (name == "payment" and self.touch_c_data) else {}
                with self.quiescer.transaction():
                    res = suite.exec(
                        lambda db: run_transaction(name, db, rng, self.scale, **kw),
                        refs=[self.spine_ref], label="spine",
                    )[0]
                done += 1
                with self._lock:
                    self.counts[name] = self.counts.get(name, 0) + 1
                    if not res.ok:
                        self.failures += 1
                        if self.on_error and self.failures <= 3:
                            self.on_error(f"spine {name}: {res.error}")
        finally:
            suite.close_connection()
