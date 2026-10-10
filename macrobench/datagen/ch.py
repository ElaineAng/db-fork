"""CH-benCHmark seed data at a scale factor, written through exec().

The generator follows the TPC-C population rules closely enough that the
consistency conditions the scenarios check (C1-C5, C7-C9 in
macrobench/faults.py) hold on the seeded database:

* warehouse.w_ytd = sum(district.d_ytd) = sum(history.h_amount);
* district.d_next_o_id - 1 = max(orders.o_id) = max(new_order.no_o_id);
* the last 30% of each district's orders are undelivered and in new_order;
* sum(orders.o_ol_cnt) = count(order_line) per district;
* order_line.ol_delivery_d is NULL exactly for undelivered orders.

Rows go to the database as multi-row INSERT statements with parameters, so
one generator serves every backend dialect.
"""

import random
import string
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Iterator

from macrobench import task_pb2 as tp

DISTRICTS_PER_WAREHOUSE = 10
DELIVERED_FRACTION = 0.7
ORDER_DAYS_SPAN = 30  # orders are dated over the last 30 days

NATIONS = [
    "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE",
    "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
    "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA",
    "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES",
]
REGIONS = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
LAST_NAME_SYLLABLES = [
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
]


@dataclass
class CHScale:
    warehouses: int = 1
    items: int = 100_000
    customers_per_district: int = 3_000
    orders_per_district: int = 3_000
    suppliers: int = 10_000

    @classmethod
    def from_config(cls, schema: tp.SchemaConfig) -> "CHScale":
        return cls(
            warehouses=max(1, schema.scale_factor),
            items=schema.items or 100_000,
            customers_per_district=schema.customers_per_district or 3_000,
            orders_per_district=schema.orders_per_district or 3_000,
            suppliers=schema.suppliers or 10_000,
        )

    @property
    def districts(self) -> int:
        return self.warehouses * DISTRICTS_PER_WAREHOUSE

    def expected_counts(self) -> dict:
        d = self.districts
        return {
            "region": len(REGIONS),
            "nation": len(NATIONS),
            "supplier": self.suppliers,
            "warehouse": self.warehouses,
            "district": d,
            "item": self.items,
            "customer": d * self.customers_per_district,
            "history": d * self.customers_per_district,
            "stock": self.warehouses * self.items,
            "orders": d * self.orders_per_district,
            "new_order": d * (self.orders_per_district - self.delivered_orders),
        }

    @property
    def delivered_orders(self) -> int:
        return int(self.orders_per_district * DELIVERED_FRACTION)


def customer_last_name(num: int) -> str:
    return (LAST_NAME_SYLLABLES[(num // 100) % 10]
            + LAST_NAME_SYLLABLES[(num // 10) % 10]
            + LAST_NAME_SYLLABLES[num % 10])


class CHGenerator:
    """Yields (table, columns, rows) in dependency order."""

    def __init__(self, scale: CHScale, seed: int = 42, now: datetime = None):
        self.scale = scale
        self.rng = random.Random(seed)
        self.now = (now or datetime.now()).replace(microsecond=0)

    # -- helpers ----------------------------------------------------------

    def _text(self, lo: int, hi: int) -> str:
        n = self.rng.randint(lo, hi)
        return "".join(self.rng.choices(string.ascii_lowercase + " ", k=n)).strip() or "x"

    def _zip(self) -> str:
        return f"{self.rng.randint(0, 9999):04d}11111"

    def _order_date(self, o_id: int, per_district: int) -> datetime:
        # Older orders first, spread over ORDER_DAYS_SPAN days.
        frac = (o_id - 1) / max(1, per_district)
        offset = timedelta(days=ORDER_DAYS_SPAN * (1 - frac))
        return (self.now - offset).replace(microsecond=0)

    # -- tables -----------------------------------------------------------

    def region(self):
        cols = ["r_regionkey", "r_name", "r_comment"]
        return cols, [(k, name, self._text(10, 40)) for k, name in enumerate(REGIONS)]

    def nation(self):
        cols = ["n_nationkey", "n_name", "n_regionkey", "n_comment"]
        return cols, [(k, name, k % len(REGIONS), self._text(10, 40))
                      for k, name in enumerate(NATIONS)]

    def supplier(self):
        cols = ["su_suppkey", "su_name", "su_address", "su_nationkey",
                "su_phone", "su_acctbal", "su_comment"]

        def rows():
            for k in range(self.scale.suppliers):
                yield (k, f"Supplier#{k:09d}", self._text(10, 30),
                       k % len(NATIONS), f"{self.rng.randint(10, 34)}-{self.rng.randint(100, 999)}-{self.rng.randint(100, 999)}-{self.rng.randint(1000, 9999)}",
                       round(self.rng.uniform(-999.99, 9999.99), 2), self._text(10, 60))
        return cols, rows()

    def warehouse(self):
        cols = ["w_id", "w_name", "w_street_1", "w_street_2", "w_city",
                "w_state", "w_zip", "w_tax", "w_ytd"]
        d_ytd = 10.0 * self.scale.customers_per_district
        return cols, [
            (w, self._text(6, 10), self._text(10, 20), self._text(10, 20),
             self._text(10, 20), "".join(self.rng.choices(string.ascii_uppercase, k=2)),
             self._zip(), round(self.rng.uniform(0, 0.2), 4),
             round(DISTRICTS_PER_WAREHOUSE * d_ytd, 2))
            for w in range(1, self.scale.warehouses + 1)
        ]

    def district(self):
        cols = ["d_id", "d_w_id", "d_name", "d_street_1", "d_street_2", "d_city",
                "d_state", "d_zip", "d_tax", "d_ytd", "d_next_o_id"]
        d_ytd = round(10.0 * self.scale.customers_per_district, 2)
        return cols, [
            (d, w, self._text(6, 10), self._text(10, 20), self._text(10, 20),
             self._text(10, 20), "".join(self.rng.choices(string.ascii_uppercase, k=2)),
             self._zip(), round(self.rng.uniform(0, 0.2), 4), d_ytd,
             self.scale.orders_per_district + 1)
            for w in range(1, self.scale.warehouses + 1)
            for d in range(1, DISTRICTS_PER_WAREHOUSE + 1)
        ]

    def item(self):
        cols = ["i_id", "i_im_id", "i_name", "i_price", "i_data"]

        def rows():
            for i in range(1, self.scale.items + 1):
                data = self._text(26, 50)
                if self.rng.random() < 0.1:
                    data = data[:10] + "ORIGINAL" + data[18:]
                yield (i, self.rng.randint(1, 10000), self._text(14, 24),
                       round(self.rng.uniform(1, 100), 2), data)
        return cols, rows()

    def customer(self):
        cols = ["c_id", "c_d_id", "c_w_id", "c_first", "c_middle", "c_last",
                "c_street_1", "c_street_2", "c_city", "c_state", "c_zip",
                "c_phone", "c_since", "c_credit", "c_credit_lim", "c_discount",
                "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt",
                "c_data", "c_n_nationkey"]

        def rows():
            for w in range(1, self.scale.warehouses + 1):
                for d in range(1, DISTRICTS_PER_WAREHOUSE + 1):
                    for c in range(1, self.scale.customers_per_district + 1):
                        last = (customer_last_name(c - 1) if c <= 1000
                                else customer_last_name(self.rng.randint(0, 999)))
                        yield (c, d, w, self._text(8, 16), "OE", last,
                               self._text(10, 20), self._text(10, 20), self._text(10, 20),
                               "".join(self.rng.choices(string.ascii_uppercase, k=2)),
                               self._zip(), "".join(self.rng.choices(string.digits, k=16)),
                               self.now, "BC" if self.rng.random() < 0.1 else "GC",
                               50000.0, round(self.rng.uniform(0, 0.5), 4), -10.0, 10.0,
                               1, 0, self._text(60, 120), self.rng.randint(0, len(NATIONS) - 1))
        return cols, rows()

    def history(self):
        cols = ["h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date",
                "h_amount", "h_data"]

        def rows():
            for w in range(1, self.scale.warehouses + 1):
                for d in range(1, DISTRICTS_PER_WAREHOUSE + 1):
                    for c in range(1, self.scale.customers_per_district + 1):
                        yield (c, d, w, d, w, self.now, 10.0, self._text(12, 24))
        return cols, rows()

    def stock(self):
        cols = ["s_i_id", "s_w_id", "s_quantity"] + [f"s_dist_{k:02d}" for k in range(1, 11)] + [
            "s_ytd", "s_order_cnt", "s_remote_cnt", "s_data", "s_su_suppkey"]

        def rows():
            for w in range(1, self.scale.warehouses + 1):
                for i in range(1, self.scale.items + 1):
                    dist = ["".join(self.rng.choices(string.ascii_lowercase, k=24)) for _ in range(10)]
                    yield tuple([i, w, self.rng.randint(10, 100)] + dist + [
                        0, 0, 0, self._text(26, 50), (w * i) % self.scale.suppliers])
        return cols, rows()

    def orders(self):
        cols = ["o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_carrier_id",
                "o_ol_cnt", "o_all_local"]
        per = self.scale.orders_per_district
        delivered = self.scale.delivered_orders

        def rows():
            for w in range(1, self.scale.warehouses + 1):
                for d in range(1, DISTRICTS_PER_WAREHOUSE + 1):
                    # Permute customers so each gets at most ~one open order.
                    customers = list(range(1, self.scale.customers_per_district + 1))
                    self.rng.shuffle(customers)
                    for o in range(1, per + 1):
                        c = customers[(o - 1) % len(customers)]
                        carrier = self.rng.randint(1, 10) if o <= delivered else None
                        yield (o, d, w, c, self._order_date(o, per), carrier,
                               self.rng.randint(5, 15), 1)
        return cols, rows()

    def new_order(self):
        cols = ["no_o_id", "no_d_id", "no_w_id"]
        per = self.scale.orders_per_district
        delivered = self.scale.delivered_orders
        return cols, [
            (o, d, w)
            for w in range(1, self.scale.warehouses + 1)
            for d in range(1, DISTRICTS_PER_WAREHOUSE + 1)
            for o in range(delivered + 1, per + 1)
        ]

    def order_line(self, order_rows):
        cols = ["ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id",
                "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount",
                "ol_dist_info"]

        def rows():
            for (o, d, w, _c, entry_d, carrier, ol_cnt, _local) in order_rows:
                for n in range(1, ol_cnt + 1):
                    yield (o, d, w, n, self.rng.randint(1, self.scale.items), w,
                           entry_d if carrier is not None else None, 5,
                           round(self.rng.uniform(0.01, 9999.99), 2),
                           "".join(self.rng.choices(string.ascii_lowercase, k=24)))
        return cols, rows()

    def tables(self) -> Iterator[tuple]:
        """(table, columns, rows) in dependency order."""
        yield ("region",) + self.region()
        yield ("nation",) + self.nation()
        yield ("supplier",) + self.supplier()
        yield ("warehouse",) + self.warehouse()
        yield ("district",) + self.district()
        yield ("item",) + self.item()
        yield ("customer",) + self.customer()
        yield ("history",) + self.history()
        yield ("stock",) + self.stock()
        _, order_rows = self.orders()
        order_rows = list(order_rows)
        yield ("orders", self.orders()[0], iter(order_rows))
        yield ("new_order",) + self.new_order()
        yield ("order_line",) + self.order_line(order_rows)


def insert_rows(db, table: str, columns: list, rows, batch_rows: int = 500) -> int:
    """Insert ``rows`` into ``table`` through a Session in multi-row
    INSERTs with parameters. Returns the number of rows inserted."""
    placeholders = "(" + ", ".join(["%s"] * len(columns)) + ")"
    head = f"INSERT INTO {table} ({', '.join(columns)}) VALUES "
    total = 0
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_rows:
            total += _flush(db, head, placeholders, batch)
            batch = []
    if batch:
        total += _flush(db, head, placeholders, batch)
    return total


def _flush(db, head, placeholders, batch) -> int:
    sql = head + ", ".join([placeholders] * len(batch))
    params = tuple(v for row in batch for v in row)
    db.sql(sql, params)
    return len(batch)


def seed_ch(suite, ref, scale: CHScale, seed: int = 42, batch_rows: int = 500,
            log: Callable = print, tables: list = None) -> dict:
    """Seed the CH tables on ``ref`` (untimed). Returns {table: rows}."""
    gen = CHGenerator(scale, seed)
    counts = {}

    def script(db):
        for table, cols, rows in gen.tables():
            if tables is not None and table not in tables:
                continue
            n = insert_rows(db, table, cols, rows, batch_rows)
            counts[table] = n
            log(f"  seeded {table:>12}: {n:>9,} rows")

    res = suite.exec(script, refs=[ref], timed=False)[0]
    res.raise_for_status()
    return counts
