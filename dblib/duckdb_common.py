"""SQL shared by the DuckDB-based backends: Bauplan (its local working copy)
and MotherDuck.

The three-way merge, revert and diff here take SQL relations (a table name
or a subquery) and run in one DuckDB connection, so they work the same on a
local DuckDB holding Arrow data and on MotherDuck, where the relations are
tables in other databases. Their working tables go wherever a Scratch says:
local TEMP tables, or a schema inside a MotherDuck database so the work runs
server-side.
"""

import re
import uuid

from util.sql_parse import get_sql_operation_keyword

# Conflict rows handed to a resolve callable, per table.
CONFLICT_ROW_LIMIT = 1000

_PLACEHOLDER_RE = re.compile(r"%\((\w+)\)s|%s|%%")
_DML = ("INSERT", "UPDATE", "DELETE", "MERGE")


def to_duckdb(sql: str, vars=None):
    """pyformat SQL (%s, %(name)s) -> DuckDB SQL (?, $name) and parameters.
    As with the DB-API drivers, nothing is substituted when vars is None."""
    if vars is None:
        return sql, None

    def sub(m):
        if m.group(0) == "%%":
            return "%"
        return f"${m.group(1)}" if m.group(1) else "?"
    params = dict(vars) if isinstance(vars, dict) else list(vars)
    return _PLACEHOLDER_RE.sub(sub, sql), params


def q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def cols_sql(alias, cols) -> str:
    return ", ".join(f"{alias}.{q(c)}" if alias else q(c) for c in cols)


def join_sql(a, b, cols) -> str:
    return " AND ".join(f"{a}.{q(c)} = {b}.{q(c)}" for c in cols)


def same_sql(a, b, cols) -> str:
    if not cols:
        return "TRUE"
    return " AND ".join(f"{a}.{q(c)} IS NOT DISTINCT FROM {b}.{q(c)}" for c in cols)


def project_sql(relation: str, cols: list, have, types: dict) -> str:
    """Subquery with exactly ``cols`` of ``relation``; a column it lacks
    (``have`` is what it has) reads as NULL of the type in ``types``."""
    have = set(have)
    sel = [q(c) if c in have else f"CAST(NULL AS {types.get(c, 'VARCHAR')}) AS {q(c)}"
           for c in cols]
    return f"(SELECT {', '.join(sel)} FROM {relation})"


class DuckDBCursor:
    """DB-API cursor over a DuckDB connection that takes pyformat
    parameters and reports DML like the other drivers: DuckDB answers
    INSERT/UPDATE/DELETE with a row holding the count, which Session.sql
    would take for a result set, so it is turned into rowcount."""

    def __init__(self, conn):
        self._conn = conn
        self.description = None
        self.rowcount = -1
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, query, vars=None):
        sql, params = to_duckdb(query, vars)
        cur = self._conn.execute(sql, params) if params is not None else self._conn.execute(sql)
        rows = cur.fetchall() if cur.description else []
        if (get_sql_operation_keyword(query) or "") in _DML:
            self.description = None
            self.rowcount = int(rows[0][0]) if rows and isinstance(rows[0][0], int) else -1
            self._rows = []
        else:
            self.description = cur.description
            self._rows = rows
            self.rowcount = len(rows)
        return self

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        pass


class Scratch:
    """Names and creates the working tables of one merge, revert or diff:
    TEMP tables (schema None) or tables in ``schema``. Names are unique per
    instance, so concurrent operations never share them."""

    def __init__(self, db, schema: str = None):
        self.db = db
        self.schema = schema
        self.tag = uuid.uuid4().hex[:10]
        self.created = []

    def name(self, base: str) -> str:
        n = q(f"__bb_{base}_{self.tag}")
        return f"{self.schema}.{n}" if self.schema else n

    def create(self, base: str, select: str) -> str:
        name = self.name(base)
        kind = "TABLE" if self.schema else "TEMP TABLE"
        self.db.execute(f"CREATE OR REPLACE {kind} {name} AS {select}")
        self.created.append(name)
        return name

    def drop(self) -> None:
        for name in reversed(self.created):
            self.db.execute(f"DROP TABLE IF EXISTS {name}")
        self.created = []


def three_way(db, target: str, base: str, theirs: str, cols: list, pk: list,
              on_conflict, scratch: Scratch) -> dict:
    """Apply theirs' changes since base to table ``target`` (ours), matching
    rows by ``pk``. ``base`` and ``theirs`` are relations with exactly
    ``cols``. Rows both sides changed differently are conflicts: "ours"
    keeps ours, "theirs" takes theirs, and for a callable the conflict rows
    (Dolt's base_*/our_*/their_* shape) are returned under "rows" for the
    caller to resolve. Without a primary key, only theirs' new rows are
    added. The caller drops the scratch tables."""
    T = scratch.create("t", f"SELECT * FROM {theirs}")
    B = scratch.create("b", f"SELECT * FROM {base}")
    O = scratch.create("o", f"SELECT {cols_sql(None, cols)} FROM {target}")
    info = {"ours_changes": 0, "theirs_changes": 0, "conflicts": 0, "rows": []}
    if not pk:
        n = db.execute(f"INSERT INTO {target} ({cols_sql(None, cols)}) "
                       f"SELECT * FROM {T} EXCEPT ALL SELECT * FROM {B}").fetchall()[0][0]
        info["theirs_changes"] = n
        info["no_primary_key"] = True
        return info
    rest = [c for c in cols if c not in pk]
    keys = cols_sql(None, pk)
    changes = {}
    for side, src in (("t", T), ("o", O)):
        changes[side] = scratch.create(
            f"{side}k",
            f"SELECT {cols_sql('x', pk)}, 'u' AS side FROM {src} x WHERE NOT EXISTS "
            f"(SELECT 1 FROM {B} b WHERE {join_sql('b', 'x', pk)} AND {same_sql('b', 'x', rest)}) "
            f"UNION ALL SELECT {cols_sql('b', pk)}, 'd' FROM {B} b WHERE NOT EXISTS "
            f"(SELECT 1 FROM {src} x WHERE {join_sql('b', 'x', pk)})")
    TK, OK = changes["t"], changes["o"]
    info["theirs_changes"] = db.execute(f"SELECT COUNT(*) FROM {TK}").fetchall()[0][0]
    info["ours_changes"] = db.execute(f"SELECT COUNT(*) FROM {OK}").fetchall()[0][0]
    CK = scratch.create(
        "ck",
        f"SELECT {cols_sql('a', pk)} FROM {TK} a JOIN {OK} b ON {join_sql('a', 'b', pk)} "
        f"LEFT JOIN {O} o ON {join_sql('a', 'o', pk)} LEFT JOIN {T} t ON {join_sql('a', 't', pk)} "
        f"WHERE NOT (a.side = 'd' AND b.side = 'd') "
        f"AND NOT (a.side = 'u' AND b.side = 'u' AND {same_sql('o', 't', rest)})")
    info["conflicts"] = db.execute(f"SELECT COUNT(*) FROM {CK}").fetchall()[0][0]

    def apply(conflicting: bool):
        pick = "IN" if conflicting else "NOT IN"
        db.execute(
            f"DELETE FROM {target} WHERE ({keys}) IN "
            f"(SELECT {keys} FROM {TK} WHERE side = 'd') "
            f"AND ({keys}) {pick} (SELECT {keys} FROM {CK})")
        upsert = (f"INSERT INTO {target} ({cols_sql(None, cols)}) "
                  f"SELECT {cols_sql('t', cols)} FROM {T} t WHERE ({cols_sql('t', pk)}) IN "
                  f"(SELECT {keys} FROM {TK} WHERE side = 'u') "
                  f"AND ({cols_sql('t', pk)}) {pick} (SELECT {keys} FROM {CK}) "
                  f"ON CONFLICT ({keys}) DO ")
        sets = ", ".join(f"{q(c)} = excluded.{q(c)}" for c in rest)
        db.execute(upsert + (f"UPDATE SET {sets}" if rest else "NOTHING"))

    if info["conflicts"] and callable(on_conflict):
        info["rows"] = conflict_rows(db, cols, pk, B, O, T, CK)
    apply(conflicting=False)
    if info["conflicts"] and on_conflict == "theirs":
        apply(conflicting=True)
    return info


def conflict_rows(db, cols, pk, B, O, T, CK) -> list:
    p0 = q(pk[0])
    rows = db.execute(
        f"SELECT {cols_sql('b', cols)}, {cols_sql('o', cols)}, {cols_sql('t', cols)}, "
        f"b.{p0} IS NULL, o.{p0} IS NULL, t.{p0} IS NULL FROM {CK} c "
        f"LEFT JOIN {B} b ON {join_sql('c', 'b', pk)} "
        f"LEFT JOIN {O} o ON {join_sql('c', 'o', pk)} "
        f"LEFT JOIN {T} t ON {join_sql('c', 't', pk)} LIMIT {CONFLICT_ROW_LIMIT}").fetchall()
    lower = [c.lower() for c in cols]
    names = [f"base_{c}" for c in lower] + [f"our_{c}" for c in lower] + [f"their_{c}" for c in lower]
    out = []
    for r in rows:
        d = dict(zip(names, r[:len(names)]))
        no_base, no_ours, no_theirs = r[len(names):]

        def diff_type(missing):
            if missing:
                return "removed"
            return "added" if no_base else "modified"
        d["our_diff_type"] = diff_type(no_ours)
        d["their_diff_type"] = diff_type(no_theirs)
        out.append(d)
    return out


def revert(db, target: str, before: str, after: str, cols: list, pk: list) -> None:
    """Undo, in table ``target``, the row changes between relations
    ``before`` and ``after`` (both with exactly ``cols``): rows added are
    deleted, rows changed or deleted are put back as they were."""
    rest = [c for c in cols if c not in pk]
    keys = cols_sql(None, pk)
    db.execute(f"DELETE FROM {target} WHERE ({keys}) IN (SELECT {cols_sql('e', pk)} FROM {after} e "
               f"WHERE NOT EXISTS (SELECT 1 FROM {before} s WHERE {join_sql('s', 'e', pk)}))")
    sets = ", ".join(f"{q(c)} = excluded.{q(c)}" for c in rest)
    db.execute(
        f"INSERT INTO {target} ({cols_sql(None, cols)}) SELECT {cols_sql('s', cols)} FROM {before} s "
        f"WHERE NOT EXISTS (SELECT 1 FROM {after} e WHERE {join_sql('s', 'e', pk)} "
        f"AND {same_sql('s', 'e', rest)}) "
        f"ON CONFLICT ({keys}) DO " + (f"UPDATE SET {sets}" if rest else "NOTHING"))


def diff_counts(db, a: str, b: str, cols: list, pk: list) -> tuple:
    """(added, deleted, modified) rows from relation a to relation b, both
    with ``cols``; rows are matched by ``pk`` (by every column without one)."""
    keys = [c for c in pk if c in cols]
    rest = [c for c in cols if c not in keys]
    on = join_sql("x", "y", keys) if keys else same_sql("x", "y", cols)
    modified = (f"COUNT(*) FILTER (WHERE x.__p AND y.__p AND NOT ({same_sql('x', 'y', rest)}))"
                if keys and rest else "0")
    return tuple(int(v) for v in db.execute(
        f"SELECT COUNT(*) FILTER (WHERE x.__p IS NULL), COUNT(*) FILTER (WHERE y.__p IS NULL), "
        f"{modified} FROM (SELECT TRUE AS __p, {cols_sql(None, cols)} FROM {a}) x "
        f"FULL OUTER JOIN (SELECT TRUE AS __p, {cols_sql(None, cols)} FROM {b}) y ON {on}"
    ).fetchall()[0])
