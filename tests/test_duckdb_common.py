"""duckdb_common as MotherDuck uses it: relations in other databases of one
session, working tables in a scratch schema of the target database."""

import duckdb

from dblib import duckdb_common as ddb


def session():
    """A DuckDB session with three databases standing in for MotherDuck
    branch databases: base (the fork point), ours and theirs."""
    db = duckdb.connect()
    for name in ("base", "ours", "theirs"):
        db.execute(f"ATTACH ':memory:' AS {name}")
        db.execute(f"CREATE TABLE {name}.main.t (id INTEGER PRIMARY KEY, v VARCHAR, n INTEGER)")
        db.execute(f"INSERT INTO {name}.main.t VALUES (1,'a',1),(2,'b',2),(3,'c',3)")
    db.execute("CREATE SCHEMA ours._bb")
    return db


def contents(db, rel="ours.main.t"):
    return {r[0]: r[1:] for r in db.execute(f"SELECT id, v, n FROM {rel}").fetchall()}


COLS, PK = ["id", "v", "n"], ["id"]


def test_three_way_across_databases_with_scratch_schema():
    db = session()
    db.execute("UPDATE ours.main.t SET v = 'ours' WHERE id = 1")
    db.execute("UPDATE theirs.main.t SET v = 'theirs' WHERE id = 1")
    db.execute("UPDATE theirs.main.t SET n = 20 WHERE id = 2")
    db.execute("DELETE FROM theirs.main.t WHERE id = 3")
    db.execute("INSERT INTO theirs.main.t VALUES (4, 'd', 4)")
    scratch = ddb.Scratch(db, schema="ours._bb")
    info = ddb.three_way(db, "ours.main.t", "base.main.t", "theirs.main.t", COLS, PK,
                         "theirs", scratch)
    assert info["conflicts"] == 1 and info["theirs_changes"] == 4 and info["ours_changes"] == 1
    assert contents(db) == {1: ("theirs", 1), 2: ("b", 20), 4: ("d", 4)}
    # Working tables live in the scratch schema, and are dropped after.
    assert db.execute("SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = 'ours' "
                      "AND schema_name = '_bb'").fetchall() == [(6,)]
    scratch.drop()
    assert db.execute("SELECT COUNT(*) FROM duckdb_tables() WHERE schema_name = '_bb'").fetchall() == [(0,)]


def test_scratch_names_are_unique_per_operation():
    db = session()
    a, b = ddb.Scratch(db, "ours._bb"), ddb.Scratch(db, "ours._bb")
    assert a.name("t") != b.name("t")


def test_three_way_callable_rows_and_projection():
    db = session()
    db.execute("ALTER TABLE theirs.main.t ADD COLUMN extra INTEGER")
    db.execute("UPDATE theirs.main.t SET v = 'theirs', extra = 7 WHERE id = 1")
    db.execute("UPDATE ours.main.t SET v = 'ours' WHERE id = 1")
    db.execute("ALTER TABLE ours.main.t ADD COLUMN extra INTEGER")
    cols = COLS + ["extra"]
    types = {"extra": "INTEGER"}
    base = ddb.project_sql("base.main.t", cols, COLS, types)  # base predates extra
    scratch = ddb.Scratch(db, "ours._bb")
    info = ddb.three_way(db, "ours.main.t", base, "theirs.main.t", cols, PK, lambda *a: None, scratch)
    row = info["rows"][0]
    assert (row["base_v"], row["our_v"], row["their_v"], row["their_extra"]) == ("a", "ours", "theirs", 7)
    assert contents(db)[1] == ("ours", 1)  # left for the callable
    scratch.drop()


def test_revert_and_diff_across_databases():
    db = session()
    # "after" = base with id 1 changed, id 3 deleted and id 4 added.
    db.execute("ATTACH ':memory:' AS after")
    db.execute("CREATE TABLE after.main.t AS SELECT * FROM base.main.t")
    db.execute("UPDATE after.main.t SET v = 'changed' WHERE id = 1")
    db.execute("DELETE FROM after.main.t WHERE id = 3")
    db.execute("INSERT INTO after.main.t VALUES (4, 'd', 4)")
    assert ddb.diff_counts(db, "base.main.t", "after.main.t", COLS, PK) == (1, 1, 1)
    # Ours took that change and then changed id 2; reverting keeps id 2.
    db.execute("DELETE FROM ours.main.t")
    db.execute("INSERT INTO ours.main.t SELECT * FROM after.main.t")
    db.execute("UPDATE ours.main.t SET v = 'later' WHERE id = 2")
    ddb.revert(db, "ours.main.t", "base.main.t", "after.main.t", COLS, PK)
    assert contents(db) == {1: ("a", 1), 2: ("later", 2), 3: ("c", 3)}


def test_cursor_placeholders_and_dml_rowcount():
    db = session()
    db.execute("USE ours")
    cur = ddb.DuckDBCursor(db)
    with cur:
        cur.execute("UPDATE t SET n = n + %(by)s WHERE id <= %(upto)s", {"by": 1, "upto": 2})
        assert cur.description is None and cur.rowcount == 2
        cur.execute("SELECT v FROM t WHERE id = %s AND v LIKE 'a%%'", (1,))
        assert cur.description is not None and cur.fetchall() == [("a",)]
        cur.execute("SELECT '%s'")  # no parameters: nothing substituted
        assert cur.fetchone() == ("%s",)
