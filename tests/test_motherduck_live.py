"""MotherDuck backend against a live account (skipped unless
MOTHERDUCK_TOKEN is set, e.g. in .env). Exercises every verb through the
public API."""

import pytest

from dblib import motherduck as md
from dblib import result_collector as rc

DB = "bb_test_motherduck"

pytestmark = pytest.mark.skipif(not md.MOTHERDUCK_TOKEN, reason="no MotherDuck token configured")


@pytest.fixture
def suite():
    md.setup_database(DB)
    s = md.MotherDuckToolSuite.init_for_bench(rc.ResultCollector(), DB)
    res = s.exec(["CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20), n INT)",
                  "CREATE TABLE h (a INT, b VARCHAR(10))",
                  "INSERT INTO t VALUES (1,'a',1),(2,'b',2),(3,'c',3)",
                  "INSERT INTO h VALUES (1,'x')"], refs=["main"])[0]
    assert res.ok, res.error
    yield s
    s.close_connection()
    md.drop_database(DB)


def rows(suite, ref, sql):
    res = suite.exec([sql], refs=[ref])[0]
    assert res.ok, res.error
    return [tuple(r) for r in res.rows]


def run(suite, ref, *stmts):
    res = suite.exec(list(stmts), refs=[ref])[0]
    assert res.ok, res.error
    return res


def test_dml_reports_rows_affected(suite):
    res = run(suite, "main", "UPDATE t SET n = n + 1")
    assert res.rows is None
    res = suite.exec([("UPDATE t SET n = n + %s WHERE id <= %s", (1, 2))], refs=["main"])[0]
    assert res.ok and res.statements[-1].ok
    assert sorted(rows(suite, "main", "SELECT id, n FROM t")) == [(1, 3), (2, 4), (3, 4)]


def test_commit_log_diff_reset_revert(suite):
    c1 = suite.commit("main", "one")
    assert c1.ok and c1.value
    run(suite, "main", "UPDATE t SET v='a2' WHERE id=1", "INSERT INTO t VALUES (4,'d',4)",
        "DELETE FROM t WHERE id=3")
    c2 = suite.commit("main", "two LOGSENTINEL-x")
    log = suite.log("main", limit=10)
    assert log.ok and [e["message"] for e in log.value][:2] == ["two LOGSENTINEL-x", "one"]

    d = suite.diff(f"main@{c1.value}", f"main@{c2.value}")
    assert d.ok, d.error
    assert (d.value["rows_added"], d.value["rows_deleted"], d.value["rows_modified"]) == (1, 1, 1)

    r = suite.revert("main", c2.value)
    assert r.ok, r.error
    assert sorted(rows(suite, "main", "SELECT id, v FROM t")) == [(1, "a"), (2, "b"), (3, "c")]

    run(suite, "main", "UPDATE t SET v='zz'")
    rs = suite.reset("main", c2.value)
    assert rs.ok, rs.error
    assert sorted(rows(suite, "main", "SELECT id, v FROM t")) == [(1, "a2"), (2, "b"), (4, "d")]
    assert [e["message"] for e in suite.log("main").value][0] == "two LOGSENTINEL-x"
    # Earlier commits stay readable after the reset.
    assert rows(suite, f"main@{c1.value}", "SELECT v FROM t WHERE id = 1") == [("a",)]


def test_reset_branch_to_inherited_commit(suite):
    c1 = suite.commit("main", "base").value
    assert suite.branch("feat", "main").ok
    run(suite, "feat", "UPDATE t SET v='feat' WHERE id=1")
    suite.commit("feat", "feat work")
    assert suite.reset("feat", c1).ok
    assert rows(suite, "feat", "SELECT v FROM t WHERE id=1") == [("a",)]


def test_branch_from_commit_multi_ref_and_delete(suite):
    c1 = suite.commit("main", "base").value
    run(suite, "main", "UPDATE t SET v='later' WHERE id=1")
    suite.commit("main", "later")
    assert suite.branch("old", from_ref=f"main@{c1}").ok
    assert rows(suite, "old", "SELECT v FROM t WHERE id=1") == [("a",)]
    res = suite.exec(lambda db: db.sql(f"SELECT COUNT(*) FROM {db.table('old', 't')} o "
                                       f"JOIN {db.table('main', 't')} m ON o.id = m.id"),
                     refs=["main", "old"], mode="multi")[0]
    assert res.ok and [tuple(r) for r in res.value] == [(3,)]
    res = suite.exec(lambda db: db.sql(f"SELECT v FROM {db.table(f'main@{c1}', 't')} WHERE id = 1"),
                     refs=["main"], mode="multi")[0]
    assert res.ok and [tuple(r) for r in res.value] == [("a",)]
    # Writes on a commit ref land on a temporary copy, not on the commit.
    run(suite, f"main@{c1}", "UPDATE t SET v = 'scratch' WHERE id = 1")
    assert rows(suite, "old", "SELECT v FROM t WHERE id=1") == [("a",)]
    c_old = suite.commit("old", "on old").value
    assert "old" in suite.list_branches()
    assert suite.delete("old").ok
    assert "old" not in suite.list_branches()
    # A deleted branch's commits stay readable.
    assert rows(suite, f"old@{c_old}", "SELECT v FROM t WHERE id=1") == [("a",)]


def test_three_way_merge_and_conflicts(suite):
    suite.commit("main", "base")
    assert suite.branch("feat", "main").ok
    run(suite, "feat", "UPDATE t SET v='feat' WHERE id=1", "INSERT INTO t VALUES (10,'ten',10)",
        "DELETE FROM t WHERE id=3", "INSERT INTO h VALUES (2,'y')",
        "ALTER TABLE t ADD COLUMN extra INT")
    suite.commit("feat", "feature work SENTINEL")
    run(suite, "main", "UPDATE t SET v='spine' WHERE id=2", "UPDATE t SET n=99 WHERE id=1",
        "INSERT INTO t VALUES (20,'twenty',20)")
    suite.commit("main", "spine work")

    m = suite.merge("main", "feat", message="merge feat", on_conflict="theirs")
    assert m.ok, m.error
    assert m.value["conflicts"] == 1 and m.value["conflict_tables"] == ["t"]
    assert m.value["fast_forward"] is False
    assert "t.extra" in m.value["schema_changes"]["added_columns"]
    got = {r[0]: r[1:] for r in rows(suite, "main", "SELECT id, v, n FROM t")}
    assert got == {1: ("feat", 1), 2: ("spine", 2), 10: ("ten", 10), 20: ("twenty", 20)}
    assert sorted(rows(suite, "main", "SELECT a FROM h")) == [(1,), (2,)]
    msgs = [e["message"] for e in suite.log("main", limit=10).value]
    assert msgs[0] == "merge feat" and any("SENTINEL" in x for x in msgs)

    assert suite.branch("ff", "main").ok
    run(suite, "ff", "INSERT INTO t (id, v, n) VALUES (30,'thirty',30)")
    suite.commit("ff", "ff work")
    m2 = suite.merge("main", "ff")
    assert m2.ok and m2.value["fast_forward"] is True and m2.value["conflicts"] == 0


def test_merge_adds_tables_created_on_the_source(suite):
    suite.commit("main", "base")
    assert suite.branch("feat", "main").ok
    run(suite, "feat", "CREATE TABLE extra (id INT PRIMARY KEY, x INT)",
        "INSERT INTO extra VALUES (1, 1)")
    m = suite.merge("main", "feat")
    assert m.ok, m.error
    assert m.value["schema_changes"]["added_tables"] == ["extra"]
    assert rows(suite, "main", "SELECT id, x FROM extra") == [(1, 1)]
    # The table keeps its primary key.
    res = suite.exec(["INSERT INTO extra VALUES (1, 2)"], refs=["main"])[0]
    assert res.failed


def test_rebase_with_callable_resolver(suite):
    suite.commit("main", "base")
    assert suite.branch("dev", "main").ok
    run(suite, "dev", "UPDATE t SET n = n + 5 WHERE id=1")
    suite.commit("dev", "dev adds 5")
    run(suite, "main", "UPDATE t SET n = n + 7 WHERE id=1", "INSERT INTO t VALUES (40,'f',40)",
        "DELETE FROM t WHERE id=2")
    suite.commit("main", "spine adds 7")

    seen = {}

    def additive(db, conflicts):
        for cf in conflicts:
            seen[cf["table"]] = cf["rows"]
            for row in cf["rows"]:
                total = row["our_n"] + row["their_n"] - row["base_n"]
                db.sql("UPDATE t SET n = %s WHERE id = %s", (total, row["our_id"]))
            db.resolve(cf["table"])

    rb = suite.rebase("dev", "main", on_conflict=additive)
    assert rb.ok, rb.error
    assert rb.value["conflicts"] == 1 and seen["t"][0]["their_diff_type"] == "modified"
    got = {r[0]: r[1] for r in rows(suite, "dev", "SELECT id, n FROM t")}
    assert got == {1: 13, 3: 3, 40: 40}
    assert any(e["message"] == "dev adds 5" for e in suite.log("dev", limit=20).value)
    m = suite.merge("main", "dev")
    assert m.ok and m.value["conflicts"] == 0 and m.value["fast_forward"] is True
    assert {r[0]: r[1] for r in rows(suite, "main", "SELECT id, n FROM t")} == got


def test_rebase_conflict_sides_follow_dolt(suite):
    suite.commit("main", "base")
    for name in ("keep_up", "keep_br"):
        assert suite.branch(name, "main").ok
        run(suite, name, "UPDATE t SET v = 'branch' WHERE id = 1")
    run(suite, "main", "UPDATE t SET v = 'upstream' WHERE id = 1")
    assert suite.rebase("keep_up", "main", on_conflict="ours").ok
    assert suite.rebase("keep_br", "main", on_conflict="theirs").ok
    assert rows(suite, "keep_up", "SELECT v FROM t WHERE id = 1") == [("upstream",)]
    assert rows(suite, "keep_br", "SELECT v FROM t WHERE id = 1") == [("branch",)]


def test_schema_conflicts_fail_without_touching_data(suite):
    suite.commit("main", "base")
    assert suite.branch("pk", "main").ok
    run(suite, "pk", "DROP TABLE t",
        "CREATE TABLE t (id INT, v VARCHAR(20), n INT, PRIMARY KEY (id, n))",
        "INSERT INTO t VALUES (1,'a',1)")
    m = suite.merge("main", "pk")
    assert m.failed and "schema conflict" in m.error
    assert rows(suite, "main", "SELECT COUNT(*) FROM t") == [(3,)]
    # S4.3a: the same column added with different types on the two sides.
    assert suite.branch("dev", "main").ok
    run(suite, "dev", "ALTER TABLE t ADD COLUMN c_tier VARCHAR(8)", "UPDATE t SET c_tier = 'gold'")
    run(suite, "main", "ALTER TABLE t ADD COLUMN c_tier INT")
    rb = suite.rebase("dev", "main", on_conflict="theirs")
    assert rb.failed and "schema conflict" in rb.error
    assert rows(suite, "dev", "SELECT DISTINCT c_tier FROM t") == [("gold",)]


def test_branches_are_visible_to_other_connections(suite):
    other = md.MotherDuckToolSuite.init_for_bench(rc.ResultCollector(), DB)
    try:
        assert suite.branch("shared", "main").ok
        run(suite, "shared", "UPDATE t SET v = 'shared' WHERE id = 1")
        assert rows(other, "shared", "SELECT v FROM t WHERE id = 1") == [("shared",)]
        # Two writers on the same row: the second fails with a conflict.
        a, b = suite.conn.raw, other.conn.raw
        a.execute('USE "bb_test_motherduck"')
        b.execute('USE "bb_test_motherduck"')
        a.execute("BEGIN")
        a.execute("UPDATE t SET n = 100 WHERE id = 2")
        b.execute("BEGIN")
        with pytest.raises(Exception, match="(?i)conflict"):
            b.execute("UPDATE t SET n = 200 WHERE id = 2")
        b.execute("ROLLBACK")
        a.execute("COMMIT")
    finally:
        other.close_connection()


def test_nothing_left_after_drop():
    db = DB + "_drop"
    md.setup_database(db)
    s = md.MotherDuckToolSuite.init_for_bench(rc.ResultCollector(), db)
    run(s, "main", "CREATE TABLE t (id INT PRIMARY KEY)")
    c = s.commit("main", "c").value
    assert s.branch("b", "main").ok
    run(s, f"main@{c}", "SELECT 1")  # a commit-ref temporary
    assert s.reset("main", c).ok
    s.close_connection()
    md.drop_database(db)
    con = md.connect()
    try:
        assert md._run_databases(con, db) == []
    finally:
        con.close()


def test_merge_carries_views(suite):
    suite.commit("main", "base")
    assert suite.branch("cand", "main").ok
    run(suite, "cand", "CREATE VIEW recent AS SELECT id, v FROM t WHERE n >= 2",
        "UPDATE t SET n = 5 WHERE id = 1")
    m = suite.merge("main", "cand")
    assert m.ok, m.error
    assert m.value["schema_changes"]["added_views"] == ["recent"]
    assert sorted(rows(suite, "main", "SELECT id FROM recent")) == [(1,), (2,), (3,)]
    d = suite.diff("main", "cand")
    assert d.ok and [t["table_name"] for t in d.value["tables"]] == ["h", "t"]


def test_exec_after_a_failed_rebase_stays_on_its_branch(suite):
    suite.commit("main", "base")
    assert suite.branch("dev", "main").ok
    run(suite, "dev", "ALTER TABLE t ADD COLUMN c_tier VARCHAR(8)")
    run(suite, "main", "ALTER TABLE t ADD COLUMN c_tier INT")
    assert suite.rebase("dev", "main").failed
    run(suite, "dev", "CREATE TABLE only_on_dev (id INT)")
    assert rows(suite, "main", "SELECT COUNT(*) FROM duckdb_tables() WHERE database_name = "
                               "current_database() AND table_name = 'only_on_dev'") == [(0,)]


def test_concurrent_workers_see_each_others_databases(suite):
    import threading

    suite.commit("main", "base")
    errors = []

    def worker(i):
        w = md.MotherDuckToolSuite.init_for_bench(rc.ResultCollector(), DB)
        try:
            for k in range(3):
                assert w.branch(f"w{i}_{k}", "main", raise_on_error=True).ok
                w.exec([f"UPDATE t SET n = n + 1 WHERE id = {1 + i % 3}"], refs=[f"w{i}_{k}"],
                       raise_on_error=True)
                w.commit(f"w{i}_{k}", f"w{i} step {k}", raise_on_error=True)
            # A branch another worker created moments ago.
            other = f"w{(i + 1) % 5}_0"
            for _ in range(50):
                if other in w.list_branches():
                    break
            w.exec(["SELECT COUNT(*) FROM t"], refs=[other], raise_on_error=True)
        except Exception as e:
            errors.append(f"worker {i}: {type(e).__name__}: {e}")
        finally:
            w.close_connection()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert errors == []
