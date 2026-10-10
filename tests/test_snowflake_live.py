"""Snowflake backend against a live account (skipped unless SNOWFLAKE_ACCOUNT
is set, e.g. in .env). Exercises every verb through the public API."""

import pytest

from dblib import result_collector as rc
from dblib import snowflake

DB = "bb_test_snowflake"

pytestmark = pytest.mark.skipif(not snowflake.SNOWFLAKE_ACCOUNT,
                                reason="no Snowflake account configured")


@pytest.fixture
def suite():
    snowflake.setup_database(DB)
    s = snowflake.SnowflakeToolSuite.init_for_bench(rc.ResultCollector(), DB)
    s.exec(["CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20), n INT)",
            "CREATE TABLE h (a INT, b VARCHAR(10))",
            "INSERT INTO t VALUES (1,'a',1),(2,'b',2),(3,'c',3)",
            "INSERT INTO h VALUES (1,'x')"], refs=["main"])
    yield s
    s.close_connection()
    snowflake.drop_database(DB)


def rows(suite, ref, sql):
    return [tuple(r) for r in suite.exec([sql], refs=[ref])[0].rows]


def columns(suite, ref, table):
    return [r[0] for r in rows(suite, ref, "SELECT column_name FROM information_schema.columns "
                                           f"WHERE table_schema = 'PUBLIC' AND table_name = '{table}'")]


def test_dml_reports_rows_affected_not_a_result_set(suite):
    res = suite.exec(["UPDATE t SET n = n + 1"], refs=["main"])[0]
    assert res.ok and res.rows is None


def test_commit_log_diff_reset_revert(suite):
    c1 = suite.commit("main", "one")
    assert c1.ok and c1.value
    suite.exec(["UPDATE t SET v='a2' WHERE id=1", "INSERT INTO t VALUES (4,'d',4)",
                "DELETE FROM t WHERE id=3"], refs=["main"])
    c2 = suite.commit("main", "two")
    assert c2.ok
    log = suite.log("main", limit=10)
    assert log.ok and [e["message"] for e in log.value][:2] == ["two", "one"]

    d = suite.diff(f"main@{c1.value}", f"main@{c2.value}")
    assert d.ok, d.error
    assert (d.value["rows_added"], d.value["rows_deleted"], d.value["rows_modified"]) == (1, 1, 1)

    r = suite.revert("main", c2.value)
    assert r.ok, r.error
    assert sorted(rows(suite, "main", "SELECT id, v FROM t")) == [(1, "a"), (2, "b"), (3, "c")]

    suite.exec(["UPDATE t SET v='zz'"], refs=["main"])
    rs = suite.reset("main", c2.value)
    assert rs.ok, rs.error
    assert sorted(rows(suite, "main", "SELECT id, v FROM t")) == [(1, "a2"), (2, "b"), (4, "d")]
    assert [e["message"] for e in suite.log("main").value][0] == "two"


def test_history_survives_reset(suite):
    """reset swaps the branch's database for a clone; earlier snapshots
    must still be readable through the archive."""
    c1 = suite.commit("main", "one").value
    suite.exec(["UPDATE t SET v='a2' WHERE id=1"], refs=["main"])
    c2 = suite.commit("main", "two").value
    suite.exec(["UPDATE t SET v='a3' WHERE id=1"], refs=["main"])
    assert suite.reset("main", c2).ok
    d = suite.diff(f"main@{c1}", f"main@{c2}")
    assert d.ok, d.error
    assert d.value["rows_modified"] == 1
    assert rows(suite, f"main@{c1}", "SELECT v FROM t WHERE id=1") == [("a",)]
    # A second reset, to the older commit, also works.
    assert suite.reset("main", c1).ok
    assert rows(suite, "main", "SELECT v FROM t WHERE id=1") == [("a",)]


def test_revert_merged_commit_after_its_branch_is_deleted(suite):
    """S2's sequence: a commit merged into main, its branch deleted, then
    the commit reverted on main. The deleted branch's history must survive."""
    suite.commit("main", "base")
    assert suite.branch("cand", "main").ok
    suite.exec(["INSERT INTO t VALUES (9,'sentinel',9)"], refs=["cand"])
    c = suite.commit("cand", "compaction").value
    assert suite.merge("main", "cand").ok
    d = suite.delete("cand")
    assert d.ok and d.value["archived"] is True
    assert "cand" not in suite.list_branches()
    r = suite.revert("main", c)
    assert r.ok, r.error
    assert rows(suite, "main", "SELECT COUNT(*) FROM t WHERE id = 9") == [(0,)]
    # A branch nobody else references is dropped outright.
    assert suite.branch("scratch", "main").ok
    assert suite.delete("scratch").value["archived"] is False


def test_reset_branch_to_inherited_commit(suite):
    """A commit made on the parent before the fork lives in the parent's
    history; resetting the branch to it clones it from there."""
    c1 = suite.commit("main", "base").value
    assert suite.branch("feat", "main").ok
    suite.exec(["UPDATE t SET v='feat' WHERE id=1"], refs=["feat"])
    suite.commit("feat", "feat work")
    r = suite.reset("feat", c1)
    assert r.ok, r.error
    assert rows(suite, "feat", "SELECT v FROM t WHERE id=1") == [("a",)]
    assert rows(suite, "main", "SELECT v FROM t WHERE id=1") == [("a",)]


def test_branch_from_commit_and_multi_ref_exec(suite):
    c1 = suite.commit("main", "base").value
    suite.exec(["UPDATE t SET v='later' WHERE id=1"], refs=["main"])
    suite.commit("main", "later")
    assert suite.branch("old", from_ref=f"main@{c1}").ok
    assert rows(suite, "old", "SELECT v FROM t WHERE id=1") == [("a",)]
    assert rows(suite, "main", "SELECT v FROM t WHERE id=1") == [("later",)]
    res = suite.exec(lambda db: db.sql(f"SELECT COUNT(*) FROM {db.table('old', 't')} o "
                                       f"JOIN {db.table('main', 't')} m ON o.id = m.id"),
                     refs=["main", "old"], mode="multi")[0]
    assert res.ok and [tuple(r) for r in res.value] == [(3,)]
    # A commit ref in a multi-ref script reads the snapshot.
    res = suite.exec(lambda db: db.sql(f"SELECT v FROM {db.table(f'main@{c1}', 't')} WHERE id = 1"),
                     refs=["main"], mode="multi")[0]
    assert res.ok and [tuple(r) for r in res.value] == [("a",)]
    assert "old" in suite.list_branches()
    assert suite.delete("old").ok
    assert "old" not in suite.list_branches()


def test_three_way_merge_and_conflicts(suite):
    suite.commit("main", "base")
    assert suite.branch("feat", "main").ok
    # Branch: modify 1, add 10, delete 3, add a row to the keyless table.
    suite.exec(["UPDATE t SET v='feat' WHERE id=1", "INSERT INTO t VALUES (10,'ten',10)",
                "DELETE FROM t WHERE id=3", "INSERT INTO h VALUES (2,'y')",
                "ALTER TABLE t ADD COLUMN extra INT"], refs=["feat"])
    suite.commit("feat", "feature work SENTINEL")
    # Spine meanwhile: modify 2 (no conflict), modify 1 (conflict), add 20.
    suite.exec(["UPDATE t SET v='spine' WHERE id=2", "UPDATE t SET n=99 WHERE id=1",
                "INSERT INTO t VALUES (20,'twenty',20)"], refs=["main"])
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

    # Fast-forward: a branch merged back without spine changes.
    assert suite.branch("ff", "main").ok
    suite.exec(["INSERT INTO t (id, v, n) VALUES (30,'thirty',30)"], refs=["ff"])
    suite.commit("ff", "ff work")
    m2 = suite.merge("main", "ff")
    assert m2.ok and m2.value["fast_forward"] is True and m2.value["conflicts"] == 0


def test_merge_adds_tables_created_on_the_source(suite):
    suite.commit("main", "base")
    assert suite.branch("feat", "main").ok
    suite.exec(["CREATE TABLE extra (id INT PRIMARY KEY, x INT)",
                "INSERT INTO extra VALUES (1, 1)"], refs=["feat"])
    m = suite.merge("main", "feat")
    assert m.ok, m.error
    assert m.value["schema_changes"]["added_tables"] == ["extra"]
    assert rows(suite, "main", "SELECT id, x FROM extra") == [(1, 1)]


def test_rebase_with_callable_resolver(suite):
    suite.commit("main", "base")
    assert suite.branch("dev", "main").ok
    suite.exec(["UPDATE t SET n = n + 5 WHERE id=1"], refs=["dev"])
    suite.commit("dev", "dev adds 5")
    suite.exec(["UPDATE t SET n = n + 7 WHERE id=1", "INSERT INTO t VALUES (40,'f',40)",
                "DELETE FROM t WHERE id=2"], refs=["main"])
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
    assert got == {1: 13, 3: 3, 40: 40}  # 1+5+7, spine's delete and insert applied
    assert any(e["message"] == "dev adds 5" for e in suite.log("dev", limit=20).value)
    # Merging the rebased branch back is a fast-forward with no conflicts.
    m = suite.merge("main", "dev")
    assert m.ok and m.value["conflicts"] == 0 and m.value["fast_forward"] is True
    assert {r[0]: r[1] for r in rows(suite, "main", "SELECT id, n FROM t")} == got


def test_rebase_conflict_sides_follow_dolt(suite):
    """In a rebase, "ours" is the upstream and "theirs" the branch."""
    suite.commit("main", "base")
    for name in ("keep_up", "keep_branch"):
        assert suite.branch(name, "main").ok
        suite.exec(["UPDATE t SET v = 'branch' WHERE id = 1"], refs=[name])
    suite.exec(["UPDATE t SET v = 'upstream' WHERE id = 1"], refs=["main"])
    assert suite.rebase("keep_up", "main", on_conflict="ours").ok
    assert suite.rebase("keep_branch", "main", on_conflict="theirs").ok
    assert rows(suite, "keep_up", "SELECT v FROM t WHERE id = 1") == [("upstream",)]
    assert rows(suite, "keep_branch", "SELECT v FROM t WHERE id = 1") == [("branch",)]


def test_schema_conflict_fails_without_touching_data(suite):
    suite.commit("main", "base")
    assert suite.branch("pkchange", "main").ok
    suite.exec(["ALTER TABLE t DROP PRIMARY KEY", "ALTER TABLE t ADD PRIMARY KEY (id, n)"],
               refs=["pkchange"])
    suite.commit("pkchange", "pk change")
    suite.exec(["UPDATE t SET v = 'changed' WHERE id = 1"], refs=["pkchange"])
    m = suite.merge("main", "pkchange")
    assert m.failed and "schema conflict" in m.error
    assert rows(suite, "main", "SELECT v FROM t WHERE id = 1") == [("a",)]


def test_branch_from_commit_drops_later_schema(suite):
    c1 = suite.commit("main", "before ddl").value
    suite.exec(["ALTER TABLE t ADD COLUMN later INT", "CREATE TABLE newer (id INT PRIMARY KEY)"],
               refs=["main"])
    suite.commit("main", "after ddl")
    assert suite.branch("old", from_ref=f"main@{c1}").ok
    assert "LATER" not in columns(suite, "old", "T")
    assert rows(suite, "old", "SELECT COUNT(*) FROM information_schema.tables "
                               "WHERE table_schema = 'PUBLIC' AND table_name = 'NEWER'") == [(0,)]
    # The same step's DDL can now be replayed on the fork.
    assert suite.exec(["ALTER TABLE t ADD COLUMN later INT"], refs=["old"])[0].ok


def test_rebase_carries_column_default(suite):
    """A column added with a DEFAULT on the spine reaches the branch with the
    same default, so rows the branch writes afterwards get the value."""
    suite.commit("main", "base")
    assert suite.branch("feat", "main").ok
    suite.exec(["ALTER TABLE t ADD COLUMN cat VARCHAR(16) DEFAULT 'general'",
                "INSERT INTO t (id, v, n) VALUES (40,'forty',40)"], refs=["main"])
    suite.commit("main", "migration")
    suite.exec(["INSERT INTO t (id, v, n) VALUES (50,'fifty',50)"], refs=["feat"])
    suite.commit("feat", "feature work")

    r = suite.rebase("feat", "main")
    assert r.ok, r.error
    suite.exec(["INSERT INTO t (id, v, n) VALUES (60,'sixty',60)"], refs=["feat"])
    got = dict(rows(suite, "feat", "SELECT id, cat FROM t WHERE id IN (40, 50, 60)"))
    assert got[40] == "general" and got[60] == "general"


def test_no_databases_left_after_drop():
    snowflake.setup_database(DB)
    s = snowflake.SnowflakeToolSuite.init_for_bench(rc.ResultCollector(), DB)
    s.exec(["CREATE TABLE t (id INT PRIMARY KEY)"], refs=["main"])
    c = s.commit("main", "c").value
    s.branch("b", "main")
    s.exec(["SELECT 1"], refs=[f"main@{c}"])  # materialises a commit-ref temporary
    s.reset("main", c)  # leaves an archive
    s.close_connection()
    snowflake.drop_database(DB)
    conn = snowflake.connect()
    try:
        with conn.cursor() as cur:
            left = snowflake._run_databases(cur, DB)
            cur.execute(f"SHOW DATABASES LIKE '{DB.upper()}'")
            left += [r[1] for r in cur.fetchall()]
    finally:
        conn.close()
    assert left == []
