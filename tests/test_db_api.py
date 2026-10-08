import asyncio
import os
import tempfile

import pyarrow.parquet as pq
import pytest

from dblib import result_pb2 as rslt
from dblib.db_api import Ref, UnsupportedOperation, OpStatus
from dblib.result_collector import ResultCollector
from tests.fake_backend import FakeSuite


@pytest.fixture
def suite(tmp_path):
    rc = ResultCollector(run_id="t", output_dir=str(tmp_path))
    s = FakeSuite(rc)
    s.exec(["CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            ("INSERT INTO t VALUES (?, ?)", (1, "a"))], timed=False)
    return s


def rows(suite, op=None):
    out = suite.result_collector.results
    return [r for r in out if op is None or r.op_type == op]


def test_ref_parse():
    assert Ref.parse("main") == Ref("main")
    assert Ref.parse("main@abc") == Ref("main", "abc")
    assert str(Ref("b", "h")) == "b@h"
    assert Ref.parse(Ref("x")) == Ref("x")


def test_capabilities():
    caps = FakeSuite.capabilities()
    assert caps["branch"] and caps["commit"] and caps["log"] and caps["diff"]
    assert not caps["merge"] and not caps["rebase"] and not caps["revert"]
    assert caps["exec_async"]


def test_unsupported_verb_records_and_continues(suite):
    r = suite.merge("main", "other")
    assert r.unsupported and r.latency == 0
    row = rows(suite, rslt.OpType.MERGE)[0]
    assert row.status == rslt.OpStatus.UNSUPPORTED
    assert not suite.result_collector.workflow_supported
    assert "MERGE" in suite.result_collector.support_summary()["unsupported_ops"][0]
    with pytest.raises(UnsupportedOperation):
        r.raise_for_status()
    with pytest.raises(UnsupportedOperation):
        suite.rebase("main", "x", raise_on_error=True)


def test_failed_verb_records(suite):
    r = suite.branch("main")  # already exists
    assert r.failed and "exists" in r.error
    row = rows(suite, rslt.OpType.BRANCH)[0]
    assert row.status == rslt.OpStatus.FAILED and row.error_message
    assert suite.result_collector.workflow_supported  # failed != unsupported


def test_branch_commit_log_diff_reset_delete(suite):
    assert suite.commit("main", "init").ok
    b = suite.branch("feat", "main", storage=True)
    assert b.ok and b.storage_before > 0 and b.storage_after > b.storage_before
    res = suite.exec([("INSERT INTO t VALUES (?, ?)", (2, "b"))], refs=["feat"])
    assert res[0].ok and res[0].connect is not None and res[0].connect.ok
    c = suite.commit("feat", "add")
    assert c.ok and isinstance(c.value, str)
    log = suite.log("feat", limit=5)
    assert [e["message"] for e in log.value] == ["add", "init"]
    d = suite.diff("main", "feat")
    assert d.value == {"rows_a": 1, "rows_b": 2}
    # commit ref (supported here)
    at = suite.exec(["SELECT count(*) FROM t"], refs=[f"feat@{log.value[1]['hash']}"])
    assert at[0].ok and at[0].rows == [(1,)]
    assert not rows(suite, rslt.OpType.EXEC)[-1].commit_ref_fallback
    # reset feat back to the init snapshot
    assert suite.reset("feat", log.value[1]["hash"]).ok
    assert suite.exec(["SELECT count(*) FROM t"], refs=["feat"])[0].rows == [(1,)]
    # delete: the backend moves the connection off the branch first
    assert suite.current_ref == Ref("feat")
    assert suite.delete("feat").ok
    assert suite.current_ref == Ref("main")
    assert "feat" not in suite.list_branches()
    assert suite.delete("main").failed


def test_exec_rows_and_statement_breakdown(suite):
    suite.result_collector.record_num_keys_touched(3)
    res = suite.exec(
        ["SELECT * FROM t", ("INSERT INTO t VALUES (?, ?)", (5, "e")),
         "CREATE INDEX i ON t(v)"],
        refs=["main"], label="phase1",
    )[0]
    assert res.ok and len(res.statements) == 3
    ops = [r.op_type for r in rows(suite)]
    # on main already: no CONNECT row; then READ, INSERT, DDL, EXEC
    assert ops == [rslt.OpType.READ, rslt.OpType.INSERT, rslt.OpType.DDL,
                   rslt.OpType.EXEC]
    out = rows(suite)
    assert out[0].num_keys_touched == 3 and out[1].num_keys_touched == 0
    assert all(r.exec_id == out[0].exec_id for r in out)
    assert all(r.label == "phase1" for r in out)
    assert "CREATE INDEX" in out[-1].sql_query
    assert out[-1].latency >= sum(r.latency for r in out[:-1]) * 0.5


def test_exec_python_script_and_params(suite):
    script = """
n = params["n"]
def run(db):
    db.sql("INSERT INTO t VALUES (?, ?)", (n, "p"))
    return db.sql("SELECT count(*) FROM t")[0][0]
"""
    res = suite.exec(script, refs=["main"], params={"n": 9})[0]
    assert res.ok and res.value == 2
    # top-level only script
    res = suite.exec("db.sql('SELECT 1')", refs=["main"])[0]
    assert res.ok and res.rows == [(1,)]
    # callable
    res = suite.exec(lambda db: db.sql("SELECT 2")[0][0], refs=["main"])[0]
    assert res.value == 2


def test_exec_failure_in_script(suite):
    res = suite.exec(["SELECT * FROM nope"], refs=["main"])[0]
    assert res.failed and "nope" in res.error
    stmt_row, exec_row = rows(suite)
    assert stmt_row.status == rslt.OpStatus.FAILED
    assert exec_row.status == rslt.OpStatus.FAILED
    with pytest.raises(Exception):
        suite.exec(["SELECT * FROM nope"], refs=["main"], raise_on_error=True)


def test_exec_per_ref_and_connect_rows(suite):
    suite.branch("b1", "main", timed=False)
    suite.branch("b2", "main", timed=False)
    res = suite.exec(["SELECT count(*) FROM t"], refs=["b1", "b2", "b2"])
    assert [r.ref for r in res] == ["b1", "b2", "b2"]
    assert res[2].connect is None  # already on b2
    connects = rows(suite, rslt.OpType.CONNECT)
    assert [c.ref for c in connects] == ["b1", "b2"]
    assert all(c.exec_id == connects[0].exec_id for c in connects)


def test_exec_multi_unsupported(suite):
    res = suite.exec(["SELECT 1"], refs=["main", "main"], mode="multi")
    assert len(res) == 1 and res[0].unsupported
    row = rows(suite, rslt.OpType.EXEC)[0]
    assert list(row.refs) == ["main", "main"]
    assert not suite.result_collector.workflow_supported


def test_commit_ref_fallback(suite, capsys):
    FakeSuite.SUPPORTS_COMMIT_REFS = False
    try:
        res = suite.exec(["SELECT 1"], refs=["main@deadbeef"])[0]
        assert res.ok and res.ref == "main"
        assert rows(suite, rslt.OpType.EXEC)[0].commit_ref_fallback
        assert "WARNING" in capsys.readouterr().out
    finally:
        FakeSuite.SUPPORTS_COMMIT_REFS = True


def test_untimed_exec_records_nothing(suite):
    suite.exec(["SELECT 1"], refs=["main"], timed=False)
    assert rows(suite) == []


def test_exec_async(suite):
    suite.branch("b1", "main", timed=False)

    async def go():
        await suite.open_async_pool(2)
        r1 = await suite.exec_async(["SELECT count(*) FROM t"], refs=["main"])
        script = "async def run(db):\n    return (await db.sql('SELECT 1'))[0][0]"
        r2 = await suite.exec_async(script, refs=["b1"])
        r3 = await suite.exec_async("db.sql('SELECT 1')", refs=["main"])
        await suite.close_async_pool()
        return r1, r2, r3

    r1, r2, r3 = asyncio.run(go())
    assert r1[0].ok and r1[0].rows == [(1,)]
    assert r2[0].ok and r2[0].value == 1
    assert r3[0].failed and "async def run" in r3[0].error
    ops = [(r.op_type, r.status) for r in rows(suite)]
    assert ops[0] == (rslt.OpType.CONNECT, rslt.OpStatus.OK)
    assert ops[1] == (rslt.OpType.READ, rslt.OpStatus.OK)
    assert ops[2] == (rslt.OpType.EXEC, rslt.OpStatus.OK)
    assert suite.async_pool is None


def test_parquet_output(suite, tmp_path):
    suite.commit("main", "x")
    suite.merge("main", "y")
    suite.exec(["SELECT 1"], refs=["main"])
    suite.result_collector.write_to_parquet()
    df = pq.read_table(os.path.join(tmp_path, "t.parquet")).to_pandas()
    assert set(df["op_name"]) == {"COMMIT", "MERGE", "READ", "EXEC"}
    assert set(df["status"]) == {"OK", "UNSUPPORTED"}
    assert "ref" in df.columns and "exec_id" in df.columns
