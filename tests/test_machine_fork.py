"""machine_fork backend against a fake engine and a fake server: argument
construction, port allocation, commit/reset/delete bookkeeping and the
lazy retirement of fork bases. No VM is started."""

import threading

import pytest

import dblib.machine_fork as mf
from dblib.db_api import Ref
from dblib.result_collector import ResultCollector


class FakeEngine:
    """Records the smolvm verbs it is asked for and tracks fork bases the
    way the engine does: a machine with live clones cannot be deleted."""

    def __init__(self):
        self.calls = []
        self.machines = {}      # name -> {"parent": str|None, "paused": bool}

    def create(self, name, image, port, env, cpus, mem_mib, net):
        self.calls.append(("create", name, image, port, dict(env), cpus, mem_mib, net))
        self.machines[name] = {"parent": None, "paused": False}

    def start_branchable(self, name):
        self.calls.append(("start", name))

    def branch(self, source, child, port):
        self.calls.append(("branch", source, child, port))
        assert source in self.machines, f"branch from unknown machine {source}"
        assert not self.machines[source]["paused"], f"branch from paused machine {source}"
        self.machines[child] = {"parent": source, "paused": False}

    def pause(self, name):
        self.calls.append(("pause", name))
        self.machines[name]["paused"] = True

    def resume(self, name):
        self.calls.append(("resume", name))
        self.machines[name]["paused"] = False

    def delete(self, name, cascade=False):
        self.calls.append(("delete", name, cascade))
        clones = [n for n, m in self.machines.items() if m["parent"] == name]
        if clones and not cascade:
            raise mf.EngineError(f"{name} is the fork base for {len(clones)} clone(s)")
        for c in clones:
            self.delete(c, cascade=True)
        self.machines.pop(name, None)

    def exec_ok(self, name, *cmd):
        return True

    def status(self, name):
        return {"name": name, "state": "paused" if self.machines[name]["paused"] else "running"}

    def data_dir(self, name):
        return ""

    def list_names(self):
        return list(self.machines)


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rows = None
        self.description = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, sql, args=None):
        self.conn.statements.append((sql, args))
        self.rows, self.description = self.conn.server.execute(sql, args)

    def fetchall(self):
        return list(self.rows or [])

    def fetchone(self):
        return (self.rows or [None])[0]


class FakeServer:
    """The _bb_commits table of one machine (copied on fork)."""

    def __init__(self, rows=None):
        self.rows = list(rows or [])

    def copy(self):
        return FakeServer([dict(r) for r in self.rows])

    def execute(self, sql, args):
        s = " ".join(sql.split()).upper()
        if s.startswith("CREATE TABLE"):
            return None, None
        if s.startswith("SELECT COUNT(*) FROM \"_BB_COMMITS\""):
            return [(len(self.rows),)], True
        if s.startswith("INSERT INTO \"_BB_COMMITS\""):
            cols = [c.strip().lower() for c in sql.split("(", 1)[1].split(")", 1)[0].split(",")]
            row = {c: None for c in mf._LOG_COLS}
            row.update(dict(zip(cols, args)))
            if any(r["seq"] == row["seq"] for r in self.rows):
                import psycopg2
                raise psycopg2.IntegrityError("dup seq")
            self.rows.append(row)
            return None, None
        if s.startswith("UPDATE \"_BB_COMMITS\" SET SNAP"):
            snap, seq = args
            for r in self.rows:
                if r["seq"] == seq:
                    r["snap"] = snap
            return None, None
        if s.startswith("DELETE FROM \"_BB_COMMITS\""):
            self.rows = [r for r in self.rows if r["seq"] != args[0]]
            return None, None
        if "FROM \"_BB_COMMITS\" WHERE ID = %S" in s:
            found = sorted((r for r in self.rows if r["id"] == args[0]),
                           key=lambda r: (r["snap"] is not None, r["seq"]), reverse=True)
            return [tuple(r[c] for c in mf._LOG_COLS) for r in found[:1]], True
        if "FROM \"_BB_COMMITS\"" in s:
            rows = sorted(self.rows, key=lambda r: r["seq"], reverse=True)
            if "WHERE SEQ <= %S" in s:
                rows = [r for r in rows if r["seq"] <= args[0]]
            if "LIMIT" in s:
                rows = rows[:int(s.rsplit("LIMIT", 1)[1])]
            return [tuple(r[c] for c in mf._LOG_COLS) for r in rows], True
        if s == "CHECKPOINT":
            return None, None
        raise AssertionError(f"unexpected SQL in test: {sql}")


class FakeConn:
    def __init__(self, server):
        self.server = server
        self.statements = []
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True

    def cancel(self):
        pass


@pytest.fixture
def world(monkeypatch, tmp_path):
    """A run with a golden machine, a fake engine, and fake servers keyed
    by host port (a fork copies the parent's server state)."""
    engine = FakeEngine()
    servers = {}
    ports = {}

    def fake_connect(port, db, timeout=None):
        if port not in servers:
            # A new port: a fork of the machine whose port the engine used
            # as the branch source.
            child = next(c for c in reversed(engine.calls) if c[0] == "branch" and c[3] == port)
            src_port = ports[child[1]]
            servers[port] = servers[src_port].copy()
            ports[child[2]] = port
        return FakeConn(servers[port])

    monkeypatch.setattr(mf, "_connect", fake_connect)
    monkeypatch.setattr(mf, "_port_free", lambda port: True)
    monkeypatch.setattr(mf, "COMMIT_MODE", "machine")
    mf._RUNS.clear()

    st = mf.RunState(db_name="rundb", prefix="bb-test", engine=engine)
    golden = "bb-test-main"
    engine.create(golden, "postgres:18", st.alloc_port(), {}, 2, 1024, True)
    engine.start_branchable(golden)
    gm = mf.Machine(name=golden, port=mf.PORT_BASE, parent=None, kind="golden")
    st.add_machine(gm)
    st.golden = golden
    st.branches["rundb"] = golden
    servers[gm.port] = FakeServer()
    ports[golden] = gm.port
    mf._RUNS["rundb"] = st
    rc = ResultCollector(run_id="t", output_dir=str(tmp_path))
    suite = mf.MachineForkToolSuite(None, rc, "rundb", "rundb", state=st)
    return suite, engine, st, servers, ports


def live(engine, kind_prefix=None):
    return sorted(engine.machines)


def test_capabilities_and_implementation():
    caps = mf.MachineForkToolSuite.capabilities()
    assert caps["branch"] and caps["commit"] and caps["log"] and caps["diff"]
    assert caps["reset"] and caps["delete"] and caps["commit_refs"]
    assert not caps["merge"] and not caps["rebase"] and not caps["revert"]
    assert not caps["multi_ref_exec"] and not caps["exec_async"]
    impl = mf.MachineForkToolSuite.implementation()
    assert impl["branch"] == "native" and impl["merge"] == "unsupported"


def test_branch_forks_the_parent_machine_with_its_own_port(world):
    suite, engine, st, servers, ports = world
    res = suite.branch("feature", "rundb", timed=False)
    assert res.ok, res.error
    forks = [c for c in engine.calls if c[0] == "branch"]
    assert len(forks) == 1
    _, src, child, port = forks[0]
    assert src == "bb-test-main"
    assert child.startswith("bb-test-b") and child.endswith("-feature")
    assert port == mf.PORT_BASE + 1          # the golden took PORT_BASE
    assert st.branches["feature"] == child
    # The child's log records the fork point.
    kinds = [r["kind"] for r in servers[port].rows]
    assert kinds == ["root", "fork"]
    assert suite.list_branches() == ["feature", "rundb"]
    # Branch names are unique.
    assert suite.branch("feature", "rundb", timed=False).failed


def test_root_row_only_once(world):
    suite, engine, st, servers, ports = world
    assert [r["kind"] for r in servers[mf.PORT_BASE].rows] == ["root"]
    rc = suite.result_collector
    mf.MachineForkToolSuite(None, rc, "rundb", "rundb", state=st)   # a second worker
    assert [r["kind"] for r in servers[mf.PORT_BASE].rows] == ["root"]


def test_commit_forks_a_commit_machine_and_records_it(world):
    suite, engine, st, servers, ports = world
    suite.branch("b1", "rundb", timed=False)
    res = suite.commit("b1", "first", timed=False)
    assert res.ok, res.error
    cid = res.value
    bm = st.machines[st.branches["b1"]]
    cm = st.machines[st.commits[("b1", cid)]]
    assert cm.kind == "commit" and cm.parent == bm.name and cm.owner == "b1"
    assert cm.name.startswith("bb-test-c")
    # Both the branch's and the commit machine's logs name the commit machine.
    for port in (bm.port, cm.port):
        head = max(servers[port].rows, key=lambda r: r["seq"])
        assert head["kind"] == "commit" and head["id"] == cid and head["snap"] == cm.name
    # A commit ref connects to the commit machine.
    suite._connect_impl(Ref("b1", cid))
    assert suite._conn_port == cm.port
    log = suite.log("b1", timed=False).value
    assert [e["kind"] for e in log] == ["commit", "fork", "root"]
    # Branching from the commit forks the commit machine.
    assert suite.branch("b2", f"b1@{cid}", timed=False).ok
    assert st.machines[st.branches["b2"]].parent == cm.name


def test_pause_mode_parks_commits_and_resumes_on_use(world, monkeypatch):
    suite, engine, st, servers, ports = world
    monkeypatch.setattr(mf, "COMMIT_MODE", "pause")
    suite.branch("b1", "rundb", timed=False)
    cid = suite.commit("b1", "c", timed=False).value
    cm = st.machines[st.commits[("b1", cid)]]
    assert cm.paused and engine.machines[cm.name]["paused"]
    # A fork from the paused commit resumes it and parks it again.
    assert suite.branch("b2", f"b1@{cid}", timed=False).ok
    verbs = [c[0] for c in engine.calls if c[1] == cm.name or (c[0] == "branch" and c[1] == cm.name)]
    assert verbs[-3:] == ["resume", "branch", "pause"]
    assert cm.paused


def test_reset_rebinds_the_branch_and_retires_the_old_machine(world):
    suite, engine, st, servers, ports = world
    suite.branch("b1", "rundb", timed=False)
    old = st.branches["b1"]
    cid = suite.commit("b1", "c", timed=False).value
    cm_name = st.commits[("b1", cid)]
    res = suite.reset("b1", cid, timed=False)
    assert res.ok, res.error
    new = st.branches["b1"]
    assert new != old and st.machines[new].parent == cm_name
    # The old machine still backs its commit machine: retired, not deleted.
    assert st.machines[old].retired and st.machines[old].alive
    assert old in engine.machines
    assert suite.reset("b1", "nope", timed=False).failed


def test_delete_retires_and_collects_in_dependency_order(world):
    suite, engine, st, servers, ports = world
    suite.branch("b1", "rundb", timed=False)
    cid = suite.commit("b1", "c", timed=False).value
    suite.branch("b2", f"b1@{cid}", timed=False)        # b2 is forked from b1's commit
    b1, c1, b2 = st.branches["b1"], st.commits[("b1", cid)], st.branches["b2"]
    res = suite.delete("b1", timed=False)
    assert res.ok, res.error
    assert "b1" not in st.branches and ("b1", cid) not in st.commits
    # b2 depends on c1, which depends on b1: both stay until b2 goes.
    assert c1 in engine.machines and b1 in engine.machines
    assert st.machines[c1].retired and st.machines[b1].retired
    assert suite.delete("b2", timed=False).ok
    assert b2 not in engine.machines and c1 not in engine.machines and b1 not in engine.machines
    assert sorted(engine.machines) == ["bb-test-main"]
    assert suite.delete("rundb", timed=False).failed       # the default branch stays
    assert suite.delete("ghost", timed=False).failed


def test_ports_are_allocated_once_and_freed_on_delete(world):
    suite, engine, st, servers, ports = world
    suite.branch("b1", "rundb", timed=False)
    p1 = st.machines[st.branches["b1"]].port
    assert p1 in st.ports
    suite.delete("b1", timed=False)
    assert p1 not in st.ports
    suite.branch("b2", "rundb", timed=False)
    assert st.machines[st.branches["b2"]].port != p1 or p1 not in {m.port for m in st.machines.values() if m.alive and m.name != st.branches["b2"]}


def test_concurrent_branches_get_distinct_names_and_ports(world):
    suite, engine, st, servers, ports = world
    rc = suite.result_collector
    suites = [suite] + [mf.MachineForkToolSuite(None, rc, "rundb", "rundb", state=st) for _ in range(3)]
    errors = []

    def go(i):
        try:
            r = suites[i].branch(f"w{i}", "rundb", timed=False)
            if not r.ok:
                errors.append(r.error)
        except Exception as e:   # pragma: no cover
            errors.append(repr(e))

    ts = [threading.Thread(target=go, args=(i,)) for i in range(4)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not errors
    live = [m for m in st.machines.values() if m.alive and m.kind == "branch"]
    assert len({m.name for m in live}) == 4 and len({m.port for m in live}) == 4


def test_cleanup_cascades_from_the_golden(world):
    suite, engine, st, servers, ports = world
    suite.branch("b1", "rundb", timed=False)
    suite.commit("b1", "c", timed=False)
    left = mf.MachineForkToolSuite.cleanup_state(st)
    assert left == 0 and engine.machines == {}
    assert ("delete", "bb-test-main", True) in engine.calls
    assert not any(m.alive for m in st.machines.values())


def test_engine_argument_construction(monkeypatch):
    eng = mf.Engine(binary="smolvm-does-not-exist")
    seen = []

    class P:
        returncode = 0
        stdout = ""
        stderr = ""

    def fake_run(cmd, capture_output, text, timeout):
        seen.append(cmd)
        return P()

    monkeypatch.setattr(mf.subprocess, "run", fake_run)
    eng.create("m", "postgres:18", 55001, {"POSTGRES_PASSWORD": "x"}, 2, 2048, True)
    eng.start_branchable("m")
    eng.branch("m", "m-c", 55002)
    eng.pause("m-c")
    eng.resume("m-c")
    eng.delete("m-c")
    eng.delete("m", cascade=True)
    tails = [c[1:] for c in seen]
    assert tails[0] == ["machine", "create", "--name", "m", "--image", "postgres:18", "-p", "55001:5432",
                        "--cpus", "2", "--mem", "2048", "--net", "-e", "POSTGRES_PASSWORD=x"]
    assert tails[1] == ["machine", "start", "--name", "m", "--branchable"]
    assert tails[2] == ["machine", "branch", "--from", "m", "--name", "m-c", "--branchable", "-p", "55002:5432"]
    assert tails[3] == ["machine", "pause", "--name", "m-c"]
    assert tails[4] == ["machine", "resume", "--name", "m-c"]
    assert tails[5] == ["machine", "delete", "--name", "m-c", "--force"]
    assert tails[6] == ["machine", "delete", "--name", "m", "--cascade"]
    assert "--wait-ready" not in sum(tails, [])


def test_engine_reports_failures(monkeypatch):
    eng = mf.Engine(binary="smolvm-does-not-exist")

    class P:
        returncode = 1
        stdout = ""
        stderr = "Error: vm not found: x"

    monkeypatch.setattr(mf.subprocess, "run", lambda *a, **k: P())
    with pytest.raises(mf.EngineError, match="vm not found"):
        eng.delete("x")


def test_slug_and_names():
    assert mf._slug("task_1_r3") == "task-1-r3"
    assert len(mf._slug("x" * 80)) <= 24
    st = mf.RunState(db_name="d", prefix="bb-ab12cd", engine=None)
    n1 = st.new_machine_name("branch", "Feature/One")
    n2 = st.new_machine_name("commit", "feature-abc123")
    assert n1.startswith("bb-ab12cd-b0001-feature-one") and n2.startswith("bb-ab12cd-c0002-")
    assert n1 != n2 and len(n1) < 64
