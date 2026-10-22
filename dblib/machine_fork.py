"""PostgreSQL backend where every branch is a whole machine.

The run's default branch is a PostgreSQL server inside a smolvm microVM
started as a branch source. A ``branch`` is a copy-on-write fork of the
machine the parent branch lives in: RAM, disk, the running postmaster and
its buffer cache, all at once, in one engine call (``smolvm machine
branch``, about 100-300 ms on the hosts measured so far). The child gets
its own host port, so every branch is an ordinary PostgreSQL server at
127.0.0.1:<port>, with native branch-local performance and nothing of the
"template must be idle" dance plain PostgreSQL clones need.

    branch     smolvm machine branch --from <machine of the parent ref>
               (native); the child is itself branchable
    commit     a row in the branch's _bb_commits table, then a fork of the
               branch machine that stands in for the commit (readable
               through branch@commit, forkable for historical forks and
               resets). MACHINE_FORK_COMMIT_MODE=machine keeps the commit
               machine running (fast, ~120 MB of host memory each);
               =pause pauses it right after the fork (RAM and disk saved to
               the machine's data directory, no VMM process) and resumes
               it when a ref needs it
    log        SELECT from _bb_commits
    diff       per-table row hashes per key bucket on each side, computed
               on the host from two connections (no server-to-server link
               between machines is needed)
    reset      fork a fresh machine from the commit's machine and rebind
               the branch name to it; the old machine is retired
    delete     retire the branch machine and its commit machines
    merge, rebase, revert   unsupported: the SQL three-way machinery of
               pg_merge needs postgres_fdw between the two servers, which
               means guest-to-host networking between machines; not wired
               up yet (see the README)

A machine that other machines were forked from cannot be deleted while
those exist (its disk layers back theirs), so retiring is lazy: a retired
machine is deleted as soon as its last clone is gone, which happens inside
the delete that frees it or at the run's cleanup.

Environment: ``MACHINE_FORK_IMAGE`` (default ``postgres:18``),
``MACHINE_FORK_MEM_MIB`` (2048), ``MACHINE_FORK_CPUS`` (2),
``MACHINE_FORK_PORT_BASE`` (55000; host ports are taken from here up),
``MACHINE_FORK_COMMIT_MODE`` (``machine`` or ``pause``),
``MACHINE_FORK_PG_USER`` (``postgres``), ``MACHINE_FORK_PG_PASSWORD``
(``branchbench``), ``MACHINE_FORK_NET`` (``1``: give the machines outbound
network, needed the first time the image is pulled), ``SMOLVM_BIN`` (the
engine binary, default ``smolvm`` on PATH), ``MACHINE_FORK_CLI_TIMEOUT``
(seconds per engine call, 300).
"""

import hashlib
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

import psycopg2
import psycopg2.extensions
from dotenv import load_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dblib.db_api import DBToolSuite, Ref
from dblib.pg_merge import META_PREFIX, _q, empty_diff
import dblib.result_collector as rc
import dblib.util as dbutil

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

IMAGE = os.environ.get("MACHINE_FORK_IMAGE", "postgres:18")
MEM_MIB = int(os.environ.get("MACHINE_FORK_MEM_MIB", "2048"))
CPUS = int(os.environ.get("MACHINE_FORK_CPUS", "2"))
PORT_BASE = int(os.environ.get("MACHINE_FORK_PORT_BASE", "55000"))
COMMIT_MODE = os.environ.get("MACHINE_FORK_COMMIT_MODE", "machine").lower()
PG_USER = os.environ.get("MACHINE_FORK_PG_USER", "postgres")
PG_PASSWORD = os.environ.get("MACHINE_FORK_PG_PASSWORD", "branchbench")
NET = os.environ.get("MACHINE_FORK_NET", "1") not in ("0", "false", "no", "")
SMOLVM_BIN = os.environ.get("SMOLVM_BIN", "smolvm")
CLI_TIMEOUT = float(os.environ.get("MACHINE_FORK_CLI_TIMEOUT", "300"))
# How long to wait for a freshly started/forked server to accept connections.
READY_TIMEOUT = float(os.environ.get("MACHINE_FORK_READY_TIMEOUT", "240"))
HOST = "127.0.0.1"
GUEST_PG_PORT = 5432

COMMITS_TABLE = META_PREFIX + "commits"
_COMMITS_DDL = f"""
CREATE TABLE IF NOT EXISTS "{COMMITS_TABLE}" (
    seq BIGINT PRIMARY KEY,
    id TEXT NOT NULL,
    kind TEXT NOT NULL,
    message TEXT,
    snap TEXT,
    branch TEXT,
    prev_snap TEXT,
    prev_branch TEXT,
    ts TIMESTAMPTZ DEFAULT now()
)
"""
_LOG_COLS = ("seq", "id", "kind", "message", "snap", "branch", "prev_snap", "prev_branch", "ts")
_BUCKETS = 1024

if COMMIT_MODE not in ("machine", "pause"):
    raise ValueError(f"MACHINE_FORK_COMMIT_MODE must be 'machine' or 'pause', got {COMMIT_MODE!r}")


def _short(name: str, n: int = 8) -> str:
    return hashlib.sha1(name.encode()).hexdigest()[:n]


def _slug(name: str, limit: int = 24) -> str:
    s = re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-") or "x"
    if len(s) > limit:
        s = f"{s[:limit - 9]}-{_short(name)}"
    return s


# ----------------------------------------------------------------------------
# The engine: every smolvm call goes through here (a test swaps it out)
# ----------------------------------------------------------------------------


class EngineError(RuntimeError):
    pass


class Engine:
    """Thin wrapper over the smolvm CLI."""

    def __init__(self, binary: str = SMOLVM_BIN, timeout: float = CLI_TIMEOUT):
        self.binary = self._resolve(binary)
        self.timeout = timeout
        self.calls: list = []

    @staticmethod
    def _resolve(binary: str) -> str:
        found = shutil.which(binary)
        if found:
            return found
        for cand in (os.path.expanduser("~/.local/bin/smolvm"), os.path.expanduser("~/.smolvm/smolvm")):
            if os.path.exists(cand):
                return cand
        return binary

    def run(self, *args: str, timeout: float = None, check: bool = True) -> subprocess.CompletedProcess:
        cmd = [self.binary, "machine", *args]
        self.calls.append(cmd[2:])
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout or self.timeout)
        except subprocess.TimeoutExpired as e:
            raise EngineError(f"{' '.join(cmd[1:])}: timed out after {e.timeout:.0f}s") from e
        if check and p.returncode != 0:
            err = (p.stderr or p.stdout or "").strip().splitlines()
            raise EngineError(f"{' '.join(cmd[1:])}: exit {p.returncode}: {err[-1] if err else ''}")
        return p

    # -- verbs ------------------------------------------------------------

    def create(self, name: str, image: str, port: int, env: dict, cpus: int, mem_mib: int, net: bool) -> None:
        args = ["create", "--name", name, "--image", image, "-p", f"{port}:{GUEST_PG_PORT}",
                "--cpus", str(cpus), "--mem", str(mem_mib)]
        if net:
            args.append("--net")
        for k, v in env.items():
            args += ["-e", f"{k}={v}"]
        self.run(*args)

    def start_branchable(self, name: str) -> None:
        self.run("start", "--name", name, "--branchable")

    def branch(self, source: str, child: str, port: int) -> None:
        self.run("branch", "--from", source, "--name", child, "--branchable",
                 "-p", f"{port}:{GUEST_PG_PORT}")

    def pause(self, name: str) -> None:
        self.run("pause", "--name", name)

    def resume(self, name: str) -> None:
        self.run("resume", "--name", name)

    def delete(self, name: str, cascade: bool = False) -> None:
        args = ["delete", "--name", name]
        args.append("--cascade" if cascade else "--force")
        self.run(*args)

    def exec_ok(self, name: str, *cmd: str) -> bool:
        p = self.run("exec", "--name", name, "--", *cmd, check=False, timeout=60)
        return p.returncode == 0

    def status(self, name: str) -> dict:
        p = self.run("status", "--name", name, "--json", check=False, timeout=60)
        if p.returncode != 0:
            return {}
        try:
            return json.loads(p.stdout)
        except json.JSONDecodeError:
            return {}

    def data_dir(self, name: str) -> str:
        p = self.run("data-dir", "--name", name, check=False, timeout=60)
        return p.stdout.strip() if p.returncode == 0 else ""

    def list_names(self) -> list:
        p = self.run("ls", "--json", check=False, timeout=60)
        if p.returncode != 0:
            return []
        try:
            d = json.loads(p.stdout)
        except json.JSONDecodeError:
            return []
        items = d if isinstance(d, list) else d.get("machines", [])
        return [m.get("name") for m in items if isinstance(m, dict) and m.get("name")]


# ----------------------------------------------------------------------------
# Run-level state: machines, branches, commits (shared by every worker)
# ----------------------------------------------------------------------------


@dataclass
class Machine:
    name: str
    port: int
    parent: Optional[str]
    kind: str                 # "golden" | "branch" | "commit"
    alive: bool = True        # the engine still has it
    retired: bool = False     # delete it as soon as it has no live clones
    deleting: bool = False    # a worker is deleting it right now
    paused: bool = False
    owner: str = ""           # branch name (branch and commit machines)


@dataclass
class RunState:
    db_name: str
    prefix: str
    engine: Engine
    lock: threading.RLock = field(default_factory=threading.RLock)
    machines: dict = field(default_factory=dict)   # machine name -> Machine
    branches: dict = field(default_factory=dict)   # branch name -> machine name
    commits: dict = field(default_factory=dict)    # (branch, commit id) -> machine name
    ports: set = field(default_factory=set)
    data_dirs: dict = field(default_factory=dict)  # machine name -> data directory
    next_port: int = PORT_BASE
    golden: str = ""
    seq: int = 0
    meta_ready: bool = False

    # -- names and ports -------------------------------------------------

    def new_machine_name(self, kind: str, hint: str) -> str:
        self.seq += 1
        tag = {"branch": "b", "commit": "c", "golden": "main"}[kind]
        return f"{self.prefix}-{tag}{self.seq:04x}-{_slug(hint)}"

    def alloc_port(self) -> int:
        with self.lock:
            while True:
                port = self.next_port
                self.next_port += 1
                if port in self.ports or not _port_free(port):
                    continue
                self.ports.add(port)
                return port

    def free_port(self, port: int) -> None:
        with self.lock:
            self.ports.discard(port)

    # -- registry --------------------------------------------------------

    def add_machine(self, m: Machine) -> None:
        with self.lock:
            self.machines[m.name] = m

    def live_children(self, name: str) -> list:
        with self.lock:
            return [m.name for m in self.machines.values() if m.alive and m.parent == name]

    def machine_of(self, ref: Ref) -> Machine:
        with self.lock:
            if ref.commit:
                key = (ref.branch, ref.commit)
                if key not in self.commits:
                    raise ValueError(f"unknown commit '{ref.commit}' on {ref.branch}")
                return self.machines[self.commits[key]]
            if ref.branch not in self.branches:
                raise ValueError(f"unknown branch '{ref.branch}'")
            return self.machines[self.branches[ref.branch]]

    def live_machines(self) -> list:
        with self.lock:
            return [m for m in self.machines.values() if m.alive]


def _port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((HOST, port))
            return True
        except OSError:
            return False


_RUNS: dict = {}          # db_name -> RunState
_LAST_RUN: list = []      # [db_name] of the run set up last
_RUNS_LOCK = threading.Lock()


def _run_state(db_name: str) -> RunState:
    with _RUNS_LOCK:
        st = _RUNS.get(db_name)
    if st is None:
        raise RuntimeError(f"machine_fork run '{db_name}' is not set up (MachineForkInfo first)")
    return st


def _uri(port: int, db: str) -> str:
    return dbutil.format_db_uri(PG_USER, PG_PASSWORD, HOST, port, db)


def _connect(port: int, db: str, timeout: float = READY_TIMEOUT):
    """An autocommit connection to the server behind ``port``, waiting out
    the moment right after a fork or a resume while the forward comes up."""
    deadline = time.monotonic() + timeout
    delay = 0.05
    while True:
        try:
            conn = psycopg2.connect(_uri(port, db), connect_timeout=10)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            return conn
        except psycopg2.OperationalError as e:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"server at {HOST}:{port}/{db} not reachable: {e}") from e
            time.sleep(delay)
            delay = min(1.0, delay * 1.5)


# ----------------------------------------------------------------------------


class MachineForkToolSuite(DBToolSuite):
    BACKEND_NAME = "machine_fork"
    STORAGE_SCOPE = "database"
    SUPPORTS_COMMIT_REFS = True
    SUPPORTS_MULTI_REF_EXEC = False
    # Each ref is an ordinary PostgreSQL server, so the suite's catalog query
    # answers schema_diff; _bb_ bookkeeping tables are skipped by the default.
    SCHEMA_DIALECT = "postgres"

    IMPLEMENTATION = {
        "branch": "native",
        "commit": "composed",
        "diff": "simulated",
        "schema_diff": "simulated",
        "log": "simulated",
        "reset": "composed",
        "delete": "composed",
        "commit_refs": "composed",
    }
    IMPLEMENTATION_NOTES = {
        "branch": "smolvm machine branch: a copy-on-write fork of the parent's whole machine (RAM, disk, "
                  "the running postmaster) with its own host port; the parent keeps running",
        "commit": "_bb_commits row, then a fork of the branch machine as the commit's machine "
                  f"(MACHINE_FORK_COMMIT_MODE={COMMIT_MODE}: a running machine, or one paused right after the "
                  "fork and resumed when a ref needs it)",
        "log": "SELECT from the _bb_commits bookkeeping table",
        "diff": "per-table row hashes per key bucket on each machine, differing buckets fetched from both "
                "servers and compared on the host",
        "schema_diff": "information_schema columns of each ref's server compared on the host",
        "reset": "fork a fresh machine from the commit's machine and rebind the branch to it; the old "
                 "machine is retired (deleted once nothing is forked from it)",
        "delete": "retire the branch machine and its commit machines: each is deleted as soon as no live "
                  "machine is forked from it (the engine keeps a fork base while its clones exist)",
        "commit_refs": "a commit ref connects to the commit's machine (read-only)",
        "merge": "not implemented: pg_merge's SQL three-way merge needs postgres_fdw from one machine's "
                 "server to another's",
    }

    OBSERVED: dict = {}
    _OBS_LOCK = threading.Lock()

    @classmethod
    def observe(cls, key: str, amount=1) -> None:
        with cls._OBS_LOCK:
            cls.OBSERVED[key] = cls.OBSERVED.get(key, 0) + amount

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def get_default_connection_uri(cls) -> str:
        """Admin URI (the ``postgres`` database) of the run set up last."""
        with _RUNS_LOCK:
            st = _RUNS.get(_LAST_RUN[0]) if _LAST_RUN else None
        if st is None:
            raise RuntimeError("machine_fork run is not set up (MachineForkInfo first)")
        return _uri(st.machines[st.golden].port, "postgres")

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        st = _run_state(db_name)
        return _uri(st.machines[st.golden].port, db_name)

    @classmethod
    def init_for_bench(cls, collector: rc.ResultCollector, db_name: str, default_branch_name: str,
                       measure_storage: bool = False):
        return cls(None, collector, db_name, default_branch_name or db_name, measure_storage)

    def __init__(self, connection, collector: rc.ResultCollector, db_name: str,
                 default_branch_name: str, measure_storage: bool = False, state: RunState = None):
        super().__init__(None, collector, measure_storage)
        self.db_name = db_name
        self.default_branch = default_branch_name or db_name
        self.state = state or _run_state(db_name)
        self.engine = self.state.engine
        self._conn_port: Optional[int] = None
        self.conn = self._open(self.state.machine_of(Ref(self.default_branch)))
        self._current_ref = Ref(self.default_branch)
        with self.state.lock:
            if not self.state.meta_ready:
                self._ensure_meta(self.conn, self.default_branch)
                self.state.meta_ready = True

    # ------------------------------------------------------------------
    # Connections
    # ------------------------------------------------------------------

    def _ensure_running(self, m: Machine) -> bool:
        """Resume a paused commit machine before it is read or forked;
        returns whether it had to (the caller may park it again)."""
        with self.state.lock:
            if not m.paused:
                return False
            t0 = time.perf_counter()
            self.engine.resume(m.name)
            m.paused = False
            self.observe("resumes")
            self.observe("resume_sec", round(time.perf_counter() - t0, 3))
            return True

    def _park(self, m: Machine) -> None:
        """Pause a commit machine again (pause mode) once a one-off use of
        it is over, so idle commits hold no VMM process."""
        with self.state.lock:
            if m.paused or not m.alive or m.kind != "commit":
                return
            t0 = time.perf_counter()
            try:
                self.engine.pause(m.name)
            except EngineError as e:
                # The engine refuses to pause a fork source while branches
                # depend on its live state; such a commit stays running (as in
                # `machine` mode) and is paused on a later park, once its
                # children are gone.
                if "depend on its live state" in str(e):
                    self.observe("pause_deferred")
                    return
                raise
            m.paused = True
            self.observe("pauses")
            self.observe("pause_sec", round(time.perf_counter() - t0, 3))

    def _open(self, m: Machine):
        self._ensure_running(m)
        conn = _connect(m.port, self.db_name)
        self._conn_port = m.port
        return conn

    def _close_conn(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None
        self._conn_port = None

    def _connect_impl(self, ref: Ref) -> None:
        m = self.state.machine_of(ref)
        if self.conn is not None and not self.conn.closed and self._conn_port == m.port:
            return
        self._close_conn()
        self.conn = self._open(m)

    def _on(self, ref: Ref) -> Machine:
        """Point self.conn at the branch head of ``ref``; returns its machine."""
        m = self.state.machine_of(ref.head())
        if self._current_ref != ref.head() or self._conn_port != m.port or self.conn is None or self.conn.closed:
            self._close_conn()
            self.conn = self._open(m)
            self._current_ref = ref.head()
        return m

    def _exec(self, conn, sql: str, args=None):
        self._check_cancel()
        with conn.cursor() as cur:
            cur.execute(sql, args)
            if cur.description is not None:
                return cur.fetchall()
            return None

    def _side(self, ref: Ref) -> tuple:
        """(connection, machine, resumed) for the server holding ``ref``;
        release with _release_side."""
        m = self.state.machine_of(ref)
        resumed = self._ensure_running(m)
        return _connect(m.port, self.db_name), m, resumed

    def _release_side(self, side: tuple) -> None:
        conn, m, resumed = side
        try:
            conn.close()
        except Exception:
            pass
        if resumed:
            self._park(m)

    def close_connection(self) -> None:
        self._close_conn()
        super().close_connection()

    def list_branches(self) -> list:
        with self.state.lock:
            return sorted(self.state.branches)

    # ------------------------------------------------------------------
    # Bookkeeping (commit log), the same table file_copy keeps
    # ------------------------------------------------------------------

    @classmethod
    def _ensure_meta(cls, conn, default_branch: str) -> None:
        with conn.cursor() as cur:
            cur.execute(_COMMITS_DDL)
            cur.execute(f'SELECT COUNT(*) FROM "{COMMITS_TABLE}"')
            if not cur.fetchone()[0]:
                seq = time.time_ns()
                cur.execute(f'INSERT INTO "{COMMITS_TABLE}" (seq, id, kind, message, branch) VALUES (%s, %s, %s, %s, %s)',
                            (seq, f"{seq:x}", "root", "initial state", default_branch))

    def _add_row(self, conn, kind: str, message: str, branch: str, snap: str = None,
                 prev_snap: str = None, prev_branch: str = None, commit_id: str = None,
                 seq: int = None) -> tuple:
        seq = seq or time.time_ns()
        commit_id = commit_id or f"{seq:x}"
        for _ in range(16):
            try:
                self._exec(conn, f'INSERT INTO "{COMMITS_TABLE}" (seq, id, kind, message, snap, branch, '
                                 "prev_snap, prev_branch) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                           (seq, commit_id, kind, message or "", snap, branch, prev_snap, prev_branch))
                return commit_id, seq
            except psycopg2.IntegrityError:
                seq += 1
        raise RuntimeError("could not record commit")

    def _log_rows(self, conn, limit: int = None, max_seq: int = None) -> list:
        sql = f'SELECT {", ".join(_LOG_COLS)} FROM "{COMMITS_TABLE}"'
        args = []
        if max_seq is not None:
            sql += " WHERE seq <= %s"
            args.append(int(max_seq))
        sql += " ORDER BY seq DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        rows = self._exec(conn, sql, tuple(args) if args else None) or []
        return [dict(zip(_LOG_COLS, r)) for r in rows]

    def _find_commit(self, conn, commit_id: str) -> dict:
        rows = self._exec(conn, f'SELECT {", ".join(_LOG_COLS)} FROM "{COMMITS_TABLE}" '
                                "WHERE id = %s ORDER BY (snap IS NOT NULL) DESC, seq DESC LIMIT 1",
                          (str(commit_id),))
        if not rows:
            raise ValueError(f"unknown commit '{commit_id}'")
        return dict(zip(_LOG_COLS, rows[0]))

    @staticmethod
    def _state_machine(row: dict):
        if row["kind"] == "commit":
            return row["snap"]
        if row["kind"] == "fork":
            return row["prev_snap"]
        return None

    # ------------------------------------------------------------------
    # Forking machines
    # ------------------------------------------------------------------

    def _fork(self, source: Machine, kind: str, hint: str, owner: str) -> Machine:
        """Fork ``source`` into a new machine of ``kind``; registered alive.
        A paused source is resumed for the fork and parked again after."""
        st = self.state
        resumed = self._ensure_running(source)
        name = st.new_machine_name(kind, hint)
        port = st.alloc_port()
        try:
            self.engine.branch(source.name, name, port)
        except BaseException:
            st.free_port(port)
            raise
        finally:
            if resumed:
                self._park(source)
        m = Machine(name=name, port=port, parent=source.name, kind=kind, owner=owner)
        st.add_machine(m)
        return m

    def _retire(self, m: Machine) -> None:
        """Delete ``m`` now if nothing is forked from it, else as soon as
        that is the case (see _collect)."""
        with self.state.lock:
            m.retired = True
        self._collect()

    def _collect(self) -> int:
        """Delete every retired machine without live clones; repeats while
        a deletion frees another. Returns the number deleted."""
        st = self.state
        deleted = 0
        while True:
            with st.lock:
                # Claim what this pass deletes, so two workers collecting at
                # once do not both delete the same machine.
                ready = [m for m in st.machines.values()
                         if m.alive and m.retired and not m.deleting and not st.live_children(m.name)]
                for m in ready:
                    m.deleting = True
            if not ready:
                return deleted
            this_pass = 0
            for m in ready:
                try:
                    self.engine.delete(m.name)
                except EngineError as e:
                    if "not found" in str(e):
                        # Gone already (another worker, or an earlier cascade).
                        pass
                    else:
                        # Still a fork base for something the registry does
                        # not know about: leave it for a later pass.
                        self.observe("delete_deferred")
                        print(f"Warning: could not delete {m.name} yet: {e}")
                        with st.lock:
                            m.deleting = False
                        continue
                with st.lock:
                    m.alive = False
                    m.deleting = False
                    st.free_port(m.port)
                this_pass += 1
            deleted += this_pass
            if this_pass == 0:
                return deleted

    # ------------------------------------------------------------------
    # Storage
    # ------------------------------------------------------------------

    def _storage_bytes(self) -> int:
        total = 0
        st = self.state
        for m in st.live_machines():
            with st.lock:
                d = st.data_dirs.get(m.name)
            if d is None:
                d = self.engine.data_dir(m.name)
                if d:
                    with st.lock:
                        st.data_dirs[m.name] = d
            if d and os.path.isdir(d):
                total += dbutil.get_directory_size_bytes(d)
        return total

    def _gc_impl(self) -> None:
        self._on(Ref(self.default_branch))
        self._exec(self.conn, "CHECKPOINT")

    # ------------------------------------------------------------------
    # Verbs
    # ------------------------------------------------------------------

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        st = self.state
        with st.lock:
            if name in st.branches:
                raise ValueError(f"Cannot create branch {name}, already exists")
        src = st.machine_of(from_ref)
        # Our own session on the source would be inherited by the child as
        # a backend whose client is gone; close it first (it reopens lazily).
        if self._conn_port == src.port:
            self._close_conn()
            self._current_ref = None
        child = self._fork(src, "branch", name, owner=name)
        conn = _connect(child.port, self.db_name)
        try:
            # The fork point: a commit's machine, or (from a head) the state
            # the child itself holds right now, which is why the row names
            # the child's own machine.
            base = src.name if from_ref.commit else child.name
            self._add_row(conn, "fork", f"fork from {from_ref}", branch=name, prev_snap=base,
                          prev_branch=from_ref.branch)
        finally:
            conn.close()
        with st.lock:
            st.branches[name] = child.name

    def _commit_impl(self, ref: Ref, message: str) -> str:
        st = self.state
        bm = self._on(ref)
        head = self._log_rows(self.conn, limit=1)[0]
        seq = time.time_ns()
        # Record the commit first so the commit machine's own log ends with
        # it; its machine name is filled in once the fork exists.
        commit_id, seq = self._add_row(self.conn, "commit", message, branch=ref.branch,
                                       snap=None, prev_snap=self._state_machine(head),
                                       prev_branch=head["branch"], commit_id=f"{seq:x}", seq=seq)
        self._close_conn()
        self._current_ref = None
        try:
            cm = self._fork(bm, "commit", f"{ref.branch}-{commit_id[-6:]}", owner=ref.branch)
        except BaseException:
            try:
                self.conn = self._open(bm)
                self._current_ref = ref.head()
                self._exec(self.conn, f'DELETE FROM "{COMMITS_TABLE}" WHERE seq = %s', (seq,))
            except Exception:
                pass
            raise
        # Name the machine in both logs (the commit machine already carries
        # the row; fill its snap column there too for log() readers).
        self.conn = self._open(bm)
        self._current_ref = ref.head()
        self._exec(self.conn, f'UPDATE "{COMMITS_TABLE}" SET snap = %s WHERE seq = %s', (cm.name, seq))
        cconn = _connect(cm.port, self.db_name)
        try:
            with cconn.cursor() as cur:
                cur.execute(f'UPDATE "{COMMITS_TABLE}" SET snap = %s WHERE seq = %s', (cm.name, seq))
        finally:
            cconn.close()
        with st.lock:
            st.commits[(ref.branch, commit_id)] = cm.name
        if COMMIT_MODE == "pause":
            t0 = time.perf_counter()
            self.engine.pause(cm.name)
            with st.lock:
                cm.paused = True
            self.observe("pauses")
            self.observe("pause_sec", round(time.perf_counter() - t0, 3))
        return commit_id

    def _log_impl(self, ref: Ref, limit: int) -> list:
        side = self._side(ref)
        conn = side[0]
        try:
            max_seq = int(self._find_commit(conn, ref.commit)["seq"]) if ref.commit else None
            return [{"commit_hash": r["id"], "kind": r["kind"], "committer": "machine_fork", "email": "",
                     "date": r["ts"], "message": r["message"], "snapshot": r["snap"]}
                    for r in self._log_rows(conn, limit=limit, max_seq=max_seq)]
        finally:
            self._release_side(side)

    def _reset_impl(self, ref: Ref, to: str) -> None:
        st = self.state
        bm = self._on(ref)
        row = self._find_commit(self.conn, to)
        with st.lock:
            cname = st.commits.get((ref.branch, to)) or row["snap"]
            cm = st.machines.get(cname) if cname else None
        if cm is None or not cm.alive:
            raise ValueError(f"commit {to} has no machine to reset to ({row['kind']} row)")
        self._close_conn()
        self._current_ref = None
        new = self._fork(cm, "branch", ref.branch, owner=ref.branch)
        with st.lock:
            st.branches[ref.branch] = new.name
        self._retire(bm)

    def _delete_impl(self, ref: Ref) -> None:
        st = self.state
        if ref.branch == self.default_branch:
            raise ValueError("the default branch cannot be deleted")
        with st.lock:
            if ref.branch not in st.branches:
                raise ValueError(f"unknown branch '{ref.branch}'")
            bname = st.branches.pop(ref.branch)
            doomed = [st.machines[bname]] + [m for m in st.machines.values()
                                             if m.alive and m.kind == "commit" and m.owner == ref.branch]
            for m in doomed:
                m.retired = True
            for key in [k for k in st.commits if k[0] == ref.branch]:
                st.commits.pop(key, None)
        if self._current_ref is not None and self._current_ref.branch == ref.branch:
            self._close_conn()
            self._current_ref = None
        self._collect()

    # -- diff: computed on the host from two connections -----------------

    def _tables(self, conn) -> list:
        rows = self._exec(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "
                                "AND table_type = 'BASE TABLE' AND table_name NOT LIKE %s ORDER BY table_name",
                          (META_PREFIX.replace("_", r"\_") + "%",))
        return [r[0] for r in rows or []]

    def _columns(self, conn, table: str) -> list:
        rows = self._exec(conn, "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' "
                                "AND table_name = %s ORDER BY ordinal_position", (table,))
        return [r[0] for r in rows or []]

    def _pk(self, conn, table: str) -> list:
        rows = self._exec(conn, "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
                                "AND a.attnum = ANY(i.indkey) WHERE i.indrelid = %s::regclass AND i.indisprimary "
                                "ORDER BY array_position(i.indkey, a.attnum)", (f'public."{table}"',))
        return [r[0] for r in rows or []]

    def _count(self, conn, table: str) -> int:
        return int(self._exec(conn, f"SELECT COUNT(*) FROM {_q(table)}")[0][0])

    @staticmethod
    def _bucket_expr(pk: list) -> str:
        key = " || '|' || ".join(f"COALESCE({_q(c)}::text, '')" for c in pk)
        return f"(hashtext({key}) & {_BUCKETS - 1})"

    @staticmethod
    def _row_hash(cols: list) -> str:
        return "md5(ROW(" + ", ".join(_q(c) for c in cols) + ")::text)"

    def _bucket_hashes(self, conn, table: str, bexpr: str, cols: list) -> dict:
        rows = self._exec(conn, f"SELECT bb, md5(string_agg(h, ',' ORDER BY h)) FROM "
                                f"(SELECT {bexpr} AS bb, {self._row_hash(cols)} AS h FROM {_q(table)}) s "
                                f"GROUP BY bb") or []
        return {int(b): h for b, h in rows}

    def _rows_in_buckets(self, conn, table: str, bexpr: str, pk: list, cols: list, buckets: list) -> dict:
        rows = self._exec(conn, f"SELECT {', '.join(_q(c) for c in pk)}, {self._row_hash(cols)} FROM {_q(table)} "
                                f"WHERE {bexpr} = ANY(%s)", (buckets,)) or []
        return {tuple(r[:-1]): r[-1] for r in rows}

    def _multiset_hashes(self, conn, table: str, cols: list) -> dict:
        rows = self._exec(conn, f"SELECT {self._row_hash(cols)} AS h, COUNT(*) FROM {_q(table)} GROUP BY 1") or []
        return {h: int(n) for h, n in rows}

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        ma, mb = self.state.machine_of(ref_a), self.state.machine_of(ref_b)
        if ma.name == mb.name:
            return empty_diff()
        sa = self._side(ref_a)
        try:
            sb = self._side(ref_b)
        except BaseException:
            self._release_side(sa)
            raise
        ca, cb = sa[0], sb[0]
        try:
            ta, tb = set(self._tables(ca)), set(self._tables(cb))
            out = []
            for t in sorted(ta | tb):
                if t in ta and t not in tb:
                    out.append({"table_name": t, "rows_added": 0, "rows_deleted": self._count(ca, t), "rows_modified": 0})
                    continue
                if t in tb and t not in ta:
                    out.append({"table_name": t, "rows_added": self._count(cb, t), "rows_deleted": 0, "rows_modified": 0})
                    continue
                cols_b = self._columns(cb, t)
                cols_a = set(self._columns(ca, t))
                cols = [c for c in cols_b if c in cols_a]
                if not cols:
                    continue
                pk = [c for c in self._pk(cb, t) if c in cols]
                if not pk:
                    ha, hb = self._multiset_hashes(ca, t, cols), self._multiset_hashes(cb, t, cols)
                    added = sum(max(0, hb.get(h, 0) - ha.get(h, 0)) for h in hb)
                    deleted = sum(max(0, ha.get(h, 0) - hb.get(h, 0)) for h in ha)
                    if added or deleted:
                        out.append({"table_name": t, "rows_added": added, "rows_deleted": deleted, "rows_modified": 0})
                    continue
                bexpr = self._bucket_expr(pk)
                ba, bb = self._bucket_hashes(ca, t, bexpr, cols), self._bucket_hashes(cb, t, bexpr, cols)
                changed = sorted(b for b in set(ba) | set(bb) if ba.get(b) != bb.get(b))
                if not changed:
                    continue
                ra = self._rows_in_buckets(ca, t, bexpr, pk, cols, changed)
                rb = self._rows_in_buckets(cb, t, bexpr, pk, cols, changed)
                added = sum(1 for k in rb if k not in ra)
                deleted = sum(1 for k in ra if k not in rb)
                modified = sum(1 for k in rb if k in ra and ra[k] != rb[k])
                out.append({"table_name": t, "rows_added": added, "rows_deleted": deleted, "rows_modified": modified})
            return {"tables": out, "rows_added": sum(x["rows_added"] for x in out),
                    "rows_deleted": sum(x["rows_deleted"] for x in out),
                    "rows_modified": sum(x["rows_modified"] for x in out)}
        finally:
            self._release_side(sa)
            self._release_side(sb)

    # ------------------------------------------------------------------
    # Run setup / cleanup (used by the runners)
    # ------------------------------------------------------------------

    class MachineForkInfo:
        """Run-level state: creates the golden machine with the run's
        database and the bookkeeping table; ``cleanup`` removes every
        machine of the run."""

        def __init__(self, db_name: str, engine: Engine = None, image: str = IMAGE,
                     mem_mib: int = MEM_MIB, cpus: int = CPUS, net: bool = NET):
            self.db_name = db_name
            self.engine = engine or Engine()
            prefix = f"bb-{secrets.token_hex(3)}"
            self.state = RunState(db_name=db_name, prefix=prefix, engine=self.engine)
            self._remove_stale(db_name)
            golden = f"{prefix}-main-{_slug(db_name, 20)}"
            port = self.state.alloc_port()
            t0 = time.monotonic()
            self.engine.create(golden, image, port, {"POSTGRES_PASSWORD": PG_PASSWORD, "POSTGRES_USER": PG_USER},
                               cpus, mem_mib, net)
            self.engine.start_branchable(golden)
            self.state.add_machine(Machine(name=golden, port=port, parent=None, kind="golden"))
            self.state.golden = golden
            # The server is ready once the admin database answers (the
            # official image runs a socket-only server first, while it
            # initialises the cluster).
            _connect(port, "postgres").close()
            # The runner creates the run's database through the admin URI
            # (BackendManager._create_database); the first suite adds the
            # bookkeeping table.
            self.state.branches[db_name] = golden
            with _RUNS_LOCK:
                _RUNS[db_name] = self.state
                _LAST_RUN[:] = [db_name]
            print(f"machine_fork: golden machine {golden} ({image}, {mem_mib} MiB, {cpus} vCPU) ready at "
                  f"{HOST}:{port} in {time.monotonic() - t0:.1f}s; commits are "
                  f"{'paused forks' if COMMIT_MODE == 'pause' else 'running forks'}")

        @staticmethod
        def _remove_stale(db_name: str) -> None:
            with _RUNS_LOCK:
                old = _RUNS.pop(db_name, None)
            if old is not None:
                try:
                    MachineForkToolSuite.cleanup_state(old)
                    print(f"Removed the machines of an earlier '{db_name}' run")
                except Exception as e:
                    print(f"Warning: could not remove earlier machines: {e}")

        @property
        def default_branch_name(self) -> str:
            return self.db_name

        @property
        def golden_port(self) -> int:
            return self.state.machines[self.state.golden].port

    @classmethod
    def cleanup_state(cls, st: RunState) -> int:
        """Delete every machine of the run (the golden with --cascade takes
        its descendants with it); returns how many the engine still listed."""
        names = {m.name for m in st.machines.values()}
        if st.golden and st.machines.get(st.golden, Machine("", 0, None, "golden")).alive:
            try:
                st.engine.delete(st.golden, cascade=True)
            except EngineError as e:
                print(f"Warning: cascade delete of {st.golden} failed: {e}")
        left = [n for n in st.engine.list_names() if n in names]
        for n in left:
            try:
                st.engine.delete(n, cascade=True)
            except EngineError as e:
                print(f"Warning: could not delete {n}: {e}")
        left = [n for n in st.engine.list_names() if n in names]
        with st.lock:
            for m in st.machines.values():
                m.alive = m.name in left
            st.ports = {m.port for m in st.machines.values() if m.alive}
        with _RUNS_LOCK:
            if _RUNS.get(st.db_name) is st:
                _RUNS.pop(st.db_name, None)
        return len(left)

    @classmethod
    def cleanup(cls, info) -> None:
        try:
            n = len(info.state.machines)
            left = cls.cleanup_state(info.state)
            print(f"machine_fork run '{info.db_name}': {n - left} of {n} machines deleted"
                  + (f", {left} left behind" if left else "") + ".")
        except Exception as e:
            print(f"Error deleting machines: {e}")
