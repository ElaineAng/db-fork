"""Neon backend (Lakebase Postgres on Neon).

Branches are Neon branches, each with one read-write compute; the benchmark
runs in a project of its own (``BackendManager`` creates and deletes it).
A branch is its own Postgres endpoint, so connecting to one means opening
a new connection.

Neon versions storage but has no commits, diff or merge. The verbs the
API lacks are built from what it does have:

* branching at a point in time (``parent_lsn``) and ``restore`` to an LSN;
* ``postgres_fdw``, with which one branch's compute can read another's
  tables, and plain Postgres on each compute.

    branch     POST /branches with parent_id (+ parent_lsn for a commit
               ref) and a read-write compute; the verb waits for Neon's
               operations to finish, then writes a fork row on the branch
    commit     a row in the branch's _bb_commits table plus the branch's
               pg_current_wal_flush_lsn() as the commit's point in time
    log        SELECT FROM _bb_commits
    diff       per-table, per-key-bucket row hashes on each side (local
               to each compute); only the differing buckets are pulled over
               postgres_fdw into temp tables and compared locally
    merge      SQL three-way merge on the target's compute: base = a
               temporary branch at the source's fork LSN (created and
               timed inside the verb), theirs = the source branch over
               postgres_fdw; only the key buckets theirs changed are
               pulled into temp tables; conflicts per on_conflict as on
               the other backends
    rebase     the same three-way merge of the upstream into the branch,
               after which a rebase row moves the branch's base to the
               upstream's LSN so the merge back only sees newer changes
    revert     temporary branches at the commit's LSN and its predecessor,
               inverse delta applied locally
    reset      POST /branches/{id}/restore to the commit's LSN (native;
               Neon keeps the pre-restore state in a backup branch that
               cannot be deleted before the project)
    delete     DELETE /branches/{id} (a branch with children is deferred
               and retried once they are gone)

Quotas are treated as capacity. Neon counts *active computes* per project
(20 on the Launch plan, the default branch exempt), not branches. The
process keeps a registry of the computes it activated and, before it
needs one more above ``NEON_ACTIVE_BUDGET``, suspends the least recently
used compute no connection is using; a suspended compute resumes on the
next connect, which lands in CONNECT. All API calls go
through one process-wide token bucket sized under the documented 700
requests/minute, with waits recorded as API_RETRY_WAIT rows; 429/423/503
responses are retried as the backstop.

Multi-branch queries (``exec(mode="multi")``) are declared unsupported:
Neon has no multi-branch query semantics, and routing the scenarios'
cross-branch reads over postgres_fdw would be slower than running them on
each branch, which the scenarios do when the flag is off.
"""

import os
import random
import re
import threading
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dblib.db_api import DBToolSuite, Ref, Session
from dblib import result_pb2 as rslt
import dblib.result_collector as rc

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))
API_KEY = os.environ.get("NEON_API_KEY_ORG", "")
NEON_API_BASE_URL = "https://console.neon.tech/api/v2/"
NEON_REGION = os.environ.get("NEON_REGION", "aws-us-east-1")
NEON_PG_VERSION = int(os.environ.get("NEON_PG_VERSION", "17"))
# Fixed compute size for every endpoint (min = max), in CU.
NEON_COMPUTE_CU = float(os.environ.get("NEON_COMPUTE_CU", "2"))
# How long Neon keeps history (commit refs are LSNs inside this window).
NEON_HISTORY_RETENTION_SEC = int(os.environ.get("NEON_HISTORY_RETENTION_SEC", str(2 * 86400)))
# Scale-to-zero of branch computes: -1 (never) by default. Neon's
# autosuspend counts only running statements as activity, not open
# connections: with the plan default (5 min on Launch, its minimum) a
# branch compute that a worker holds but has not queried for 5 minutes
# (waiting for a lock, in a quiet review phase, or read over postgres_fdw
# by a merge that is busy elsewhere) is suspended under its connections,
# which then fail with "SSL connection has been closed unexpectedly".
# The active-compute budget below suspends computes explicitly instead.
NEON_BRANCH_SUSPEND_TIMEOUT_SEC = int(os.environ.get("NEON_BRANCH_SUSPEND_TIMEOUT_SEC", "-1"))
# Active computes the process keeps at most (the plan allows 20 besides the
# default branch): when a new compute is needed above this, the least
# recently used idle one is suspended first.
NEON_ACTIVE_BUDGET = int(os.environ.get("NEON_ACTIVE_BUDGET", "18"))
# Also suspend a branch's compute whenever the connection moves off it
# (off by default: the budget above only suspends when room is needed).
NEON_SUSPEND_ON_SWITCH = os.environ.get("NEON_SUSPEND_ON_SWITCH", "0").lower() in ("1", "true", "yes")
# Token bucket for the API: requests per minute and burst.
NEON_API_RATE_PER_MIN = float(os.environ.get("NEON_API_RATE_PER_MIN", "600"))
NEON_API_BURST = int(os.environ.get("NEON_API_BURST", "20"))
# Projects whose name starts with this are the benchmark's and are deleted
# before a run (stale ones from interrupted runs) and after it.
PROJECT_PREFIX = "project_macro_"

_LSN_RE = re.compile(r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$")
_OP_POLL_SEC = 0.5
_OP_TIMEOUT_SEC = 600
_ACTIVE_LIMIT_MSG = "concurrently active endpoints"

META_PREFIX = "_bb_"
COMMITS_TABLE = "_bb_commits"
_COMMITS_DDL = f"""
CREATE TABLE IF NOT EXISTS "{COMMITS_TABLE}" (
    seq BIGINT PRIMARY KEY,
    id TEXT NOT NULL,
    kind TEXT NOT NULL,
    message TEXT,
    lsn TEXT,
    branch_id TEXT,
    prev_lsn TEXT,
    prev_branch_id TEXT,
    ts TIMESTAMPTZ DEFAULT now()
)
"""
_CONFLICT_ROW_LIMIT = 1000


def lsn_to_int(lsn: str) -> int:
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


# ----------------------------------------------------------------------------
# API plumbing: token bucket, retries, operations
# ----------------------------------------------------------------------------


class _TokenBucket:
    """Process-wide limiter: tokens refill at ``rate`` per second up to
    ``burst``; acquire() blocks until one is available and returns the
    seconds waited."""

    def __init__(self, rate_per_sec: float, burst: int):
        self.rate = rate_per_sec
        self.burst = burst
        self.tokens = float(burst)
        self.updated = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> float:
        waited = 0.0
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.burst, self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return waited
                need = (1 - self.tokens) / self.rate
            time.sleep(need)
            waited += need


_BUCKET = _TokenBucket(NEON_API_RATE_PER_MIN / 60.0, NEON_API_BURST)


class NeonAPIError(RuntimeError):
    def __init__(self, status: int, text: str):
        super().__init__(f"HTTP {status}: {text[:300]}")
        self.status = status
        self.text = text


def api_request(method: str, endpoint: str, record=None, max_retries: int = 8, **kwargs):
    """One Neon API call through the token bucket, retrying 408/423/429/5xx
    (and connection errors) with Retry-After or jittered backoff.
    ``record(seconds)`` is told about every wait."""
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {API_KEY}"
    headers["Accept"] = "application/json"
    headers["Content-Type"] = "application/json"
    kwargs.setdefault("timeout", 60)
    for attempt in range(max_retries):
        waited = _BUCKET.acquire()
        if waited:
            NeonToolSuite.observe("api_bucket_wait_sec", waited)
        try:
            r = requests.request(method, NEON_API_BASE_URL + endpoint, headers=headers, **kwargs)
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            delay = 0.5 * (2 ** attempt) * (0.5 + random.random())
            if record:
                record(delay)
            time.sleep(delay)
            continue
        if r.status_code < 400:
            if r.status_code == 204 or not r.content:
                return {}
            return r.json()
        if r.status_code in (408, 423, 429, 500, 502, 503, 504) and attempt < max_retries - 1:
            retry_after = r.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after else 0.5 * (2 ** attempt)
            except ValueError:
                delay = 0.5 * (2 ** attempt)
            delay *= 0.5 + random.random()
            if record:
                record(delay)
            time.sleep(delay)
            continue
        raise NeonAPIError(r.status_code, r.text)
    raise NeonAPIError(0, "retries exhausted")


def wait_operations(project_id: str, operations, record=None, timeout: float = _OP_TIMEOUT_SEC) -> float:
    """Poll each operation until it finishes; returns seconds spent."""
    t0 = time.perf_counter()
    for op in operations or []:
        op_id = op.get("id") if isinstance(op, dict) else op
        if not op_id:
            continue
        if isinstance(op, dict) and op.get("status") == "finished":
            continue
        while True:
            o = api_request("GET", f"projects/{project_id}/operations/{op_id}", record=record)["operation"]
            status = o.get("status")
            if status == "finished":
                break
            if status in ("failed", "error", "cancelled", "skipped"):
                raise RuntimeError(f"Neon operation {o.get('action')} {status}: {o.get('error')}")
            if time.perf_counter() - t0 > timeout:
                raise RuntimeError(f"Neon operation {o.get('action')} did not finish in {timeout}s")
            time.sleep(_OP_POLL_SEC)
    return time.perf_counter() - t0


def _endpoint_spec(suspend_timeout: int) -> dict:
    return {
        "type": "read_write",
        "autoscaling_limit_min_cu": NEON_COMPUTE_CU,
        "autoscaling_limit_max_cu": NEON_COMPUTE_CU,
        "suspend_timeout_seconds": suspend_timeout,
    }


# ----------------------------------------------------------------------------
# Project helpers (BackendManager)
# ----------------------------------------------------------------------------


def create_project(project_name: str) -> dict:
    """A project with a fixed-size, never-suspending default compute and
    a history window long enough for a run's commit refs."""
    body = {
        "project": {
            "name": project_name,
            "pg_version": NEON_PG_VERSION,
            "region_id": NEON_REGION,
            "history_retention_seconds": NEON_HISTORY_RETENTION_SEC,
            "default_endpoint_settings": {
                "autoscaling_limit_min_cu": NEON_COMPUTE_CU,
                "autoscaling_limit_max_cu": NEON_COMPUTE_CU,
                "suspend_timeout_seconds": -1,
            },
        }
    }
    resp = api_request("POST", "projects", json=body)
    wait_operations(resp["project"]["id"], resp.get("operations"))
    return resp


def delete_project(project_id: str) -> None:
    api_request("DELETE", f"projects/{project_id}")
    print(f"Neon project {project_id} deleted.")


def list_projects() -> list:
    out, cursor = [], None
    while True:
        q = "projects?limit=100" + (f"&cursor={cursor}" if cursor else "")
        resp = api_request("GET", q)
        out.extend(resp.get("projects", []))
        cursor = (resp.get("pagination") or {}).get("cursor")
        if not cursor or not resp.get("projects"):
            break
    return out


def delete_stale_projects(prefix: str = PROJECT_PREFIX) -> list:
    """Delete every project of this org whose name starts with ``prefix``
    (leftovers of interrupted runs)."""
    deleted = []
    for p in list_projects():
        if str(p.get("name", "")).startswith(prefix):
            try:
                delete_project(p["id"])
                deleted.append(p["name"])
            except Exception as e:
                print(f"Warning: could not delete stale Neon project {p['name']}: {e}")
    return deleted


def connection_uri(project_id: str, branch_id: str, db_name: str, role: str = "neondb_owner",
                   record=None, max_retries: int = 20) -> str:
    """Connection URI of a branch; 404 (branch not visible yet) is retried."""
    # Direct (unpooled) endpoint: the pooler runs PgBouncer in transaction
    # mode, where session state (temp tables, the search_path postgres_fdw
    # sets on its remote sessions) leaks between clients.
    endpoint = (f"projects/{project_id}/connection_uri?branch_id={branch_id}"
                f"&database_name={db_name}&role_name={role}&pooled=false")
    for attempt in range(max_retries):
        try:
            return api_request("GET", endpoint, record=record)["uri"]
        except NeonAPIError as e:
            if e.status == 404 and attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            raise
    raise RuntimeError("connection URI unavailable")


def get_project_branches(project_id: str) -> dict:
    return api_request("GET", f"projects/{project_id}/branches")


# ----------------------------------------------------------------------------


class _State:
    """A ref resolved to a branch (id, name) and, for a commit, its LSN."""

    __slots__ = ("branch", "branch_id", "lsn", "seq")

    def __init__(self, branch: str, branch_id: str, lsn: str = None, seq: int = None):
        self.branch = branch
        self.branch_id = branch_id
        self.lsn = lsn
        self.seq = seq


class NeonToolSuite(DBToolSuite):
    BACKEND_NAME = "neon"
    STORAGE_SCOPE = "default-branch"
    SUPPORTS_COMMIT_REFS = True
    SUPPORTS_MULTI_REF_EXEC = False
    IMPLEMENTATION = {
        "branch": "native",
        "commit": "composed",
        "diff": "simulated",
        "log": "simulated",
        "merge": "simulated",
        "rebase": "simulated",
        "revert": "simulated",
        "reset": "native",
        "delete": "native",
        "commit_refs": "native",
        "multi_ref_exec": "unsupported",
    }
    IMPLEMENTATION_NOTES = {
        "branch": "POST /branches with parent_id (+ parent_lsn for a commit) and a 2 CU compute "
                  "that never auto-suspends, waiting for the operations; computes are suspended "
                  "by the process's active-compute budget (LRU) and on delete (CONNECT carries "
                  "the resume)",
        "commit": "_bb_commits row + pg_current_wal_flush_lsn() as the point in time",
        "log": "SELECT from the _bb_commits bookkeeping table",
        "diff": "per-table row hashes on each compute, differing tables pulled over postgres_fdw "
                "and compared locally",
        "merge": "SQL three-way merge on the target's compute; base = temporary branch at the "
                 "fork LSN (created inside the verb), theirs pulled over postgres_fdw",
        "rebase": "three-way merge of the upstream into the branch; the base moves to the "
                  "upstream's LSN",
        "revert": "temporary branches at the commit's LSN and its predecessor; inverse delta "
                  "applied locally",
        "reset": "POST /branches/{id}/restore to the commit's LSN (preserve_under_name leaves an "
                 "undeletable backup branch)",
        "delete": "DELETE /branches/{id}; a branch with children is deferred until they are gone",
        "commit_refs": "parent_lsn / restore to LSN; exec on a commit ref uses a temporary branch",
        "multi_ref_exec": "not provided: no multi-branch query semantics; the scenarios fall back "
                          "to running cross-branch reads once per branch",
    }

    # Facts observed during a run (shared by every suite of the process):
    # throttling, compute-limit waits, deferred deletes.
    OBSERVED: dict = {}
    _OBS_LOCK = threading.Lock()
    # Computes this process activated: branch id -> last use (monotonic),
    # and how many open connections each one has. Shared by every suite.
    _ACTIVE: dict = {}
    _HOLDS: dict = {}
    _ACT_LOCK = threading.Lock()

    @classmethod
    def observe(cls, key: str, amount=1) -> None:
        with cls._OBS_LOCK:
            cls.OBSERVED[key] = cls.OBSERVED.get(key, 0) + amount

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return f"neon://{NEON_REGION}"

    @classmethod
    def create_neon_project(cls, project_name: str) -> dict:
        return create_project(project_name)

    @classmethod
    def delete_project(cls, project_id: str, timeout: int = 60) -> None:
        try:
            delete_project(project_id)
        except Exception as e:
            print(f"Warning: Neon project deletion failed: {e}")

    @classmethod
    def get_project_branches(cls, project_id: str) -> dict:
        return get_project_branches(project_id)

    @classmethod
    def _get_neon_connection_uri(cls, project_id: str, branch_id: str, db_name: str) -> str:
        return connection_uri(project_id, branch_id, db_name)

    @classmethod
    def init_for_bench(cls, result_collector: rc.ResultCollector, project_id: str,
                       branch_id: str, branch_name: str, database_name: str,
                       measure_storage: bool = False):
        return cls(None, result_collector, project_id, branch_name, branch_id,
                   database_name, measure_storage)

    def __init__(self, connection, result_collector: rc.ResultCollector, project_id: str,
                 branch_name: str, branch_id: str, database_name: str = None,
                 measure_storage: bool = False):
        super().__init__(connection, result_collector, measure_storage)
        self.project_id = project_id
        self.db_name = database_name
        self.default_branch = branch_name
        self._branches: dict = {}        # name -> {"id", "parent_id", "parent_lsn"}
        self._uris: dict = {}            # branch id -> connection uri
        self._endpoints: dict = {}       # branch id -> endpoint id
        self._temp: dict = {}            # (branch id, lsn) -> temp branch name
        self._deferred_deletes: list = []
        self._fdw_ready: set = set()     # branch ids whose db has postgres_fdw
        self._conn_branch_id = None      # branch whose compute self.conn is on
        self._register(branch_name, branch_id, None, None)
        self._ensure_meta(branch_name)
        if connection is None:
            self._connect_impl(Ref(branch_name))
            self._current_ref = Ref(branch_name)

    # ------------------------------------------------------------------
    # API helpers that record waits on this suite's collector
    # ------------------------------------------------------------------

    def _wait_row(self, seconds: float, label: str = "api_wait") -> None:
        self.observe(f"{label}_sec", seconds)
        self.observe(f"{label}_count", 1)
        now = time.time()
        self.result_collector.emit(
            rslt.OpType.API_RETRY_WAIT, status=rslt.OpStatus.OK, latency=seconds,
            start_time=now - seconds, end_time=now, ref="", label=label, num_keys_touched=0)

    def _api(self, method: str, endpoint: str, **kwargs):
        return api_request(method, endpoint, record=self._wait_row, **kwargs)

    def _wait(self, operations) -> float:
        return wait_operations(self.project_id, operations, record=self._wait_row)

    # ------------------------------------------------------------------
    # Branch registry
    # ------------------------------------------------------------------

    def _register(self, name: str, branch_id: str, parent_id, parent_lsn) -> None:
        self._branches[name] = {"id": branch_id, "parent_id": parent_id, "parent_lsn": parent_lsn}

    def _refresh_branches(self) -> None:
        resp = self._api("GET", f"projects/{self.project_id}/branches")
        for b in resp.get("branches", []):
            self._register(b["name"], b["id"], b.get("parent_id"), b.get("parent_lsn"))

    def _branch_info(self, name: str) -> dict:
        info = self._branches.get(name)
        if info is None:
            self._refresh_branches()
            info = self._branches.get(name)
        if info is None:
            raise ValueError(f"Branch '{name}' does not exist")
        return info

    def _branch_id(self, name: str) -> str:
        return self._branch_info(name)["id"]

    def _uri(self, branch_id: str) -> str:
        uri = self._uris.get(branch_id)
        if not uri:
            uri = connection_uri(self.project_id, branch_id, self.db_name, record=self._wait_row)
            self._uris[branch_id] = uri
        return uri

    def _endpoint_id(self, branch_id: str):
        ep = self._endpoints.get(branch_id)
        if ep is None:
            resp = self._api("GET", f"projects/{self.project_id}/branches/{branch_id}/endpoints")
            for e in resp.get("endpoints", []):
                if e.get("type") == "read_write":
                    ep = e["id"]
                    break
            self._endpoints[branch_id] = ep or ""
        return ep or None

    def _suspend(self, branch_id: str) -> None:
        """Suspend a branch's compute (never the default branch's)."""
        if branch_id == self._branch_id(self.default_branch):
            return
        ep = self._endpoint_id(branch_id)
        if not ep:
            return
        with type(self)._ACT_LOCK:
            type(self)._ACTIVE.pop(branch_id, None)
        try:
            self._api("POST", f"projects/{self.project_id}/endpoints/{ep}/suspend")
        except NeonAPIError as e:
            if e.status not in (409, 422, 423):   # already suspended / in transition
                raise

    def _make_room(self, target: str) -> None:
        """Keep the process under NEON_ACTIVE_BUDGET active computes: before
        ``target`` is (re)activated, suspend least-recently-used computes
        that no connection holds. The default branch is exempt."""
        default_id = self._branches[self.default_branch]["id"]
        if target == default_id:
            return
        cls = type(self)
        with cls._ACT_LOCK:
            if target in cls._ACTIVE:
                cls._ACTIVE[target] = time.monotonic()
                return
            victims = []
            now = time.monotonic()
            while len(cls._ACTIVE) + 1 > NEON_ACTIVE_BUDGET:
                # Only computes nobody holds and nobody touched recently: a
                # worker switching branches releases one for a moment.
                idle = [b for b in cls._ACTIVE if cls._HOLDS.get(b, 0) <= 0 and b != default_id
                        and now - cls._ACTIVE[b] > 20.0]
                if not idle:
                    break
                lru = min(idle, key=lambda b: cls._ACTIVE[b])
                del cls._ACTIVE[lru]
                victims.append(lru)
            cls._ACTIVE[target] = time.monotonic()
        for b in victims:
            try:
                self._suspend(b)
                self.observe("lru_suspends")
            except Exception as e:
                print(f"Warning: could not suspend compute of {b}: {e}")

    def _reserve(self, branch_id: str, n: int = 1) -> None:
        """Hold (or release, n=-1) a compute without a connection, so the
        LRU never suspends it mid-verb."""
        cls = type(self)
        with cls._ACT_LOCK:
            cls._HOLDS[branch_id] = max(0, cls._HOLDS.get(branch_id, 0) + n)

    def _close(self, conn) -> None:
        """Close a connection opened by _open and release its hold."""
        if conn is None:
            return
        bid = getattr(self, "_conn_owner", {}).pop(id(conn), None)
        try:
            conn.close()
        except Exception:
            pass
        if bid:
            with type(self)._ACT_LOCK:
                type(self)._HOLDS[bid] = max(0, type(self)._HOLDS.get(bid, 0) - 1)

    def _open(self, branch_id: str, max_wait: float = 300.0):
        """A new autocommit connection to the branch's compute, counted in
        the process's active-compute registry; a connect refused for the
        active-compute limit (or failing while the compute changes state)
        is retried, with the waits recorded."""
        uri = self._uri(branch_id)
        self._make_room(branch_id)
        t0 = time.perf_counter()
        attempt = 0
        while True:
            try:
                conn = psycopg2.connect(uri, connect_timeout=30)
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                break
            except psycopg2.OperationalError as e:
                msg = str(e)
                attempt += 1
                if time.perf_counter() - t0 > max_wait:
                    raise
                if _ACTIVE_LIMIT_MSG in msg:
                    self.observe("active_compute_limit_hits")
                    delay = 2.0 + random.random()
                    label = "compute_wait"
                else:
                    delay = min(3.0, 0.5 * attempt)
                    label = "connect_retry"
                    self.observe("connect_error: " + " ".join(msg.split())[:70])
                self._wait_row(delay, label)
                time.sleep(delay)
        cls = type(self)
        with cls._ACT_LOCK:
            if branch_id != self._branches[self.default_branch]["id"]:
                cls._ACTIVE[branch_id] = time.monotonic()
            cls._HOLDS[branch_id] = cls._HOLDS.get(branch_id, 0) + 1
        if not hasattr(self, "_conn_owner"):
            self._conn_owner = {}
        self._conn_owner[id(conn)] = branch_id
        return conn

    def list_branches(self) -> list:
        self._refresh_branches()
        return [n for n in self._branches if not n.startswith("tmp_") and "_bk_" not in n]

    # ------------------------------------------------------------------
    # Connection hook
    # ------------------------------------------------------------------

    def _connect_impl(self, ref: Ref) -> None:
        if ref.commit:
            target = self._temp_branch(self._state(ref))
        else:
            target = self._branch_id(ref.branch)
        previous = self._conn_branch_id
        if self.conn:
            self._close(self.conn)
            self.conn = None
        if NEON_SUSPEND_ON_SWITCH and previous and previous != target:
            try:
                self._suspend(previous)
            except Exception as e:
                print(f"Warning: could not suspend compute of {previous}: {e}")
        self.conn = self._open(target)
        self._conn_branch_id = target

    def _connect(self, ref: Ref, exec_id: int, timed: bool, label: str, fallback: bool):
        """A branch restore or a compute restart closes existing
        connections; a dead connection is reopened on the next exec and
        recorded as a CONNECT like any other switch."""
        if self.conn is not None and getattr(self.conn, "closed", 0):
            self._current_ref = None
            self._conn_branch_id = None
        return super()._connect(ref, exec_id, timed, label, fallback)

    def _on(self, ref: Ref) -> str:
        """Point self.conn at the branch head; returns its id."""
        if self.conn is not None and getattr(self.conn, "closed", 0):
            self._current_ref = None
            self._conn_branch_id = None
        if self._current_ref != Ref(ref.branch) or self.conn is None:
            self._connect_impl(Ref(ref.branch))
            self._current_ref = Ref(ref.branch)
        return self._branch_id(ref.branch)

    def _temp_branch(self, st: _State) -> str:
        """A branch at ``st``'s LSN for reading a commit; cached, suspended
        between uses, deleted when the connection closes."""
        key = (st.branch_id, st.lsn)
        if key in self._temp:
            return self._temp[key]
        name = f"tmp_{lsn_to_int(st.lsn):x}_{os.getpid() % 10000}_{threading.get_ident() % 10000}_{len(self._temp)}"
        resp = self._api("POST", f"projects/{self.project_id}/branches", json={
            "endpoints": [_endpoint_spec(NEON_BRANCH_SUSPEND_TIMEOUT_SEC)],
            "branch": {"name": name, "parent_id": st.branch_id, "parent_lsn": st.lsn},
        })
        self._wait(resp.get("operations"))
        b = resp["branch"]
        self._register(name, b["id"], b.get("parent_id"), b.get("parent_lsn"))
        for e in resp.get("endpoints", []):
            if e.get("type") == "read_write":
                self._endpoints[b["id"]] = e["id"]
        self._temp[key] = b["id"]
        return b["id"]

    # ------------------------------------------------------------------
    # Bookkeeping (commit log)
    # ------------------------------------------------------------------

    _LOG_COLS = ("seq", "id", "kind", "message", "lsn", "branch_id", "prev_lsn", "prev_branch_id", "ts")

    def _exec(self, conn, sql: str, args=None):
        with conn.cursor() as cur:
            cur.execute(sql, args)
            if cur.description is not None:
                return cur.fetchall()
            return None

    def _flush_lsn(self, conn) -> str:
        return self._exec(conn, "SELECT pg_current_wal_flush_lsn()::text")[0][0]

    def _ensure_meta(self, branch: str) -> None:
        conn = self._open(self._branch_id(branch))
        try:
            self._exec(conn, _COMMITS_DDL)
            n = self._exec(conn, f'SELECT COUNT(*) FROM "{COMMITS_TABLE}"')[0][0]
            if not n:
                self._add_commit(conn, self._branch_id(branch), "root", "initial state")
        finally:
            self._close(conn)

    def _add_commit(self, conn, branch_id: str, kind: str, message: str, commit_id: str = None,
                    prev_lsn: str = None, prev_branch_id: str = None, lsn: str = None,
                    seq: int = None) -> tuple:
        """Append a log row; unless ``lsn`` is given, the branch's flush LSN
        right after the row is stored as the commit's point in time (so a
        branch created there carries the row). Returns (id, lsn)."""
        seq = seq or time.time_ns()
        commit_id = commit_id or f"{seq:x}"
        for _ in range(8):
            try:
                self._exec(conn, f'INSERT INTO "{COMMITS_TABLE}" (seq, id, kind, message, lsn, branch_id, '
                                 "prev_lsn, prev_branch_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                           (seq, commit_id, kind, message or "", lsn, branch_id, prev_lsn, prev_branch_id))
                break
            except psycopg2.IntegrityError:
                seq += 1
        else:
            raise RuntimeError("could not record commit")
        if lsn is None:
            lsn = self._flush_lsn(conn)
            self._exec(conn, f'UPDATE "{COMMITS_TABLE}" SET lsn = %s WHERE seq = %s', (lsn, seq))
        return commit_id, lsn

    def _log_rows(self, conn, limit: int = None, max_seq: int = None) -> list:
        sql = f'SELECT {", ".join(self._LOG_COLS)} FROM "{COMMITS_TABLE}"'
        args = []
        if max_seq is not None:
            sql += " WHERE seq <= %s"
            args.append(int(max_seq))
        sql += " ORDER BY seq DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        rows = self._exec(conn, sql, tuple(args) if args else None) or []
        return [dict(zip(self._LOG_COLS, r)) for r in rows]

    def _find_commit(self, conn, commit_id: str) -> dict:
        rows = self._exec(conn, f'SELECT {", ".join(self._LOG_COLS)} FROM "{COMMITS_TABLE}" '
                                "WHERE id = %s ORDER BY seq DESC LIMIT 1", (str(commit_id),))
        if not rows:
            raise ValueError(f"unknown commit '{commit_id}'")
        return dict(zip(self._LOG_COLS, rows[0]))

    def _base_row(self, conn) -> dict:
        rows = self._exec(conn, f'SELECT {", ".join(self._LOG_COLS)} FROM "{COMMITS_TABLE}" '
                                "WHERE kind IN ('fork', 'rebase', 'root') ORDER BY seq DESC LIMIT 1")
        return dict(zip(self._LOG_COLS, rows[0]))

    def _state(self, ref: Ref) -> _State:
        bid = self._branch_id(ref.branch)
        if not ref.commit:
            return _State(ref.branch, bid)
        conn = self._conn_for(bid)
        try:
            row = self._find_commit(conn, ref.commit)
        finally:
            self._release(conn)
        if not row["lsn"]:
            raise ValueError(f"commit '{ref.commit}' has no LSN recorded on {ref.branch}")
        return _State(ref.branch, row["branch_id"] or bid, row["lsn"], int(row["seq"]))

    def _conn_for(self, branch_id: str):
        """self.conn when it is on that branch, else a short-lived one
        (callers close it through _release)."""
        if self.conn is not None and self._conn_branch_id == branch_id:
            return self.conn
        return self._open(branch_id)

    def _release(self, conn) -> None:
        if conn is not None and conn is not self.conn:
            self._close(conn)

    # ------------------------------------------------------------------
    # Table metadata (Postgres)
    # ------------------------------------------------------------------

    def _tables(self, conn) -> list:
        rows = self._exec(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "
                                "AND table_type = 'BASE TABLE' AND table_name NOT LIKE %s ORDER BY table_name",
                          (META_PREFIX.replace("_", r"\_") + "%",))
        return [r[0] for r in rows or []]

    def _columns(self, conn, table: str) -> list:
        rows = self._exec(conn, "SELECT column_name, data_type, character_maximum_length, numeric_precision, "
                                "numeric_scale, is_nullable, column_default FROM information_schema.columns "
                                "WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position", (table,))
        return [tuple(r) for r in rows or []]

    def _pk(self, conn, table: str) -> list:
        rows = self._exec(conn, "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
                                "AND a.attnum = ANY(i.indkey) WHERE i.indrelid = %s::regclass AND i.indisprimary "
                                "ORDER BY array_position(i.indkey, a.attnum)", (f'public."{table}"',))
        return [r[0] for r in rows or []]

    def _table_hash(self, conn, table: str) -> tuple:
        """(row count, order-independent hash) of a table, computed where
        the table lives."""
        rows = self._exec(conn, f'SELECT COUNT(*), COALESCE(SUM(hashtext(t::text)::bigint), 0) FROM {_q(table)} t')
        return (int(rows[0][0]), int(rows[0][1]))

    _NB = 256  # hash buckets per table for change detection and partial pulls

    def _bucket_expr(self, conn, table: str, pk: list, alias: str = None) -> str:
        """SQL for a row's bucket (0..255) from its integer primary-key
        columns, or None when the key has none. Built from immutable
        functions only, so postgres_fdw ships the predicate to the remote
        compute and only matching rows travel."""
        types = {c[0]: c[1] for c in self._columns(conn, table)}
        parts = []
        for c in pk:
            col = f"{alias}.{_q(c)}" if alias else _q(c)
            t = types.get(c)
            if t in ("integer", "smallint"):
                parts.append(f"hashint4({col}::int)")
            elif t == "bigint":
                parts.append(f"hashint8({col})")
        if not parts:
            return None
        return "((" + " # ".join(parts) + f") & {self._NB - 1})"

    def _bucket_hashes(self, conn, table: str, bexpr: str) -> dict:
        """{bucket: (rows, hash)} computed where the table lives."""
        rows = self._exec(conn, f"SELECT {bexpr} AS b, COUNT(*), COALESCE(SUM(hashtext(t::text)::bigint), 0) "
                                f"FROM {_q(table)} t GROUP BY 1") or []
        return {int(b): (int(n), int(h)) for b, n, h in rows}

    @staticmethod
    def _changed_buckets(a: dict, b: dict) -> list:
        return sorted(k for k in set(a) | set(b) if a.get(k, (0, 0)) != b.get(k, (0, 0)))

    @staticmethod
    def _type_sql(col) -> str:
        name, dtype, char_len, prec, scale, nullable, default = col
        t = dtype
        if dtype in ("character varying", "character") and char_len:
            t = f"{dtype}({char_len})"
        elif dtype == "numeric" and prec is not None:
            t = f"numeric({prec},{scale})"
        return t

    # ------------------------------------------------------------------
    # postgres_fdw: read another branch's tables from this connection
    # ------------------------------------------------------------------

    def _fdw_schema(self, conn, local_branch_id: str, remote_branch_id: str, tables: list) -> str:
        """Import ``tables`` of the remote branch into a schema on this
        connection's database; returns the schema name."""
        if local_branch_id not in self._fdw_ready:
            self._exec(conn, "CREATE EXTENSION IF NOT EXISTS postgres_fdw")
            self._fdw_ready.add(local_branch_id)
        pr = urlparse(self._uri(remote_branch_id))
        srv = f"bb_srv_{remote_branch_id.replace('-', '_')}"
        schema = f"bb_{remote_branch_id.replace('-', '_')}"
        self._exec(conn, f"CREATE SERVER IF NOT EXISTS {_q(srv)} FOREIGN DATA WRAPPER postgres_fdw "
                         f"OPTIONS (host '{pr.hostname}', dbname '{self.db_name}', port '5432', "
                         f"sslmode 'require', fetch_size '10000', "
                         f"options '-c idle_in_transaction_session_timeout=0 -c statement_timeout=0')")
        self._exec(conn, f"CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER {_q(srv)} "
                         f"OPTIONS (user '{pr.username}', password '{pr.password}')")
        self._exec(conn, f"DROP SCHEMA IF EXISTS {_q(schema)} CASCADE")
        self._exec(conn, f"CREATE SCHEMA {_q(schema)}")
        if tables:
            names = ", ".join(_q(t) for t in tables)
            self._exec(conn, f"IMPORT FOREIGN SCHEMA public LIMIT TO ({names}) FROM SERVER {_q(srv)} INTO {_q(schema)}")
        return schema

    def _fdw_cleanup(self, conn, remote_branch_id: str) -> None:
        srv = f"bb_srv_{remote_branch_id.replace('-', '_')}"
        schema = f"bb_{remote_branch_id.replace('-', '_')}"
        for sql in (f"DROP SCHEMA IF EXISTS {_q(schema)} CASCADE", f"DROP SERVER IF EXISTS {_q(srv)} CASCADE"):
            try:
                self._exec(conn, sql)
            except Exception:
                pass

    def _fk_order(self, conn, tables: list) -> list:
        """``tables`` sorted so that referenced tables come before the
        tables that reference them (inserts in this order, deletes in
        reverse keep foreign keys satisfied without superuser rights)."""
        rows = self._exec(conn, "SELECT c.conrelid::regclass::text, c.confrelid::regclass::text FROM pg_constraint c "
                                "WHERE c.contype = 'f' AND c.connamespace = 'public'::regnamespace") or []
        deps = {t: set() for t in tables}
        for child, parent in rows:
            child, parent = child.strip('"'), parent.strip('"')
            if child in deps and parent in deps and child != parent:
                deps[child].add(parent)
        out, done = [], set()
        while len(out) < len(tables):
            ready = [t for t in tables if t not in done and deps[t] <= done]
            if not ready:
                ready = [t for t in tables if t not in done]  # cycle: give up on order
            for t in ready:
                out.append(t)
                done.add(t)
        return out

    def _pull(self, conn, schema: str, table: str, local_name: str, cols: list, available=None,
              types: dict = None, where: str = None) -> None:
        """Copy a foreign table into a local temp table with exactly ``cols``;
        columns the remote table lacks (``available`` lists what it has)
        come out NULL, cast to the local column's type (``types``)."""
        def col(c):
            if available is None or c in available:
                return _q(c)
            return f"NULL::{types[c]} AS {_q(c)}" if types and c in types else f"NULL AS {_q(c)}"
        select = ", ".join(col(c) for c in cols)
        self._exec(conn, f"DROP TABLE IF EXISTS {_q(local_name)}")
        self._exec(conn, f"CREATE TEMP TABLE {_q(local_name)} AS SELECT {select} FROM {_q(schema)}.{_q(table)}"
                         + (f" WHERE {where}" if where else ""))

    # ------------------------------------------------------------------
    # SQL fragments (Postgres)
    # ------------------------------------------------------------------

    @staticmethod
    def _join(a, b, pk):
        return " AND ".join(f"{a}.{_q(c)} = {b}.{_q(c)}" for c in pk)

    @staticmethod
    def _same(a, b, cols):
        if not cols:
            return "TRUE"
        return " AND ".join(f"{a}.{_q(c)} IS NOT DISTINCT FROM {b}.{_q(c)}" for c in cols)

    @staticmethod
    def _key(alias, pk):
        return "concat_ws('|', " + ", ".join(f"{alias}.{_q(c)}::text" for c in pk) + ")"

    @staticmethod
    def _cols(alias, cols):
        return ", ".join(f"{alias}.{_q(c)}" for c in cols)

    @staticmethod
    def _upsert(dst, cols, pk, select):
        rest = [c for c in cols if c not in pk]
        head = f"INSERT INTO {dst} ({', '.join(_q(c) for c in cols)}) {select}"
        keys = ", ".join(_q(c) for c in pk)
        if not rest:
            return f"{head} ON CONFLICT ({keys}) DO NOTHING"
        sets = ", ".join(f"{_q(c)} = EXCLUDED.{_q(c)}" for c in rest)
        return f"{head} ON CONFLICT ({keys}) DO UPDATE SET {sets}"

    # ------------------------------------------------------------------
    # Verbs
    # ------------------------------------------------------------------

    def _storage_bytes(self) -> int:
        """pg_database_size() of the run's database on the default branch,
        over a dedicated connection (the API's logical_size and the
        project's synthetic_storage_size lag by more than a run, so they
        cannot feed the sampler). Branch computes are not touched, so this
        is the spine's logical size, not the project's billed storage."""
        if getattr(self, "_closed", False):
            return 0  # the run's project is gone (after cleanup)
        for attempt in range(2):
            try:
                if getattr(self, "_size_conn", None) is None:
                    self._size_conn = self._open(self._branch_id(self.default_branch), max_wait=15)
                return int(self._exec(self._size_conn, "SELECT pg_database_size(current_database())")[0][0])
            except Exception:
                try:
                    if getattr(self, "_size_conn", None) is not None:
                        self._close(self._size_conn)
                except Exception:
                    pass
                self._size_conn = None
                if attempt == 1:
                    raise
        return 0

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        st = self._state(from_ref)
        body = {"endpoints": [_endpoint_spec(NEON_BRANCH_SUSPEND_TIMEOUT_SEC)],
                "branch": {"name": name, "parent_id": st.branch_id}}
        if st.lsn:
            body["branch"]["parent_lsn"] = st.lsn
        resp = self._api("POST", f"projects/{self.project_id}/branches", json=body)
        self._wait(resp.get("operations"))
        b = resp["branch"]
        self._register(name, b["id"], b.get("parent_id"), b.get("parent_lsn"))
        for e in resp.get("endpoints", []):
            if e.get("type") == "read_write":
                self._endpoints[b["id"]] = e["id"]
        # The fork row: the branch's own LSN right after creation, and the
        # parent point it was cloned from (the base of later merges).
        conn = self._open(b["id"])
        try:
            if st.seq is not None:
                self._exec(conn, f'DELETE FROM "{COMMITS_TABLE}" WHERE seq > %s', (st.seq,))
                # The commit's row predates its own LSN (written after the
                # row); give the copy the value the parent has.
                self._exec(conn, f'UPDATE "{COMMITS_TABLE}" SET lsn = %s WHERE seq = %s', (st.lsn, st.seq))
            self._add_commit(conn, b["id"], "fork", f"fork from {from_ref}",
                             prev_lsn=b.get("parent_lsn") or st.lsn, prev_branch_id=st.branch_id)
        finally:
            self._close(conn)
        if NEON_SUSPEND_ON_SWITCH:
            self._suspend(b["id"])

    def _commit_impl(self, ref: Ref, message: str) -> str:
        bid = self._on(ref)
        head = self._log_rows(self.conn, limit=1)[0]
        commit_id, _ = self._add_commit(self.conn, bid, "commit", message,
                                        prev_lsn=head["lsn"], prev_branch_id=head["branch_id"])
        return commit_id

    def _log_impl(self, ref: Ref, limit: int) -> list:
        bid = self._branch_id(ref.branch)
        conn = self._conn_for(bid)
        try:
            max_seq = int(self._find_commit(conn, ref.commit)["seq"]) if ref.commit else None
            return [{"commit_hash": r["id"], "kind": r["kind"], "committer": "neon", "email": "",
                     "date": r["ts"], "message": r["message"], "lsn": r["lsn"]}
                    for r in self._log_rows(conn, limit=limit, max_seq=max_seq)]
        finally:
            self._release(conn)

    # -- diff ----------------------------------------------------------------

    def _reading_branch(self, st: _State) -> str:
        """Branch id whose compute holds ``st``: the branch itself for a
        head, a temporary branch for a commit."""
        if st.lsn and not self._is_head_lsn(st):
            return self._temp_branch(st)
        return st.branch_id

    def _is_head_lsn(self, st: _State) -> bool:
        """True when nothing was written on the branch since the commit
        (its flush LSN still equals the commit's)."""
        conn = self._conn_for(st.branch_id)
        try:
            return self._flush_lsn(conn) == st.lsn
        finally:
            self._release(conn)

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        a_id = self._reading_branch(self._state(ref_a))
        b_id = self._reading_branch(self._state(ref_b))
        ca, cb = self._open(a_id), (self._conn_for(b_id) if b_id != a_id else None)
        changed_fdw = False
        try:
            if cb is None:
                return {"tables": [], "rows_added": 0, "rows_deleted": 0, "rows_modified": 0}
            ta, tb = set(self._tables(ca)), set(self._tables(cb))
            out = []
            changed = {}
            for t in sorted(ta | tb):
                if t in ta and t not in tb:
                    out.append({"table_name": t, "rows_added": 0, "rows_deleted": self._table_hash(ca, t)[0], "rows_modified": 0})
                elif t in tb and t not in ta:
                    out.append({"table_name": t, "rows_added": self._table_hash(cb, t)[0], "rows_deleted": 0, "rows_modified": 0})
                else:
                    pk0 = self._pk(cb, t)
                    bexpr = self._bucket_expr(cb, t, pk0) if pk0 else None
                    if bexpr is None:
                        if self._table_hash(ca, t) != self._table_hash(cb, t):
                            changed[t] = None
                    else:
                        diffb = self._changed_buckets(self._bucket_hashes(ca, t, bexpr), self._bucket_hashes(cb, t, bexpr))
                        if diffb:
                            changed[t] = f"{bexpr} IN ({', '.join(str(b) for b in diffb)})"
            if changed:
                changed_fdw = True
                schema = self._fdw_schema(cb, b_id, a_id, list(changed))
                for t, where in changed.items():
                    cols_b = [c[0] for c in self._columns(cb, t)]
                    cols_a = {c[0] for c in self._columns(ca, t)}
                    cols = [c for c in cols_b if c in cols_a]
                    pk = [c for c in self._pk(cb, t) if c in cols] or cols
                    rest = [c for c in cols if c not in pk]
                    self._pull(cb, schema, t, "_bb_a", cols, available=cols_a,
                               types={c[0]: self._type_sql(c) for c in self._columns(cb, t)}, where=where)
                    self._exec(cb, 'DROP TABLE IF EXISTS "_bb_bsub"')
                    self._exec(cb, f'CREATE TEMP TABLE "_bb_bsub" AS SELECT {", ".join(_q(c) for c in cols)} FROM {_q(t)}'
                                   + (f" WHERE {where}" if where else ""))
                    B = '"_bb_bsub"'
                    added = self._exec(cb, f'SELECT COUNT(*) FROM {B} y LEFT JOIN "_bb_a" x ON {self._join("x", "y", pk)} WHERE x.{_q(pk[0])} IS NULL')[0][0]
                    deleted = self._exec(cb, f'SELECT COUNT(*) FROM "_bb_a" x LEFT JOIN {B} y ON {self._join("x", "y", pk)} WHERE y.{_q(pk[0])} IS NULL')[0][0]
                    modified = 0
                    if rest:
                        modified = self._exec(cb, f'SELECT COUNT(*) FROM "_bb_a" x JOIN {B} y ON {self._join("x", "y", pk)} WHERE NOT ({self._same("x", "y", rest)})')[0][0]
                    out.append({"table_name": t, "rows_added": int(added), "rows_deleted": int(deleted), "rows_modified": int(modified)})
                    self._exec(cb, 'DROP TABLE IF EXISTS "_bb_a"; DROP TABLE IF EXISTS "_bb_bsub"')
            return {"tables": out, "rows_added": sum(x["rows_added"] for x in out),
                    "rows_deleted": sum(x["rows_deleted"] for x in out),
                    "rows_modified": sum(x["rows_modified"] for x in out)}
        finally:
            if cb is not None and changed_fdw:
                self._fdw_cleanup(cb, a_id)
            self._close(ca)
            self._release(cb)

    # -- three-way merge -----------------------------------------------------

    def _reconcile_schema(self, ours, theirs_conn, theirs_id: str) -> dict:
        """Give ours the tables and columns theirs has and it lacks (data
        of new tables copied over fdw); a differing primary key is a
        schema conflict raised before any change."""
        ours_tables = set(self._tables(ours))
        theirs_tables = self._tables(theirs_conn)
        new_tables = sorted(set(theirs_tables) - ours_tables)
        plan_cols = []
        for t in sorted(ours_tables & set(theirs_tables)):
            if self._pk(ours, t) != self._pk(theirs_conn, t):
                raise RuntimeError(f"schema conflict on {t}: primary keys differ")
            have = {c[0] for c in self._columns(ours, t)}
            for col in self._columns(theirs_conn, t):
                if col[0] not in have:
                    plan_cols.append((t, col))
        if new_tables:
            schema = self._fdw_schema(ours, self._branch_id(self._current_ref.branch), theirs_id, new_tables)
            for t in new_tables:
                cols = self._columns(theirs_conn, t)
                defs = ", ".join(f"{_q(c[0])} {self._type_sql(c)}" + (" NOT NULL" if c[5] == "NO" else "") for c in cols)
                pk = self._pk(theirs_conn, t)
                if pk:
                    defs += f", PRIMARY KEY ({', '.join(_q(c) for c in pk)})"
                self._exec(ours, f"CREATE TABLE {_q(t)} ({defs})")
                self._exec(ours, f"INSERT INTO {_q(t)} SELECT {', '.join(_q(c[0]) for c in cols)} FROM {_q(schema)}.{_q(t)}")
        for t, col in plan_cols:
            ddl = f"ALTER TABLE {_q(t)} ADD COLUMN {_q(col[0])} {self._type_sql(col)}"
            if col[6] is not None:
                ddl += f" DEFAULT {col[6]}"
            self._exec(ours, ddl)
        return {"added_tables": new_tables, "added_columns": [f"{t}.{c[0]}" for t, c in plan_cols]}

    def _three_way(self, ours, ours_id: str, theirs_conn, theirs_id: str, base_conn, base_id: str,
                   on_conflict, ref: Ref, new_tables: list, keep_ours: bool) -> tuple:
        """Apply theirs' changes since base onto ours (self.conn's branch).
        Per table, theirs and base are pulled over postgres_fdw into temp
        tables and the three-way comparison runs locally; deletes are
        applied in reverse foreign-key order and upserts in forward order.
        Returns (info, conflicts-for-callable)."""
        info = {"conflicts": 0, "conflict_tables": [], "ours_changes": 0, "theirs_changes": 0,
                "merged_tables": [], "skipped_tables": {}}
        conflicts = []
        want_rows = callable(on_conflict)
        base_tables = set(self._tables(base_conn))
        tables = sorted((set(self._tables(ours)) & set(self._tables(theirs_conn))) - set(new_tables))
        # Change detection per hash bucket of the key, each side hashing
        # its own rows locally; only buckets theirs changed are pulled.
        buckets = {}   # table -> (bexpr, theirs-changed buckets, ours-changed buckets) or None
        changed = []
        for t in tables:
            pk = self._pk(ours, t)
            bexpr = self._bucket_expr(ours, t, pk) if pk else None
            if bexpr is None:
                if t not in base_tables or self._table_hash(theirs_conn, t) != self._table_hash(base_conn, t):
                    changed.append(t)
                    buckets[t] = None
                continue
            h_t = self._bucket_hashes(theirs_conn, t, bexpr)
            h_b = self._bucket_hashes(base_conn, t, bexpr) if t in base_tables else {}
            tb = self._changed_buckets(h_t, h_b)
            if not tb:
                continue
            h_o = self._bucket_hashes(ours, t, bexpr)
            ob = self._changed_buckets(h_o, h_b)
            info["ours_changes"] += len(ob)
            changed.append(t)
            buckets[t] = (bexpr, tb, ob)
        if not changed:
            return info, conflicts
        changed = self._fk_order(ours, changed)
        s_t = self._fdw_schema(ours, ours_id, theirs_id, changed)
        s_b = self._fdw_schema(ours, ours_id, base_id, [t for t in changed if t in base_tables])
        plans = {}
        temps = []
        self._exec(ours, "BEGIN")
        try:
            for i, t in enumerate(changed):
                ocols = self._columns(ours, t)
                types = {c[0]: self._type_sql(c) for c in ocols}
                cols_o = [c[0] for c in ocols]
                cols_t = {c[0] for c in self._columns(theirs_conn, t)}
                cols = [c for c in cols_o if c in cols_t]
                pk = [c for c in self._pk(ours, t) if c in cols]
                T, B, OS = f"_bb_t{i}", f"_bb_b{i}", f"_bb_o{i}"
                temps += [T, B, OS]
                where = None
                if buckets.get(t):
                    bexpr, tb, _ = buckets[t]
                    where = f"{bexpr} IN ({', '.join(str(b) for b in tb)})"
                self._pull(ours, s_t, t, T, cols, where=where)
                if t in base_tables:
                    self._pull(ours, s_b, t, B, cols, available={c[0] for c in self._columns(base_conn, t)}, types=types, where=where)
                else:
                    self._exec(ours, f'CREATE TEMP TABLE {_q(B)} AS SELECT {", ".join(_q(c) for c in cols)} FROM {_q(T)} WHERE FALSE')
                # Our rows in the same buckets (the only ones theirs can conflict with).
                self._exec(ours, f'CREATE TEMP TABLE {_q(OS)} AS SELECT {", ".join(_q(c) for c in cols)} FROM {_q(t)}'
                                 + (f" WHERE {where}" if where else ""))
                plans[t] = {"cols": cols, "pk": pk, "T": _q(T), "B": _q(B), "O": _q(t), "OS": _q(OS)}
                if not pk:
                    continue
                rest = [c for c in cols if c not in pk]
                kt, ko, kb = self._key("t", pk), self._key("o", pk), self._key("b", pk)
                O, Tq, Bq = _q(OS), _q(T), _q(B)
                d_tb = f"b.{_q(pk[0])} IS NULL" + (f" OR NOT ({self._same('t', 'b', rest)})" if rest else "")
                d_ob = f"b.{_q(pk[0])} IS NULL" + (f" OR NOT ({self._same('o', 'b', rest)})" if rest else "")
                TK, OK, CK = f"_bb_tk{i}", f"_bb_ok{i}", f"_bb_ck{i}"
                temps += [TK, OK, CK]
                self._exec(ours, f'CREATE TEMP TABLE {_q(TK)} AS SELECT {kt} AS k, \'u\' AS side FROM {Tq} t LEFT JOIN {Bq} b ON {self._join("t", "b", pk)} WHERE {d_tb} '
                                 f'UNION ALL SELECT {kb}, \'d\' FROM {Bq} b LEFT JOIN {Tq} t ON {self._join("t", "b", pk)} WHERE t.{_q(pk[0])} IS NULL')
                self._exec(ours, f'CREATE TEMP TABLE {_q(OK)} AS SELECT {ko} AS k, \'u\' AS side FROM {O} o LEFT JOIN {Bq} b ON {self._join("o", "b", pk)} WHERE {d_ob} '
                                 f'UNION ALL SELECT {kb}, \'d\' FROM {Bq} b LEFT JOIN {O} o ON {self._join("o", "b", pk)} WHERE o.{_q(pk[0])} IS NULL')
                self._exec(ours, f'CREATE INDEX ON {_q(TK)} (k); CREATE INDEX ON {_q(OK)} (k)')
                n_t = int(self._exec(ours, f'SELECT COUNT(*) FROM {_q(TK)}')[0][0])
                n_o = int(self._exec(ours, f'SELECT COUNT(*) FROM {_q(OK)}')[0][0])
                info["theirs_changes"] += n_t
                if not buckets.get(t):
                    info["ours_changes"] += n_o
                same_ot = self._same("o", "t", rest) if rest else "TRUE"
                self._exec(ours, f'CREATE TEMP TABLE {_q(CK)} AS SELECT a.k FROM {_q(TK)} a JOIN {_q(OK)} b ON b.k = a.k '
                                 f'LEFT JOIN {O} o ON {ko} = a.k LEFT JOIN {Tq} t ON {kt} = a.k '
                                 f"WHERE NOT (a.side = 'd' AND b.side = 'd') AND NOT (a.side = 'u' AND b.side = 'u' AND {same_ot})")
                self._exec(ours, f'CREATE INDEX ON {_q(CK)} (k)')
                n_c = int(self._exec(ours, f'SELECT COUNT(*) FROM {_q(CK)}')[0][0])
                plans[t].update({"rest": rest, "TK": _q(TK), "CK": _q(CK), "n_t": n_t, "n_c": n_c})
            # Their deletes, children first; then their upserts, parents first.
            for t in reversed(changed):
                pl = plans[t]
                if not pl["pk"] or not pl.get("n_t"):
                    continue
                not_c = f'NOT EXISTS (SELECT 1 FROM {pl["CK"]} c WHERE c.k = s.k)'
                apply_c = "" if (keep_ours or want_rows) else " OR TRUE"
                self._exec(ours, f'DELETE FROM {pl["O"]} o WHERE {self._key("o", pl["pk"])} IN '
                                 f'(SELECT s.k FROM {pl["TK"]} s WHERE s.side = \'d\' AND ({not_c}{apply_c}))')
            for t in changed:
                pl = plans[t]
                cols, pk = pl["cols"], pl["pk"]
                if not pk:
                    # Rows theirs added since base, as a multiset difference:
                    # EXCEPT ALL hashes (NULLs compare equal), where an anti-join
                    # on IS NOT DISTINCT FROM over every column is a nested loop
                    # that grows with |theirs| x |base| (the history table of a
                    # dev_agent run made that the rebase's dominant cost).
                    cl = ", ".join(_q(c) for c in cols)
                    self._exec(ours, f'INSERT INTO {pl["O"]} ({cl}) SELECT {cl} FROM '
                                     f'(SELECT {cl} FROM {pl["T"]} EXCEPT ALL SELECT {cl} FROM {pl["B"]}) x')
                    info["merged_tables"].append(t)
                    info["skipped_tables"][t] = "no primary key: new rows only"
                    continue
                if not pl.get("n_t"):
                    continue
                not_c = f'NOT EXISTS (SELECT 1 FROM {pl["CK"]} c WHERE c.k = s.k)'
                apply_c = "" if (keep_ours or want_rows) else " OR TRUE"
                self._exec(ours, self._upsert(pl["O"], cols, pk, f'SELECT {self._cols("t", cols)} FROM {pl["T"]} t WHERE {self._key("t", pk)} IN '
                                                                 f'(SELECT s.k FROM {pl["TK"]} s WHERE s.side = \'u\' AND ({not_c}{apply_c}))'))
                info["merged_tables"].append(t)
                if pl["n_c"]:
                    info["conflicts"] += pl["n_c"]
                    info["conflict_tables"].append(t)
                    if want_rows:
                        conflicts.append({"table": t, "rows": self._conflict_rows(ours, pl["OS"], pl["T"], pl["B"], pl["CK"], cols, pk)})
            self._exec(ours, "COMMIT")
        except BaseException:
            try:
                self._exec(ours, "ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            for tmp in temps:
                try:
                    self._exec(ours, f'DROP TABLE IF EXISTS {_q(tmp)}')
                except Exception:
                    pass
        return info, conflicts

    def _conflict_rows(self, ours, O, T, B, CK, cols, pk) -> list:
        select = ", ".join([f"b.{_q(c)}" for c in cols] + [f"o.{_q(c)}" for c in cols] + [f"t.{_q(c)}" for c in cols])
        rows = self._exec(ours, f'SELECT {select}, b.{_q(pk[0])} IS NULL, o.{_q(pk[0])} IS NULL, t.{_q(pk[0])} IS NULL '
                                f'FROM {CK} c LEFT JOIN {B} b ON {self._key("b", pk)} = c.k '
                                f'LEFT JOIN {O} o ON {self._key("o", pk)} = c.k LEFT JOIN {T} t ON {self._key("t", pk)} = c.k '
                                f'LIMIT {_CONFLICT_ROW_LIMIT}') or []
        names = [f"base_{c}" for c in cols] + [f"our_{c}" for c in cols] + [f"their_{c}" for c in cols]
        out = []
        for r in rows:
            d = dict(zip(names, r[:len(names)]))
            no_base, no_ours, no_theirs = r[len(names):]

            def diff_type(missing):
                return "removed" if missing else ("added" if no_base else "modified")
            d["our_diff_type"] = diff_type(no_ours)
            d["their_diff_type"] = diff_type(no_theirs)
            out.append(d)
        return out

    def _merge_base(self, ours_id: str, theirs_conn, theirs_id: str, ours_conn) -> _State:
        """The fork/rebase point of whichever side descends from the other."""
        rt = self._base_row(theirs_conn)
        ro = self._base_row(ours_conn)
        cands = []
        if rt["prev_branch_id"] == ours_id and rt["kind"] in ("fork", "rebase"):
            cands.append((rt, _State(None, rt["prev_branch_id"], rt["prev_lsn"], int(rt["seq"]))))
        if ro["prev_branch_id"] == theirs_id and ro["kind"] in ("fork", "rebase"):
            cands.append((ro, _State(None, ro["prev_branch_id"], ro["prev_lsn"], int(ro["seq"]))))
        if not cands:
            return None
        return max(cands, key=lambda c: int(c[0]["seq"]))[1]

    def _copy_log(self, into_conn, from_conn, max_seq=None) -> None:
        rows = self._log_rows(from_conn)
        have = {r["id"] for r in self._log_rows(into_conn)}
        for r in reversed(rows):
            if r["kind"] not in ("commit", "merge") or r["id"] in have:
                continue
            if max_seq is not None and int(r["seq"]) > max_seq:
                continue
            self._add_commit(into_conn, r["branch_id"], r["kind"], r["message"], commit_id=r["id"],
                             prev_lsn=r["prev_lsn"], prev_branch_id=r["prev_branch_id"], lsn=r["lsn"],
                             seq=int(r["seq"]))

    def _merge_impl(self, into: Ref, source: Ref, message: str, on_conflict="ours") -> dict:
        src = self._state(source)
        theirs_id = self._reading_branch(src)
        self._reserve(theirs_id)
        try:
            ours_id = self._on(into)
            if theirs_id == ours_id:
                raise ValueError(f"Cannot merge branch '{source.branch}' into itself")
            return self._merge_reserved(into, source, message, on_conflict, src, theirs_id, ours_id)
        finally:
            self._reserve(theirs_id, -1)

    def _merge_reserved(self, into, source, message, on_conflict, src, theirs_id, ours_id) -> dict:
        ours = self.conn
        theirs_conn = self._open(theirs_id)
        base_conn, base_id = None, None
        try:
            base = self._merge_base(ours_id, theirs_conn, src.branch_id, ours)
            info = {"two_way": base is None}
            if base is not None:
                base_id = self._reading_branch(base)
                base_conn = self._open(base_id)
            else:
                print(f"Warning: {source.branch} and {into.branch} share no fork point: merging two-way")
                base_id = theirs_id
                base_conn = self._open(theirs_id)
                self._exec(base_conn, "SET search_path TO public")
            schema = self._reconcile_schema(ours, theirs_conn, theirs_id)
            keep_ours = callable(on_conflict) or on_conflict == "ours"
            if base is None:
                # No common ancestor: everything theirs has is "their change".
                tw_info, conflicts = {"conflicts": 0, "conflict_tables": [], "ours_changes": 0,
                                      "theirs_changes": 0, "merged_tables": [], "skipped_tables": {}}, []
                for t in sorted(set(self._tables(ours)) & set(self._tables(theirs_conn)) - set(schema["added_tables"])):
                    cols_o = [c[0] for c in self._columns(ours, t)]
                    cols = [c for c in cols_o if c in {c2[0] for c2 in self._columns(theirs_conn, t)}]
                    pk = [c for c in self._pk(ours, t) if c in cols]
                    s_t = self._fdw_schema(ours, ours_id, theirs_id, [t])
                    if pk:
                        self._exec(ours, self._upsert(_q(t), cols, pk, f"SELECT {', '.join(_q(c) for c in cols)} FROM {_q(s_t)}.{_q(t)}")
                                   if not keep_ours else
                                   f"INSERT INTO {_q(t)} ({', '.join(_q(c) for c in cols)}) SELECT {', '.join(_q(c) for c in cols)} FROM {_q(s_t)}.{_q(t)} ON CONFLICT DO NOTHING")
                    tw_info["merged_tables"].append(t)
                info.update(tw_info)
            else:
                tw_info, conflicts = self._three_way(ours, ours_id, theirs_conn, theirs_id, base_conn, base_id,
                                                     on_conflict, into, schema["added_tables"], keep_ours)
                info.update(tw_info)
            if conflicts:
                info["resolved"] = "custom"
                info["resolution"] = on_conflict(self._conflict_session(into), conflicts)
            elif info.get("conflicts"):
                info["resolved"] = on_conflict
            head = self._log_rows(ours, limit=1)[0]  # before the source's commits join the log
            self._copy_log(ours, theirs_conn, max_seq=src.seq)
            src_head = self._find_commit(theirs_conn, source.commit) if src.lsn else self._log_rows(theirs_conn, limit=1)[0]
            commit_id = src_head["id"] if src_head["kind"] == "commit" else None
            info["hash"], _ = self._add_commit(ours, ours_id, "merge", message or f"Merge {source.branch} into {into.branch}",
                                               commit_id=commit_id, prev_lsn=head["lsn"], prev_branch_id=head["branch_id"])
            info["fast_forward"] = info.get("ours_changes", 0) == 0 or info.get("theirs_changes", 0) == 0
            info["schema_changes"] = schema
            return info
        finally:
            self._close(theirs_conn)
            if base_conn is not None:
                self._close(base_conn)
            for bid in {theirs_id, base_id}:
                if bid:
                    self._fdw_cleanup(ours, bid)

    def _rebase_impl(self, ref: Ref, onto: Ref, on_conflict="ours") -> dict:
        ours_id = self._on(ref)
        ours = self.conn
        up = self._state(onto)
        if up.branch_id == ours_id:
            raise ValueError(f"Cannot rebase branch '{ref.branch}' onto itself")
        up_conn = self._open(up.branch_id)
        base_conn, base_id = None, None
        theirs_conn, theirs_id = None, None
        try:
            # Read the upstream at one point (a temporary branch at its LSN,
            # since a spine under load keeps moving): that LSN becomes the
            # branch's new base.
            up_lsn = up.lsn or self._flush_lsn(up_conn)
            theirs_id = self._temp_branch(_State(up.branch, up.branch_id, up_lsn))
            theirs_conn = self._open(theirs_id)
            base = self._merge_base(ours_id, up_conn, up.branch_id, ours)
            if base is None:
                print(f"Warning: {ref.branch} and {onto.branch} share no fork point: upstream conflicts not detected")
            info = {"up_to_date": False}
            base_id = self._reading_branch(base) if base is not None else None
            base_conn = self._open(base_id) if base_id else None
            schema = self._reconcile_schema(ours, theirs_conn, theirs_id)
            # git: "ours" is the upstream, "theirs" the branch's own commits.
            keep_branch = (on_conflict == "theirs")
            if base_conn is not None:
                tw_info, conflicts = self._three_way(ours, ours_id, theirs_conn, theirs_id, base_conn, base_id,
                                                     on_conflict, ref, schema["added_tables"], keep_ours=keep_branch or callable(on_conflict))
            else:
                tw_info, conflicts = {"conflicts": 0, "conflict_tables": [], "ours_changes": 0, "theirs_changes": 0,
                                      "merged_tables": [], "skipped_tables": {}}, []
            # Swap sides for the callable: our_* = upstream, their_* = branch.
            for cf in conflicts:
                for row in cf["rows"]:
                    for c in list(row):
                        if c.startswith("our_") and not c.endswith("_diff_type"):
                            row[c], row["their_" + c[4:]] = row["their_" + c[4:]], row[c]
                    row["our_diff_type"], row["their_diff_type"] = row["their_diff_type"], row["our_diff_type"]
            info.update(tw_info)
            if conflicts:
                info["resolved"] = "custom"
                info["resolution"] = on_conflict(self._conflict_session(ref), conflicts)
            elif info.get("conflicts"):
                info["resolved"] = on_conflict
            info["up_to_date"] = info.get("theirs_changes", 0) == 0 and not schema["added_tables"] and not schema["added_columns"]
            self._copy_log(ours, up_conn, max_seq=up.seq)
            self._add_commit(ours, ours_id, "rebase", f"rebase onto {onto}", prev_lsn=up_lsn,
                             prev_branch_id=up.branch_id)
            info["schema_changes"] = schema
            return info
        finally:
            self._close(up_conn)
            if theirs_conn is not None:
                self._close(theirs_conn)
            if base_conn is not None:
                self._close(base_conn)
            for bid in {theirs_id, base_id}:
                if bid:
                    self._fdw_cleanup(ours, bid)

    # -- reset / revert ------------------------------------------------------

    def _reset_impl(self, ref: Ref, to: str) -> None:
        bid = self._on(ref)
        row = self._find_commit(self.conn, to)
        if not row["lsn"]:
            raise ValueError(f"commit {to} has no LSN")
        src = row["branch_id"] or bid
        n = len([k for k in self._branches if k.startswith(f"{ref.branch}_bk_")])
        body = {"source_branch_id": src, "source_lsn": row["lsn"],
                "preserve_under_name": f"{ref.branch}_bk_{n + 1}_{int(time.time()) % 100000}"}
        resp = self._api("POST", f"projects/{self.project_id}/branches/{bid}/restore", json=body)
        self._wait(resp.get("operations"))
        self._refresh_branches()
        # The compute restarts: reconnect, then cut the log at the commit.
        if self.conn is not None:
            self._close(self.conn)
            self.conn = None
        self._current_ref = None
        self._conn_branch_id = None
        self._on(ref)
        self._exec(self.conn, f'DELETE FROM "{COMMITS_TABLE}" WHERE seq > %s', (int(row["seq"]),))
        self._exec(self.conn, f'UPDATE "{COMMITS_TABLE}" SET lsn = %s WHERE seq = %s', (row["lsn"], int(row["seq"])))

    def _revert_impl(self, ref: Ref, commit: str) -> None:
        bid = self._on(ref)
        ours = self.conn
        row = self._find_commit(ours, commit)
        if not row["prev_lsn"]:
            raise ValueError(f"commit {commit} has no predecessor to revert to")
        after = _State(None, row["branch_id"] or bid, row["lsn"])
        before = _State(None, row["prev_branch_id"] or after.branch_id, row["prev_lsn"])
        a_id, b_id = self._reading_branch(after), self._reading_branch(before)
        ca, cb = self._open(a_id), self._open(b_id)
        try:
            tables = [t for t in self._tables(ours) if t in set(self._tables(ca)) and t in set(self._tables(cb))]
            changed = [t for t in tables if self._table_hash(ca, t) != self._table_hash(cb, t)]
            if not changed:
                return
            s_a = self._fdw_schema(ours, bid, a_id, changed)
            s_b = self._fdw_schema(ours, bid, b_id, changed)
            changed = self._fk_order(ours, changed)
            self._exec(ours, "BEGIN")
            try:
                for t in reversed(changed):
                    cols = [c[0] for c in self._columns(ours, t) if c[0] in {x[0] for x in self._columns(ca, t)}]
                    pk = [c for c in self._pk(ours, t) if c in cols]
                    if not pk:
                        continue
                    rest = [c for c in cols if c not in pk]
                    types = {c[0]: self._type_sql(c) for c in self._columns(ours, t)}
                    self._pull(ours, s_a, t, "_bb_a", cols, available={c[0] for c in self._columns(ca, t)}, types=types)
                    self._pull(ours, s_b, t, "_bb_b", cols, available={c[0] for c in self._columns(cb, t)}, types=types)
                    O = _q(t)
                    # Rows the commit added: delete them (children first).
                    self._exec(ours, f'DELETE FROM {O} o WHERE EXISTS (SELECT 1 FROM "_bb_a" e LEFT JOIN "_bb_b" s ON {self._join("s", "e", pk)} '
                                     f'WHERE s.{_q(pk[0])} IS NULL AND {self._join("e", "o", pk)})')
                    self._exec(ours, f'ALTER TABLE "_bb_a" RENAME TO "_bb_a_{t}"; ALTER TABLE "_bb_b" RENAME TO "_bb_b_{t}"')
                for t in changed:
                    cols = [c[0] for c in self._columns(ours, t) if c[0] in {x[0] for x in self._columns(ca, t)}]
                    pk = [c for c in self._pk(ours, t) if c in cols]
                    if not pk:
                        continue
                    rest = [c for c in cols if c not in pk]
                    O = _q(t)
                    # Rows it deleted or modified: put the earlier version back (parents first).
                    differs = f"e.{_q(pk[0])} IS NULL" + (f" OR NOT ({self._same('s', 'e', rest)})" if rest else "")
                    self._exec(ours, self._upsert(O, cols, pk, f'SELECT {self._cols("s", cols)} FROM "_bb_b_{t}" s LEFT JOIN "_bb_a_{t}" e ON {self._join("s", "e", pk)} WHERE {differs}'))
                self._exec(ours, "COMMIT")
            except BaseException:
                self._exec(ours, "ROLLBACK")
                raise
            finally:
                for t in changed:
                    for tmp in (f"_bb_a_{t}", f"_bb_b_{t}", "_bb_a", "_bb_b"):
                        try:
                            self._exec(ours, f'DROP TABLE IF EXISTS {_q(tmp)}')
                        except Exception:
                            pass
            head = self._log_rows(ours, limit=1)[0]
            self._add_commit(ours, bid, "commit", f"revert {commit}", prev_lsn=head["lsn"], prev_branch_id=head["branch_id"])
        finally:
            self._close(ca)
            self._close(cb)
            for x in {a_id, b_id}:
                self._fdw_cleanup(ours, x)

    # -- delete --------------------------------------------------------------

    def _delete_branch(self, branch_id: str) -> None:
        resp = self._api("DELETE", f"projects/{self.project_id}/branches/{branch_id}")
        try:
            self._wait(resp.get("operations"))
        except RuntimeError as e:
            # A compute suspend that failed alongside the delete is harmless
            # once the branch is gone.
            try:
                self._api("GET", f"projects/{self.project_id}/branches/{branch_id}")
            except NeonAPIError as g:
                if g.status == 404:
                    print(f"Warning: delete of {branch_id}: {e} (branch is gone)")
                else:
                    raise e
            else:
                raise
        with type(self)._ACT_LOCK:
            type(self)._ACTIVE.pop(branch_id, None)
            type(self)._HOLDS.pop(branch_id, None)

    def _flush_deferred(self) -> None:
        still = []
        for name, bid in self._deferred_deletes:
            try:
                self._delete_branch(bid)
                self._branches.pop(name, None)
            except NeonAPIError as e:
                if "child" in e.text.lower():
                    still.append((name, bid))
                else:
                    print(f"Warning: deferred delete of {name} failed: {e}")
        self._deferred_deletes = still

    def _delete_impl(self, ref: Ref) -> None:
        bid = self._branch_id(ref.branch)
        if self._current_ref and self._current_ref.branch == ref.branch:
            if self.conn:
                self._close(self.conn)
                self.conn = None
            self._current_ref = None
            self._conn_branch_id = None
        try:
            self._delete_branch(bid)
        except NeonAPIError as e:
            if "child" in e.text.lower():
                # Neon refuses to delete a branch with children: retried once
                # they are gone (later deletes, close_connection).
                self._deferred_deletes.append((ref.branch, bid))
                self.observe("deferred_deletes")
                return
            raise
        self._branches.pop(ref.branch, None)
        self._uris.pop(bid, None)
        self._endpoints.pop(bid, None)
        if self._deferred_deletes:
            self._flush_deferred()

    def _qualified_table(self, ref: Ref, table: str) -> str:
        raise self._unsupported("multi_ref_exec", "no multi-branch query semantics; "
                                "cross-branch reads run once per branch")

    def close_connection(self) -> None:
        self._closed = True
        if getattr(self, "_size_conn", None) is not None:
            try:
                self._close(self._size_conn)
            except Exception:
                pass
            self._size_conn = None
        for key, bid in list(self._temp.items()):
            try:
                self._delete_branch(bid)
            except Exception as e:
                print(f"Warning: could not delete temporary branch {bid}: {e}")
        self._temp.clear()
        if self._deferred_deletes:
            self._flush_deferred()
        if self.conn is not None:
            self._close(self.conn)
            self.conn = None
        super().close_connection()

    # ------------------------------------------------------------------
    # Async: a psycopg pool on the branch the suite is on when the pool is
    # opened; a script on another branch gets its own connection.
    # ------------------------------------------------------------------

    async def open_async_pool(self, size: int) -> None:
        from psycopg_pool import AsyncConnectionPool

        if self._current_ref is None:
            raise ValueError("Not connected to a branch")
        self._pool_branch = self._current_ref.branch
        self.async_pool = AsyncConnectionPool(
            self._uri(self._branch_id(self._pool_branch)),
            min_size=size, max_size=size, kwargs={"autocommit": True}, open=False,
        )
        await self.async_pool.open(wait=True)

    async def _connect_impl_async(self, conn, ref: Ref):
        if ref.branch == self._pool_branch:
            return conn
        import psycopg

        return await psycopg.AsyncConnection.connect(
            self._uri(self._branch_id(ref.branch)), autocommit=True
        )

    # ------------------------------------------------------------------
    # Consumption metrics (macrobench accounting)
    # ------------------------------------------------------------------

    @classmethod
    def get_consumption_metrics(cls, project_id, org_id=None):
        """All consumption entries for a project in a 2-day window around
        now (hourly granularity), plus a summary of the most recent entry.
        Returns None on failure."""
        org_id = org_id or os.environ.get("NEON_ORG_ID", "")
        if not org_id:
            try:
                orgs = api_request("GET", "users/me/organizations").get("organizations", [])
                org_id = orgs[0]["id"] if orgs else ""
            except Exception:
                org_id = ""
        if not org_id:
            print("Warning: NEON_ORG_ID not set, skipping consumption metrics")
            return None
        now = datetime.now(timezone.utc)
        start = (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end = (now + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        endpoint = (
            f"consumption_history/v2/projects"
            f"?org_id={org_id}&project_ids={project_id}"
            f"&from={start}&to={end}&granularity=hourly"
            f"&metrics=root_branch_bytes_month,child_branch_bytes_month,"
            f"compute_unit_seconds,public_network_transfer_bytes,"
            f"private_network_transfer_bytes"
        )
        try:
            resp = api_request("GET", endpoint)
        except Exception as e:
            print(f"Warning: consumption metrics request failed: {e}")
            return None
        try:
            projects = resp.get("projects", [])
            if not projects:
                return None
            all_entries = []
            for period in projects[0].get("periods", []):
                for entry in period.get("consumption", []):
                    metrics = entry.get("metrics", [])
                    if metrics:
                        d = {"period_id": period.get("period_id", ""), "timestamp": entry.get("timestamp", "")}
                        for m in metrics:
                            d[m["metric_name"]] = m["value"]
                        all_entries.append(d)
            if not all_entries:
                return None
            most_recent = all_entries[-1]
            return {"all_metrics": all_entries, "count": len(all_entries),
                    "summary": {k: v for k, v in most_recent.items() if k not in ("period_id", "timestamp")}}
        except (KeyError, IndexError) as e:
            print(f"Warning: could not parse consumption metrics: {e}")
            return None
