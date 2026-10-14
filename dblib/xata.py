"""Xata backend.

Branches are Xata branches (REST API), each its own Postgres instance, so
connecting means opening a new connection. Xata has no commits; the verbs
around them are built from child branches and a bookkeeping table:

    branch     POST /branches with parentID, waiting until the compute is
               ready; from "branch@commit" the parent is the commit's
               frozen branch. A fork row is then written on the new branch
    commit     a row in the branch's _bb_commits table plus a child branch
               of the working branch that nothing writes to (the frozen
               branch), hibernated once it is ready
    log        the _bb_commits rows, walked from the head (or the commit)
               along their parent ids
    diff       the hash-bucket comparison of the Neon backend, with the row
               comparison done in this process: each side hashes its own
               tables per bucket, the rows of the buckets that differ are
               read from both sides and compared here. Neither side is
               written to
    delete     DELETE /branches/{id}

A commit ref ("branch@commit") is the commit's frozen branch: branch forks
from it, exec and diff reactivate it and connect to it.

reset, merge, rebase and revert are not provided (IMPLEMENTATION_NOTES says why). Xata's restore
endpoint is no in-place rollback (see IMPLEMENTATION_NOTES["reset"]).
"""

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Tuple
from dotenv import load_dotenv
import psycopg2
import requests

from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite, Ref
import dblib.result_collector as rc

load_dotenv()
API_KEY = os.environ.get("XATA_API_KEY", "")

# Defaults target Xata cloud; override these (e.g. in .env) to point at a
# self-hosted instance:
#   XATA_API_URL          base URL of the Xata REST API
#   XATA_ORGANIZATION_ID  organization that owns the projects
#   XATA_REGION           region new projects are created in
#   XATA_INSTANCE_TYPE    compute size of a project's "main" branch
#   XATA_POSTGRES_IMAGE   Postgres image (full name, e.g. postgres:<version>)
XATA_API_URL = os.environ.get("XATA_API_URL", "https://api.xata.tech").rstrip("/")
# Everything runs in one pre-created organization.
XATA_ORGANIZATION_ID = os.environ.get("XATA_ORGANIZATION_ID", "")
XATA_API_BASE_URL = f"{XATA_API_URL}/organizations/{XATA_ORGANIZATION_ID}/"

# Compute for the "main" branch of a new project (children inherit it).
XATA_REGION = os.environ.get("XATA_REGION", "us-east-1")
XATA_INSTANCE_TYPE = os.environ.get("XATA_INSTANCE_TYPE", "xata.medium")
XATA_POSTGRES_IMAGE = os.environ.get("XATA_POSTGRES_IMAGE", "postgres:18.0")
# Hibernate a commit's frozen branch once it is ready (it holds state, it
# needs no compute until something reads the commit).
XATA_HIBERNATE_COMMITS = os.environ.get("XATA_HIBERNATE_COMMITS", "1").lower() in ("1", "true", "yes")

META_PREFIX = "_bb_"
COMMITS_TABLE = "_bb_commits"
# Name prefix of the frozen branches behind commits (hidden from list_branches).
COMMIT_BRANCH_PREFIX = "bbc-"
_COMMITS_DDL = f"""
CREATE TABLE IF NOT EXISTS "{COMMITS_TABLE}" (
    seq BIGINT PRIMARY KEY,
    id TEXT NOT NULL,
    kind TEXT NOT NULL,
    message TEXT,
    branch TEXT,
    frozen_branch_id TEXT,
    parent_id TEXT,
    ts TIMESTAMPTZ DEFAULT now()
)
"""


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _hashable(v):
    """A row value as something usable in a dict key (arrays and json
    come back as lists and dicts)."""
    if isinstance(v, (list, tuple)):
        return tuple(_hashable(x) for x in v)
    if isinstance(v, dict):
        return tuple(sorted((k, _hashable(x)) for k, x in v.items()))
    if isinstance(v, memoryview):
        return bytes(v)
    return v


class XataToolSuite(DBToolSuite):
    BACKEND_NAME = "xata"
    SUPPORTS_COMMIT_REFS = True
    SUPPORTS_MULTI_REF_EXEC = False
    IMPLEMENTATION = {
        "commit": "composed",
        "log": "simulated",
        "diff": "composed",
        "commit_refs": "composed",
    }
    IMPLEMENTATION_NOTES = {
        "branch": "POST /branches with parentID, waiting for the compute; from a commit ref the "
                  "parent is the commit's frozen branch",
        "commit": "_bb_commits row + a child branch of the working branch that is never written "
                  "to (hibernated once ready). The child is a snapshot taken shortly after the "
                  "POST, not at it, so writes by other connections during the commit can be in it",
        "log": "_bb_commits rows walked along their parent ids",
        "diff": "the Neon backend's hash-bucket diff (256 buckets on the integer primary-key "
                "columns, COUNT and SUM(hashtext(row)) per bucket computed on each side), with "
                "the rows of differing buckets read from both sides and compared client-side. "
                "Client-side because postgres_fdw cannot reach across branches: the branch host "
                "names (nip.io) do not resolve inside a branch pod and the per-branch "
                "NetworkPolicy blocks egress to other branches. Both sides are read in read-only "
                "transactions. A table whose columns differ between the sides is not compared "
                "and is listed under skipped_tables: a deliberate divergence from Neon, which "
                "diffs the columns the two sides share, so where a diff crosses a schema change "
                "on a table both sides have, a Xata/Neon difference in the counts is "
                "definitional, not a discrepancy. Whether a row is modified is decided by "
                "Python equality of the fetched values, so a NaN value reads as modified where "
                "Neon's IS DISTINCT FROM would not",
        "commit_refs": "the commit's frozen branch: branch forks from it, exec and diff "
                       "reactivate it and connect to it",
        "reset": "not provided: POST /branches/{id}/restore is no in-place rollback. It creates "
                 "a new branch (new id, parentID = the source) by replaying the source's WAL "
                 "archive to the latest archived segment; the source is unchanged, the request "
                 "takes no time or LSN (such fields are ignored), and writes are missing until "
                 "their WAL segment is archived. Scenarios branch from the commit ref instead",
        "merge": "not provided: Xata has no merge of branches. The Neon backend simulates one "
                 "as a SQL three-way merge over postgres_fdw, which cannot reach across Xata "
                 "branches (see diff); a client-side one is not built",
        "rebase": "not provided: no Xata equivalent; it would be the same three-way merge as "
                  "merge, replayed onto the new base",
        "revert": "not provided: no Xata equivalent; it would need the inverse of a commit's "
                  "delta against its predecessor applied to the head, which is not built",
    }

    # ------------------------------------------------------------------
    # Project-level helpers
    # ------------------------------------------------------------------

    @classmethod
    def add_db_name_to_connection_string(cls, connection_string: str, db_name: str) -> str:
        conn_components = connection_string.split("/")
        conn_components[-1] = db_name
        return "/".join(conn_components) + "?sslmode=require"

    @classmethod
    def create_xata_project(cls, project_name: str) -> Tuple[str, str, str, str]:
        """Create a project with a "main" branch; returns (project id,
        branch id, branch name, connection uri for the postgres db)."""
        project_details = cls._request("POST", "projects", json={"name": project_name})
        endpoint = f"projects/{project_details['id']}/branches"
        branch_payload = {
            "mode": "custom",
            "name": "main",
            "scaleToZero": {"enabled": True, "inactivityPeriodMinutes": 30},
            "configuration": {
                "region": XATA_REGION,
                "instanceType": XATA_INSTANCE_TYPE,
                "image": XATA_POSTGRES_IMAGE,
                "replicas": 0,
            },
        }
        default_branch = cls._request("POST", endpoint, json=branch_payload)
        cls._poll_branch_active(
            project_details["id"],
            default_branch["id"],
            initial_conn_string=default_branch.get("connectionString"),
            initial_status_type=(default_branch.get("status") or {}).get("statusType", ""),
        )
        return (
            project_details["id"],
            default_branch["id"],
            default_branch["name"],
            cls._get_xata_connection_uri(project_details["id"], default_branch["id"], "postgres"),
        )

    @classmethod
    def delete_project(cls, project_id: str) -> None:
        """Delete every branch, then the project (as the API requires)."""
        response = cls._request("GET", f"projects/{project_id}/branches")
        for branch in response.get("branches", []):
            cls._request("DELETE", f"projects/{project_id}/branches/{branch['id']}")
        time.sleep(2)
        cls._request("DELETE", f"projects/{project_id}")

    @classmethod
    def init_for_bench(
        cls,
        result_collector: rc.ResultCollector,
        project_id: str,
        branch_id: str,
        branch_name: str,
        database_name: str,
        measure_storage: bool = False,
    ):
        uri = cls._get_xata_connection_uri(project_id, branch_id, database_name)
        conn = psycopg2.connect(uri)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return cls(conn, result_collector, project_id, branch_name, branch_id,
                   database_name, measure_storage)

    @classmethod
    def _request(cls, method: str, endpoint: str, **kwargs):
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {API_KEY}"
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"
        r = requests.request(method, XATA_API_BASE_URL + endpoint, headers=headers, **kwargs)
        r.raise_for_status()
        if r.status_code == 204 or not r.content:
            return {}
        return r.json()

    _READY_STATUSES = {"STATUS_TYPE_ACTIVE", "STATUS_TYPE_HEALTHY"}

    @classmethod
    def _poll_branch_active(
        cls,
        project_id: str,
        branch_id: str,
        initial_conn_string: str = None,
        initial_status_type: str = "",
        max_attempts: int = 30,
        interval: float = 10.0,
    ) -> str:
        """Block until the branch has a connection string and a ready status
        (a string can appear while the compute is still transient)."""
        # NOTE: this waits on the branch's deprecated ``connectionString``
        # field; it should migrate to the /credentials endpoint later.
        conn_string = initial_conn_string
        status_type = initial_status_type
        endpoint = f"projects/{project_id}/branches/{branch_id}"
        for _ in range(max_attempts):
            if conn_string and status_type in cls._READY_STATUSES:
                return conn_string
            time.sleep(interval)
            details = cls._request("GET", endpoint)
            conn_string = details.get("connectionString")
            status_type = (details.get("status") or {}).get("statusType", "")
        if not conn_string:
            raise RuntimeError(
                f"Branch {branch_id} connection string not available after {max_attempts} attempts"
            )
        raise RuntimeError(
            f"Branch {branch_id} not active after {max_attempts} attempts (status: {status_type})"
        )

    @classmethod
    def _get_xata_connection_uri(cls, project_id: str, branch_id: str, db_name: str) -> str:
        # The branch's own ``connectionString`` field is deprecated (its host
        # carries a "-deprecated" marker); the credentials endpoint is current.
        response = cls._request(
            "GET", f"projects/{project_id}/branches/{branch_id}/credentials"
        )
        return cls.add_db_name_to_connection_string(response["connectionString"], db_name)

    # ------------------------------------------------------------------

    def __init__(
        self,
        connection: _pgconn,
        result_collector: rc.ResultCollector,
        project_id: str,
        branch_name: str,
        branch_id: str,
        database_name: str = None,
        measure_storage: bool = False,
    ):
        super().__init__(connection, result_collector, measure_storage)
        self.project_id = project_id
        self.db_name = database_name or connection.get_dsn_parameters()["dbname"]
        self.current_branch_id = branch_id
        branch_name = branch_name or "main"
        self._all_branches = {branch_name: (branch_id, None)}
        self._uris: dict = {}            # branch id -> connection uri
        self._current_ref = Ref(branch_name)

    def _get_xata_branches(self) -> dict:
        response = self.__class__._request("GET", f"projects/{self.project_id}/branches")
        return {r["name"]: (r["id"], r.get("parent_id", None)) for r in response["branches"]}

    def _branch_id(self, name: str) -> str:
        info = self._all_branches.get(name)
        if info and info[0]:
            return info[0]
        all_branches = self._get_xata_branches()
        if name not in all_branches:
            raise ValueError(f"Branch '{name}' does not exist.")
        bid = all_branches[name][0]
        self._all_branches[name] = (bid, None)
        return bid

    def _uri_for(self, name: str) -> str:
        bid = self._branch_id(name)
        uri = self._all_branches[name][1]
        if not uri:
            uri = self.__class__._get_xata_connection_uri(self.project_id, bid, self.db_name)
            self._all_branches[name] = (bid, uri)
        return uri

    def list_branches(self) -> list:
        return [n for n in self._get_xata_branches() if not n.startswith(COMMIT_BRANCH_PREFIX)]

    def _uri_by_id(self, branch_id: str) -> str:
        uri = self._uris.get(branch_id)
        if not uri:
            uri = self.__class__._get_xata_connection_uri(self.project_id, branch_id, self.db_name)
            self._uris[branch_id] = uri
        return uri

    def _open(self, branch_id: str):
        conn = psycopg2.connect(self._uri_by_id(branch_id))
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def _conn_for(self, branch: str):
        """self.conn when it is on that branch's head, else a short-lived
        connection (callers close it through _release)."""
        if self.conn is not None and not self.conn.closed and self._current_ref == Ref(branch):
            return self.conn
        return self._open(self._branch_id(branch))

    def _release(self, conn) -> None:
        if conn is not None and conn is not self.conn:
            try:
                conn.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Bookkeeping (commit log)
    # ------------------------------------------------------------------

    _LOG_COLS = ("seq", "id", "kind", "message", "branch", "frozen_branch_id", "parent_id", "ts")

    def _exec(self, conn, sql: str, args=None):
        self._check_cancel()
        with conn.cursor() as cur:
            cur.execute(sql, args)
            if cur.description is not None:
                return cur.fetchall()
            return None

    def _has_meta(self, conn) -> bool:
        return bool(self._exec(conn, "SELECT to_regclass(%s) IS NOT NULL", (f'"{COMMITS_TABLE}"',))[0][0])

    def _log_rows(self, conn) -> list:
        """Every log row of the branch, newest first."""
        if not self._has_meta(conn):
            return []
        rows = self._exec(conn, f'SELECT {", ".join(self._LOG_COLS)} FROM "{COMMITS_TABLE}" ORDER BY seq DESC')
        return [dict(zip(self._LOG_COLS, r)) for r in rows or []]

    def _head_row(self, conn) -> dict:
        if not self._has_meta(conn):
            return None
        rows = self._exec(conn, f'SELECT {", ".join(self._LOG_COLS)} FROM "{COMMITS_TABLE}" ORDER BY seq DESC LIMIT 1')
        return dict(zip(self._LOG_COLS, rows[0])) if rows else None

    def _find_commit(self, conn, commit_id: str) -> dict:
        rows = None
        if self._has_meta(conn):
            rows = self._exec(conn, f'SELECT {", ".join(self._LOG_COLS)} FROM "{COMMITS_TABLE}" '
                                    "WHERE id = %s ORDER BY seq DESC LIMIT 1", (str(commit_id),))
        if not rows:
            raise ValueError(f"unknown commit '{commit_id}'")
        return dict(zip(self._LOG_COLS, rows[0]))

    def _add_row(self, conn, kind: str, message: str, branch: str, parent_id: str) -> tuple:
        """Append a log row; returns (seq, id)."""
        self._exec(conn, _COMMITS_DDL)
        seq = time.time_ns()
        for _ in range(8):
            try:
                self._exec(conn, f'INSERT INTO "{COMMITS_TABLE}" (seq, id, kind, message, branch, parent_id) '
                                 "VALUES (%s, %s, %s, %s, %s, %s)",
                           (seq, f"{seq:x}", kind, message or "", branch, parent_id))
                return seq, f"{seq:x}"
            except psycopg2.IntegrityError:
                seq += 1
        raise RuntimeError("could not record commit")

    def _commit_row(self, ref: Ref) -> dict:
        """The log row of ``ref``'s commit, read on ``ref``'s branch; it
        names the frozen branch that holds the commit's state."""
        conn = self._conn_for(ref.branch)
        try:
            row = self._find_commit(conn, ref.commit)
        finally:
            self._release(conn)
        if row["kind"] != "commit" or not row["frozen_branch_id"]:
            raise ValueError(f"'{ref.commit}' on {ref.branch} is not a commit with a frozen branch")
        return row

    def _set_hibernate(self, branch_id: str, hibernate: bool) -> None:
        self.__class__._request("PATCH", f"projects/{self.project_id}/branches/{branch_id}",
                                json={"hibernate": hibernate})

    def _wake(self, branch_id: str) -> None:
        """Reactivate a hibernated branch (the gateway refuses connections
        to one) and wait until its compute is ready."""
        details = self.__class__._request("GET", f"projects/{self.project_id}/branches/{branch_id}")
        if (details.get("status") or {}).get("statusType") != "STATUS_TYPE_HIBERNATED":
            return
        self._set_hibernate(branch_id, False)
        self.__class__._poll_branch_active(self.project_id, branch_id, interval=2.0, max_attempts=150)

    def _commit_uri(self, ref: Ref) -> tuple:
        """(frozen branch id, connection uri) for a commit ref, with the
        frozen branch's compute running."""
        bid = self._commit_row(ref)["frozen_branch_id"]
        self._wake(bid)
        return bid, self._uri_by_id(bid)

    # ------------------------------------------------------------------
    # Table metadata and hashes (same SQL as the Neon backend, so that the
    # two report the same diff)
    # ------------------------------------------------------------------

    def _tables(self, conn) -> list:
        rows = self._exec(conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "
                                "AND table_type = 'BASE TABLE' AND table_name NOT LIKE %s ORDER BY table_name",
                          (META_PREFIX.replace("_", r"\_") + "%",))
        return [r[0] for r in rows or []]

    def _columns(self, conn, table: str) -> list:
        """(name, type, length, precision, scale) per column, in order."""
        rows = self._exec(conn, "SELECT column_name, data_type, character_maximum_length, numeric_precision, "
                                "numeric_scale FROM information_schema.columns "
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

    _NB = 256  # hash buckets per table for change detection and partial reads

    def _bucket_expr(self, columns: list, pk: list) -> str:
        """SQL for a row's bucket (0..255) from its integer primary-key
        columns, or None when the key has none."""
        types = {c[0]: c[1] for c in columns}
        parts = []
        for c in pk:
            t = types.get(c)
            if t in ("integer", "smallint"):
                parts.append(f"hashint4({_q(c)}::int)")
            elif t == "bigint":
                parts.append(f"hashint8({_q(c)})")
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

    def _open_reader(self, branch_id: str):
        """A connection that reads one snapshot of the branch and cannot
        write to it."""
        conn = psycopg2.connect(self._uri_by_id(branch_id))
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        return conn

    def _reading_branch(self, ref: Ref) -> str:
        """Id of the branch that holds ``ref``'s state, with its compute
        running: the branch itself, or a commit's frozen branch."""
        return self._commit_uri(ref)[0] if ref.commit else self._branch_id(ref.branch)

    def _rows(self, conn, table: str, cols: list, where: str) -> list:
        rows = self._exec(conn, f'SELECT {", ".join(_q(c) for c in cols)} FROM {_q(table)}'
                                + (f" WHERE {where}" if where else ""))
        return [tuple(_hashable(v) for v in r) for r in rows or []]

    @staticmethod
    def _count_changes(rows_a: list, rows_b: list, n_key: int) -> tuple:
        """(added, deleted, modified) from a to b; each row is its key
        columns (the first ``n_key``) followed by the others. As in the SQL
        join this replaces, a key with a NULL in it matches nothing."""
        def index(rows):
            out = {}
            for r in rows:
                k = r[:n_key]
                if None not in k:
                    out[k] = r
            return out
        a, b = index(rows_a), index(rows_b)
        added = sum(1 for r in rows_b if None in r[:n_key] or r[:n_key] not in a)
        deleted = sum(1 for r in rows_a if None in r[:n_key] or r[:n_key] not in b)
        modified = sum(1 for k, r in a.items() if k in b and b[k] != r)
        return added, deleted, modified

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def _connect_impl(self, ref: Ref) -> None:
        if ref.commit:
            bid, uri = self._commit_uri(ref)
        else:
            uri = self._uri_for(ref.branch)
            bid = self._all_branches[ref.branch][0]
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = psycopg2.connect(uri)
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.current_branch_id = bid

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        """Create a branch inheriting from its parent and wait until its
        compute is ready (that wait is part of the BRANCH latency). From a
        commit ref the parent is the commit's frozen branch."""
        commit = self._commit_row(from_ref) if from_ref.commit else None
        parent_id = commit["frozen_branch_id"] if commit else self._branch_id(from_ref.branch)
        res = self.__class__._request(
            "POST",
            f"projects/{self.project_id}/branches",
            json={"mode": "inherit", "name": name, "parentID": parent_id},
        )
        branch_id = res["id"]
        self.__class__._poll_branch_active(
            self.project_id,
            branch_id,
            initial_conn_string=res.get("connectionString"),
            initial_status_type=(res.get("status") or {}).get("statusType", ""),
        )
        uri = self.__class__._get_xata_connection_uri(self.project_id, branch_id, self.db_name)
        self._all_branches[name] = (branch_id, uri)
        self._uris[branch_id] = uri
        # The fork row: the new branch's history continues from the source's
        # head row, which the branch inherited with the table.
        conn = self._open(branch_id)
        try:
            if commit:
                # The frozen branch was cloned before its own row got the
                # frozen branch id; give the copy the value the source has.
                self._exec(conn, f'DELETE FROM "{COMMITS_TABLE}" WHERE seq > %s', (commit["seq"],))
                self._exec(conn, f'UPDATE "{COMMITS_TABLE}" SET frozen_branch_id = %s WHERE seq = %s',
                           (commit["frozen_branch_id"], commit["seq"]))
            head = self._head_row(conn)
            self._add_row(conn, "fork", f"fork from {from_ref}", name, head["id"] if head else None)
        finally:
            self._release(conn)

    def _commit_impl(self, ref: Ref, message: str) -> str:
        """Record the commit, then freeze the branch's state in a child
        branch. The row goes first so that the frozen branch carries it."""
        name = ref.branch
        working_id = self._branch_id(name)
        conn = self._conn_for(name)
        try:
            head = self._head_row(conn)
            seq, commit_id = self._add_row(conn, "commit", message, name, head["id"] if head else None)
            try:
                res = self.__class__._request(
                    "POST",
                    f"projects/{self.project_id}/branches",
                    json={"mode": "inherit", "name": f"{COMMIT_BRANCH_PREFIX}{commit_id}", "parentID": working_id},
                )
                frozen_id = res["id"]
                self.__class__._poll_branch_active(
                    self.project_id,
                    frozen_id,
                    initial_conn_string=res.get("connectionString"),
                    initial_status_type=(res.get("status") or {}).get("statusType", ""),
                )
            except BaseException:
                # No frozen branch: take the row back (not through _exec,
                # which would raise again on a cancelled operation).
                try:
                    with conn.cursor() as cur:
                        cur.execute(f'DELETE FROM "{COMMITS_TABLE}" WHERE seq = %s', (seq,))
                except Exception:
                    pass
                raise
            self._exec(conn, f'UPDATE "{COMMITS_TABLE}" SET frozen_branch_id = %s WHERE seq = %s', (frozen_id, seq))
        finally:
            self._release(conn)
        if XATA_HIBERNATE_COMMITS:
            try:
                self._set_hibernate(frozen_id, True)
            except Exception as e:
                print(f"Warning: could not hibernate frozen branch {frozen_id}: {e}")
        return commit_id

    def _log_impl(self, ref: Ref, limit: int) -> list:
        conn = self._conn_for(ref.branch)
        try:
            if ref.commit:
                self._find_commit(conn, ref.commit)
            rows = self._log_rows(conn)
        finally:
            self._release(conn)
        by_id = {r["id"]: r for r in rows}
        cur = by_id.get(ref.commit) if ref.commit else (rows[0] if rows else None)
        out, seen = [], set()
        while cur is not None and cur["id"] not in seen and not (limit and len(out) >= limit):
            seen.add(cur["id"])
            out.append({"commit_hash": cur["id"], "kind": cur["kind"], "committer": "xata", "email": "",
                        "date": cur["ts"], "message": cur["message"], "branch": cur["branch"],
                        "frozen_branch_id": cur["frozen_branch_id"]})
            cur = by_id.get(cur["parent_id"])
        return out

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        a_id, b_id = self._reading_branch(ref_a), self._reading_branch(ref_b)
        if a_id == b_id:
            return {"tables": [], "rows_added": 0, "rows_deleted": 0, "rows_modified": 0}
        ca = cb = None
        try:
            ca, cb = self._open_reader(a_id), self._open_reader(b_id)
            ta, tb = set(self._tables(ca)), set(self._tables(cb))
            out, skipped = [], []
            for t in sorted(ta | tb):
                if t in ta and t not in tb:
                    out.append({"table_name": t, "rows_added": 0, "rows_deleted": self._table_hash(ca, t)[0], "rows_modified": 0})
                    continue
                if t in tb and t not in ta:
                    out.append({"table_name": t, "rows_added": self._table_hash(cb, t)[0], "rows_deleted": 0, "rows_modified": 0})
                    continue
                columns = self._columns(cb, t)
                if columns != self._columns(ca, t):
                    skipped.append(t)
                    continue
                pk0 = self._pk(cb, t)
                bexpr = self._bucket_expr(columns, pk0) if pk0 else None
                if bexpr is None:
                    if self._table_hash(ca, t) == self._table_hash(cb, t):
                        continue
                    where = None
                else:
                    diffb = self._changed_buckets(self._bucket_hashes(ca, t, bexpr), self._bucket_hashes(cb, t, bexpr))
                    if not diffb:
                        continue
                    where = f"{bexpr} IN ({', '.join(str(b) for b in diffb)})"
                cols = [c[0] for c in columns]
                pk = pk0 or cols
                ordered = pk + [c for c in cols if c not in pk]
                added, deleted, modified = self._count_changes(
                    self._rows(ca, t, ordered, where), self._rows(cb, t, ordered, where), len(pk))
                out.append({"table_name": t, "rows_added": added, "rows_deleted": deleted, "rows_modified": modified})
            res = {"tables": out, "rows_added": sum(x["rows_added"] for x in out),
                   "rows_deleted": sum(x["rows_deleted"] for x in out),
                   "rows_modified": sum(x["rows_modified"] for x in out)}
            if skipped:
                res["skipped_tables"] = skipped
            return res
        finally:
            for conn in (ca, cb):
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def _delete_impl(self, ref: Ref) -> None:
        bid = self._branch_id(ref.branch)
        self.__class__._request("DELETE", f"projects/{self.project_id}/branches/{bid}")
        self._all_branches.pop(ref.branch, None)
        if self._current_ref and self._current_ref.branch == ref.branch:
            self._current_ref = None

    def _get_branch_instance_ids(self, branch_id: str) -> list:
        details = self.__class__._request("GET", f"projects/{self.project_id}/branches/{branch_id}")
        instances = details.get("status", {}).get("instances", [])
        return [inst["id"] for inst in instances]

    def _get_branch_disk_bytes(self, branch_id: str) -> int:
        instance_ids = self._get_branch_instance_ids(branch_id)
        if not instance_ids:
            return 0
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=5)
        payload = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "metric": "disk",
            "instances": instance_ids,
            "aggregations": ["max"],
        }
        response = self.__class__._request(
            "POST", f"projects/{self.project_id}/branches/{branch_id}/metrics", json=payload
        )
        max_bytes = 0
        for series in response.get("series", []):
            for point in series.get("values", []):
                max_bytes = max(max_bytes, point.get("value", 0))
        return int(max_bytes)

    def _storage_bytes(self) -> int:
        """Sum of the ``disk`` metric over all branches. This is the
        logical per-instance size, so shared copy-on-write blocks are
        counted once per branch."""
        try:
            return sum(
                self._get_branch_disk_bytes(bid)
                for _, (bid, _) in self._get_xata_branches().items()
            )
        except Exception as e:
            print(f"Warning: Could not get Xata storage metrics: {e}")
            return 0

    # ------------------------------------------------------------------
    # Async
    # ------------------------------------------------------------------

    async def open_async_pool(self, size: int) -> None:
        from psycopg_pool import AsyncConnectionPool

        if self._current_ref is None:
            raise ValueError("Not connected to a branch")
        self._pool_branch = self._current_ref.branch
        self.async_pool = AsyncConnectionPool(
            self._uri_for(self._pool_branch),
            min_size=size, max_size=size, kwargs={"autocommit": True}, open=False,
        )
        await self.async_pool.open(wait=True)

    async def _connect_impl_async(self, conn, ref: Ref):
        if ref.branch == self._pool_branch and not ref.commit:
            return conn
        import psycopg

        uri = self._commit_uri(ref)[1] if ref.commit else self._uri_for(ref.branch)
        return await psycopg.AsyncConnection.connect(uri, autocommit=True)
