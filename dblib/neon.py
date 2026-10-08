"""Neon backend.

Branches are Neon branches (REST API). A branch is its own Postgres
endpoint, so connecting to one means opening a new connection. Neon has no
commits: commit/diff/log/merge/rebase/revert are unsupported, and a commit
ref falls back to the branch head. reset() maps to Neon's branch restore
(point-in-time), with ``to`` an LSN ("0/1A2B3C") or an ISO timestamp.
"""

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import re
import time
import threading
from dotenv import load_dotenv
import psycopg2
import requests

from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite, Ref
from neon_api import NeonAPI
import dblib.result_collector as rc

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))
API_KEY = os.environ.get("NEON_API_KEY_ORG", "")
neon = NeonAPI(api_key=API_KEY)
NEON_API_BASE_URL = "https://console.neon.tech/api/v2/"

_LSN_RE = re.compile(r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$")


class NeonToolSuite(DBToolSuite):
    BACKEND_NAME = "neon"
    SUPPORTS_COMMIT_REFS = False
    SUPPORTS_MULTI_REF_EXEC = False

    # ------------------------------------------------------------------
    # Project-level helpers (used by the runners' BackendManager)
    # ------------------------------------------------------------------

    @classmethod
    def create_neon_project(cls, project_name: str) -> dict:
        project_dict = {
            "project": {
                "pg_version": 17,
                "name": project_name,
                "region_id": "aws-us-east-1",
            }
        }
        return cls._request("POST", "projects", json=project_dict)

    @classmethod
    def delete_project(cls, project_id: str, timeout: int = 30) -> None:
        """Delete a Neon project, giving up after ``timeout`` seconds."""
        result = {"error": None, "success": False}

        def _delete():
            try:
                neon.project_delete(project_id)
                result["success"] = True
            except Exception as e:
                result["error"] = e

        thread = threading.Thread(target=_delete, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

        if thread.is_alive():
            print(f"Warning: Neon project deletion timed out after {timeout}s. "
                  f"Project {project_id} may still be deleted in the background.")
        elif result["error"]:
            print(f"Warning: Neon project deletion failed: {result['error']}")
        elif result["success"]:
            print(f"Neon project {project_id} deleted successfully.")

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
        uri = cls._get_neon_connection_uri(project_id, branch_id, database_name)
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
        r = requests.request(
            method, NEON_API_BASE_URL + endpoint, headers=headers, **kwargs
        )
        r.raise_for_status()
        if r.status_code == 204 or not r.content:
            return {}
        return r.json()

    @classmethod
    def _get_neon_connection_uri(
        cls,
        project_id: str,
        branch_id: str,
        db_name: str,
        max_retries: int = 10,
        retry_delay: float = 0.5,
    ) -> str:
        """Connection URI for a branch; retries 404 (branch not visible
        yet) and 429 with jittered backoff."""
        import random as _rng

        endpoint = (
            f"projects/{project_id}/connection_uri?branch_id={branch_id}"
            f"&database_name={db_name}&role_name=neondb_owner"
        )
        for attempt in range(max_retries):
            try:
                response = cls._request("GET", endpoint)
                return response["uri"]
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0
                retryable = status in (404, 429)
                if retryable and attempt < max_retries - 1:
                    delay = retry_delay * (2 ** min(attempt, 5))
                    delay *= 0.5 + _rng.random()
                    time.sleep(delay)
                    continue
                raise
        return None

    @classmethod
    def get_project_branches(cls, project_id: str) -> dict:
        return cls._request("GET", f"projects/{project_id}/branches")

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
        # branch name -> (branch id, connection uri or None)
        self._all_branches = {branch_name: (branch_id, None)}
        self._current_ref = Ref(branch_name)

    def _get_neon_branches(self) -> dict:
        response = self.__class__._request("GET", f"projects/{self.project_id}/branches")
        return {
            r["name"]: (r["id"], r.get("parent_id", None))
            for r in response["branches"]
        }

    def _branch_id(self, name: str) -> str:
        info = self._all_branches.get(name)
        if info and info[0]:
            return info[0]
        all_branches = self._get_neon_branches()
        if name not in all_branches:
            raise ValueError(f"Branch '{name}' does not exist.")
        bid = all_branches[name][0]
        self._all_branches[name] = (bid, None)
        return bid

    def _uri_for(self, name: str) -> str:
        bid = self._branch_id(name)
        uri = self._all_branches[name][1]
        if not uri:
            uri = self.__class__._get_neon_connection_uri(
                self.project_id, bid, self.db_name
            )
            self._all_branches[name] = (bid, uri)
        return uri

    def list_branches(self) -> list:
        return list(self._get_neon_branches().keys())

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def _connect_impl(self, ref: Ref) -> None:
        """Open a connection to the branch's endpoint (closing the old one).
        The first connection to a branch also fetches its URI from the API."""
        uri = self._uri_for(ref.branch)
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = psycopg2.connect(uri)
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.current_branch_id = self._all_branches[ref.branch][0]

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        parent_id = self._branch_id(from_ref.branch)
        branch_payload = {
            "endpoints": [{"type": "read_write"}],
            "branch": {"name": name, "parent_id": parent_id},
        }
        new_branch = neon.branch_create(self.project_id, **branch_payload)
        self._all_branches[name] = (new_branch.branch.id, None)

    def _delete_impl(self, ref: Ref) -> None:
        """DELETE /projects/{id}/branches/{branch_id}. Neon refuses to
        delete the default branch or a branch with children."""
        bid = self._branch_id(ref.branch)
        self.__class__._request("DELETE", f"projects/{self.project_id}/branches/{bid}")
        self._all_branches.pop(ref.branch, None)
        if self._current_ref and self._current_ref.branch == ref.branch:
            # The connection's branch is gone; reconnect lazily.
            self._current_ref = None

    def _reset_impl(self, ref: Ref, to: str) -> None:
        """Restore the branch to an earlier point of itself: ``to`` is an
        LSN or a timestamp (RFC 3339)."""
        bid = self._branch_id(ref.branch)
        source = {"source_branch_id": bid}
        if _LSN_RE.match(to):
            source["source_lsn"] = to
        else:
            source["source_timestamp"] = to
        self.__class__._request(
            "POST", f"projects/{self.project_id}/branches/{bid}/restore", json=source
        )
        if self._current_ref and self._current_ref.branch == ref.branch:
            self._current_ref = None  # endpoint restarts; reconnect lazily

    @staticmethod
    def _pg_database_size(conn) -> int:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_database_size(current_database())")
            return cur.fetchone()[0]

    _BRANCH_CONNECT_MAX_RETRIES = 3
    _BRANCH_CONNECT_RETRY_DELAY = 3.0

    def _storage_bytes(self) -> int:
        """Sum of pg_database_size() over every branch, opening a temporary
        connection per branch (the API's size metrics lag ~15 minutes).
        Branches that cannot be reached are skipped with a warning."""
        try:
            branches = self._get_neon_branches()
        except Exception as e:
            print(f"Warning: Could not list Neon branches: {e}")
            return 0

        total = 0
        for name, (branch_id, _) in branches.items():
            if branch_id == self.current_branch_id and self.conn:
                try:
                    total += self._pg_database_size(self.conn)
                except Exception as e:
                    print(f"Warning: Could not get storage for current branch '{name}': {e}")
                continue
            for attempt in range(self._BRANCH_CONNECT_MAX_RETRIES):
                try:
                    uri = self.__class__._get_neon_connection_uri(
                        self.project_id, branch_id, self.db_name
                    )
                    tmp_conn = psycopg2.connect(uri)
                    try:
                        total += self._pg_database_size(tmp_conn)
                    finally:
                        tmp_conn.close()
                    break
                except Exception as e:
                    if attempt < self._BRANCH_CONNECT_MAX_RETRIES - 1:
                        time.sleep(self._BRANCH_CONNECT_RETRY_DELAY)
                    else:
                        print(f"Warning: Could not get storage for branch '{name}': {e}")
        return total

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
            self._uri_for(self._pool_branch),
            min_size=size, max_size=size, kwargs={"autocommit": True}, open=False,
        )
        await self.async_pool.open(wait=True)

    async def _connect_impl_async(self, conn, ref: Ref):
        if ref.branch == self._pool_branch:
            return conn
        import psycopg

        return await psycopg.AsyncConnection.connect(
            self._uri_for(ref.branch), autocommit=True
        )

    # ------------------------------------------------------------------
    # Consumption metrics (macrobench storage accounting)
    # ------------------------------------------------------------------

    @classmethod
    def get_consumption_metrics(cls, project_id, org_id=None):
        """All consumption entries for a project in a 2-day window around
        now (hourly granularity), plus a summary of the most recent entry.
        Returns None on failure."""
        from datetime import datetime, timezone, timedelta

        org_id = org_id or os.environ.get("NEON_ORG_ID", "")
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
        print(f"  Consumption API: project={project_id}, org={org_id}, "
              f"window={start} to {end}", flush=True)
        try:
            resp = cls._request("GET", endpoint)
        except Exception as e:
            print(f"Warning: consumption metrics request failed: {e}")
            return None

        try:
            projects = resp.get("projects", [])
            if not projects:
                print("Warning: no projects in consumption response")
                return None
            periods = projects[0].get("periods", [])
            all_entries = []
            for period in periods:
                for entry in period.get("consumption", []):
                    metrics = entry.get("metrics", [])
                    if metrics:
                        entry_dict = {
                            "period_id": period.get("period_id", ""),
                            "timestamp": entry.get("timestamp", ""),
                        }
                        for m in metrics:
                            entry_dict[m["metric_name"]] = m["value"]
                        all_entries.append(entry_dict)
            if not all_entries:
                print("Warning: all consumption entries have empty metrics")
                return None
            most_recent = all_entries[-1]
            return {
                "all_metrics": all_entries,
                "count": len(all_entries),
                "summary": {
                    k: v for k, v in most_recent.items()
                    if k not in ("period_id", "timestamp")
                },
            }
        except (KeyError, IndexError) as e:
            print(f"Warning: could not parse consumption metrics: {e}")
            return None
