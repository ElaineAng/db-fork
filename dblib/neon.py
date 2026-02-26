from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import time
from typing import Tuple
from dotenv import load_dotenv
import psycopg2
import requests

from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
from neon_api import NeonAPI
import dblib.result_collector as rc

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))
API_KEY = os.environ.get("NEON_API_KEY_ORG", "")
neon = NeonAPI(api_key=API_KEY)
NEON_API_BASE_URL = "https://console.neon.tech/api/v2/"


class NeonToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a Neon database on a shared connection.
    """

    @classmethod
    def create_neon_project(cls, project_name: str) -> str:
        project_dict = {
            "project": {
                "pg_version": 17,
                "name": project_name,
                "region_id": "aws-us-east-1",
            }
        }
        # TODO: Handle project creation failures.
        return cls._request("POST", "projects", json=project_dict)

    @classmethod
    def delete_project(cls, project_id: str) -> None:
        """
        Deletes a Neon project by its ID.
        """
        return neon.project_delete(project_id)

    @classmethod
    def init_for_bench(
        cls,
        result_collector: rc.ResultCollector,
        project_id: str,
        branch_id: str,
        branch_name: str,
        database_name: str,
        autocommit: bool,
    ):
        uri = cls._get_neon_connection_uri(project_id, branch_id, database_name)
        conn = psycopg2.connect(uri)
        if autocommit:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return cls(
            connection=conn,
            result_collector=result_collector,
            project_id=project_id,
            branch_name=branch_name,
            branch_id=branch_id,
            autocommit=autocommit,
        )

    @classmethod
    def _request(cls, method: str, endpoint: str, **kwargs):
        """
        Helper method to make requests to the Neon API.
        """
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {API_KEY}"
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"

        r = requests.request(
            method, NEON_API_BASE_URL + endpoint, headers=headers, **kwargs
        )

        r.raise_for_status()

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
        """
        Retrieves the connection URI for a specific Neon database branch.

        Retries on HTTP 404 (newly created branches not yet visible)
        and HTTP 429 (rate limit) with jittered backoff.
        """
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
                    delay *= 0.5 + _rng.random()  # jitter
                    time.sleep(delay)
                    continue
                raise
        return None

    @classmethod
    def get_project_branches(cls, project_id: str) -> dict:
        """
        Retrieves details of a Neon project by its ID.
        """
        endpoint = f"projects/{project_id}/branches"
        return cls._request("GET", endpoint)

    def __init__(
        self,
        connection: _pgconn,
        result_collector: rc.ResultCollector,
        project_id: str,
        branch_name: str,
        branch_id: str,
        autocommit: bool,
    ):
        super().__init__(connection, result_collector)
        self.project_id = project_id
        self.result_collector = result_collector
        self.current_branch_name = branch_name
        self.current_branch_id = branch_id
        self.autocommit = autocommit
        self._all_branches = {branch_name: (branch_id, None)}

    def _get_neon_branches(self) -> dict:
        """
        Lists all branches in the current Neon project.
        """
        endpoint = f"projects/{self.project_id}/branches"
        response = self.__class__._request("GET", endpoint)
        return {
            r["name"]: (r["id"], r.get("parent_id", None))
            for r in response["branches"]
        }

    def _delete_db_on_branch(self, branch_id: str, db_name: str) -> None:
        """
        Deletes the database from a specific branch in the Neon project.
        """
        endpoint = f"projects/{self.project_id}/branches/{branch_id}/databases/{db_name}"
        self.__class__._request("DELETE", endpoint)

    def list_branches(self) -> list[str]:
        return list(self._get_neon_branches().keys())

    def delete_db(self, db_name: str) -> None:
        """
        Deletes the database from all branches in the Neon project.
        """
        for _, (branch_id, _) in self._get_neon_branches().items():
            print(f"Deleting database '{db_name}' on branch ID '{branch_id}'")
            self._delete_db_on_branch(branch_id, db_name)

    def _create_branch_impl(
        self, branch_name: str, parent_id: str = None
    ) -> None:
        """
        Creates a new branch in the Neon project.
        A branch can contain multiple databases, not the other way around.
        """
        branch_payload = {
            "endpoints": [{"type": "read_write"}],
            "branch": {"name": branch_name, "parent_id": parent_id},
        }

        # This returns a BranchOperations object with .branch attribute
        new_branch = neon.branch_create(self.project_id, **branch_payload)
        self._all_branches[branch_name] = (new_branch.branch.id, "")

    def _connect_branch_impl(self, branch_name: str) -> None:
        """
        Connects to an existing branch and a specific database to allow reads
        and writes on that branch.
        """
        # Connecting to a specific branch involves establishing a new connection
        # to essentially a different database in Neon.
        #
        # Note that the first time we connect to a branch, we need to make an API
        # call to get the connection string, which may be add slight additional
        # overhead.
        branch_info = self._all_branches.get(branch_name)
        branch_id = branch_info[0] if branch_info else None
        uri = branch_info[1] if branch_info else None
        if not branch_id:
            print(
                f"WARNING: Branch '{branch_name}' not cached. "
                "Fetching from API."
            )
            all_branches = self._get_neon_branches()
            if branch_name not in all_branches:
                raise ValueError(f"Branch '{branch_name}' does not exist.")
            branch_id = all_branches[branch_name][0]
        if not uri:
            uri = self.__class__._get_neon_connection_uri(
                self.project_id,
                branch_id,
                self.conn.get_dsn_parameters()["dbname"],
            )
            # Cache the URI - replace tuple since tuples are immutable
            self._all_branches[branch_name] = (branch_id, uri)

        self.conn.close()
        self.conn = psycopg2.connect(uri)
        if self.autocommit:
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        self.current_branch_name = branch_name
        self.current_branch_id = branch_id

    def _get_current_branch_impl(self) -> Tuple[str, str]:
        return (self.current_branch_name, self.current_branch_id)

    @staticmethod
    def _pg_database_size(conn) -> int:
        """Return pg_database_size(current_database()) in bytes via SQL."""
        with conn.cursor() as cur:
            cur.execute("SELECT pg_database_size(current_database())")
            return cur.fetchone()[0]

    _BRANCH_CONNECT_MAX_RETRIES = 3
    _BRANCH_CONNECT_RETRY_DELAY = 3.0

    def get_total_storage_bytes(self) -> int:
        """Get total storage across all branches via pg_database_size().

        Opens a temporary connection to each branch (except the current one,
        which reuses self.conn) and sums pg_database_size().  This is an
        instant, real-time metric — unlike synthetic_storage_size or the
        branch-level logical_size from the API, which lag ~15 minutes.

        Per-branch failures (e.g. cold-compute timeouts) are retried and,
        if still unsuccessful, skipped so that measurements from other
        branches are not discarded.

        Returns:
            Total storage in bytes across all branches, or 0 if unavailable.
        """
        try:
            db_name = self.conn.get_dsn_parameters()["dbname"]
            branches = self._get_neon_branches()
        except Exception as e:
            print(f"Warning: Could not list Neon branches: {e}")
            return 0

        total = 0
        for name, (branch_id, _) in branches.items():
            if branch_id == self.current_branch_id:
                try:
                    total += self._pg_database_size(self.conn)
                except Exception as e:
                    print(
                        f"Warning: Could not get storage for current "
                        f"branch '{name}': {e}"
                    )
                continue

            for attempt in range(self._BRANCH_CONNECT_MAX_RETRIES):
                try:
                    uri = self.__class__._get_neon_connection_uri(
                        self.project_id,
                        branch_id,
                        db_name,
                    )
                    tmp_conn = psycopg2.connect(uri)
                    try:
                        total += self._pg_database_size(tmp_conn)
                    finally:
                        tmp_conn.close()
                    break  # success — move to next branch
                except Exception as e:
                    if attempt < self._BRANCH_CONNECT_MAX_RETRIES - 1:
                        print(
                            f"Warning: branch '{name}' attempt "
                            f"{attempt + 1}/{self._BRANCH_CONNECT_MAX_RETRIES}"
                            f" failed ({e}), retrying in "
                            f"{self._BRANCH_CONNECT_RETRY_DELAY}s..."
                        )
                        time.sleep(self._BRANCH_CONNECT_RETRY_DELAY)
                    else:
                        print(
                            f"Warning: Could not get storage for branch "
                            f"'{name}' after "
                            f"{self._BRANCH_CONNECT_MAX_RETRIES} attempts: "
                            f"{e}"
                        )

        return total

    def _delete_branch_impl(self, branch_name: str, branch_id: str) -> None:
        """Delete a branch via the Neon REST API.

        Uses DELETE /projects/{project_id}/branches/{branch_id}.

        Restrictions enforced by Neon:
          - Cannot delete the root/default branch.
          - Cannot delete a branch that has child branches.
        """
        bid = branch_id
        if not bid:
            info = self._all_branches.get(branch_name)
            if info:
                bid = info[0]
        if not bid:
            # Fall back to API lookup
            all_branches = self._get_neon_branches()
            if branch_name in all_branches:
                bid = all_branches[branch_name][0]
        if not bid:
            raise ValueError(
                f"Cannot delete branch '{branch_name}': unknown branch ID"
            )

        endpoint = f"projects/{self.project_id}/branches/{bid}"
        self.__class__._request("DELETE", endpoint)

        # Remove from local cache.
        self._all_branches.pop(branch_name, None)

    @classmethod
    def get_consumption_metrics(cls, project_id, org_id=None):
        """Fetch storage consumption metrics for a project from the Neon API.

        Uses the ``consumption_history/v2/projects`` endpoint to retrieve
        ``root_branch_bytes_month`` and ``child_branch_bytes_month``.

        Args:
            project_id: Neon project ID.
            org_id: Neon organization ID.  Falls back to the
                     ``NEON_ORG_ID`` environment variable.

        Returns:
            Dict with ``root_branch_bytes_month`` and
            ``child_branch_bytes_month``, or ``None`` on failure.
        """
        from datetime import datetime, timezone, timedelta

        org_id = org_id or os.environ.get("NEON_ORG_ID", "")
        if not org_id:
            print("Warning: NEON_ORG_ID not set, skipping consumption metrics")
            return None

        now = datetime.now(timezone.utc)
        # Use a 24-hour window — hourly data may lag behind real time.
        start = (now - timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        endpoint = (
            f"consumption_history/v2/projects"
            f"?org_id={org_id}"
            f"&project_ids={project_id}"
            f"&from={start}&to={end}"
            f"&granularity=hourly"
            f"&metrics=root_branch_bytes_month,child_branch_bytes_month"
        )
        try:
            resp = cls._request("GET", endpoint)
        except Exception as e:
            print(f"Warning: consumption metrics request failed: {e}")
            return None

        # Walk the response to find the most recent non-empty data point.
        try:
            projects = resp.get("projects", [])
            if not projects:
                print("Warning: no projects in consumption response")
                return None
            for period in reversed(projects[0].get("periods", [])):
                for entry in reversed(period.get("consumption", [])):
                    metrics = entry.get("metrics", [])
                    if metrics:
                        result = {}
                        for m in metrics:
                            result[m["metric_name"]] = m["value"]
                        return result
            print("Warning: no consumption data points found")
            return None
        except (KeyError, IndexError) as e:
            print(f"Warning: could not parse consumption metrics: {e}")
            return None
