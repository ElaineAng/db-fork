import os
import json
from time import time
import psycopg2
import requests

from psycopg2.extensions import connection as _pgconn
from psycopg2.extensions import cursor as _pgcursor
from dblib.db_api import DBToolSuite
from neon_api import NeonAPI
import dblib.timer as dbtimer

API_KEY = os.environ.get("NEON_API_KEY_ORG", "")
neon = NeonAPI(api_key=API_KEY)
NEON_API_BASE_URL = "https://console.neon.tech/api/v2/"


class NeonToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a Neon database on a shared connection.
    """

    @classmethod
    def create_neon_project(cls, project_name: str) -> str:
        project_dict = {"pg_version": "17", "name": project_name}
        # TODO: Handle project creation failures.
        return neon.project_create(json.dumps(project_dict))

    @classmethod
    def init_for_bench(
        cls,
        timer: dbtimer.Timer,
        project_name: str,
        branch_id: str,
        branch_name: str,
        database_name: str,
    ):
        uri = cls._get_neon_connection_uri(
            project_name, branch_id, database_name
        )
        conn = psycopg2.connect(
            uri,
            connection_factory=lambda *args, **kwargs: timer.TimerConnection(
                *args, **kwargs, timer=timer
            ),
        )
        return cls(
            connection=conn,
            timed_cursor=lambda *args, **kwargs: dbtimer.TimerCursor(
                *args, **kwargs, timer=timer
            ),
            timer=timer,
            project=project_name,
            branch_name=branch_name,
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
        cls, project_name: str, branch_id: str, db_name: str
    ) -> str:
        """
        Retrieves the connection URI for a specific Neon database branch.
        """
        endpoint = (
            f"projects/{project_name}/connection_uri?branch_id={branch_id}"
            f"&database_name={db_name}&role_name=neondb_owner"
        )
        response = cls._request("GET", endpoint)
        return response["connection_uri"]

    def __init__(
        self,
        connection: _pgconn,
        timed_cursor: _pgcursor = None,
        timer: dbtimer.Timer = None,
        project: str = "",
        branch_name: str = "",
    ):
        super().__init__(connection, timed_cursor=timed_cursor)
        self.project = project
        self.timer = timer
        self.current_branch_name = branch_name or "production"

    def _get_neon_branches(self) -> list[dict]:
        """
        Lists all branches in the current Neon project.
        """
        endpoint = f"projects/{self.project}/branches"
        response = self.__class__._request("GET", endpoint)
        return {
            r["name"]: (r["id"], r["parent_id"]) for r in response["branches"]
        }

    def _delete_db_on_branch(self, branch_id: str, db_name: str) -> None:
        """
        Deletes the database from a specific branch in the Neon project.
        """
        endpoint = (
            f"projects/{self.project}/branches/{branch_id}/databases/{db_name}"
        )
        self.__class__._request("DELETE", endpoint)

    def create_db_branch(
        self, branch_name: str, parent_id: str, timed: bool = False
    ) -> None:
        """
        Creates a new branch in the Neon project.
        The branch can contain multiple databases.
        """
        branch_payload = {
            "branch": {"name": branch_name, "parent_id": parent_id}
        }
        # Branch creation isn't a database operation in Neon, so we have to
        # explicitly time it here.
        if self.timer and timed:
            start_time = time()
        branch = neon.branch_create(self.project, json.dumps(branch_payload))
        if self.timer and timed:
            end_time = time()
            # Report the collected time to the cursor elapsed, compatible with
            # those backends whose branching operations are done via SQL
            # queries.
            self.timer.collect_cursor_elapsed(
                end_time - start_time, tag="neon_branching"
            )
        print(f"Created branch: {branch}")

    def connect_db_branch(self, branch_name: str, timed: bool = False) -> None:
        """
        Connects to an existing branch and a specific database to allow reads
        and writes on that branch.
        """
        # Connecting to a specific branch involves establishing a new connection
        # to essentially a different database in Neon.
        if self.timer and timed:
            start_time = time()
        all_branches = self._get_neon_branches()
        if branch_name not in all_branches:
            raise ValueError(f"Branch '{branch_name}' does not exist.")
        uri = self.__class__._get_neon_connection_uri(
            all_branches[branch_name][0],
            self.conn.get_dsn_parameters()["dbname"],
        )
        self.conn.close()
        self.conn = psycopg2.connect(uri)
        self.current_branch_name = branch_name

        if self.timer and timed:
            end_time = time()
            self.timer.collect_cursor_elapsed(
                end_time - start_time, tag="neon_branching"
            )

    def list_db_branches(self, timed: bool = False) -> list[str]:
        return list(self._get_neon_branches().keys())

    def get_current_db_branch(self, timed: bool = False) -> str:
        return self.current_branch_name

    def delete_db(self, db_name: str) -> None:
        """
        Deletes the database from all branches in the Neon project.
        """
        for _, (branch_id, _) in self._get_neon_branches().items():
            print(f"Deleting database '{db_name}' on branch ID '{branch_id}'")
            self._delete_db_on_branch(branch_id, db_name)

    def commit_changes(self, message: str = "", timed: bool = False) -> None:
        pass
