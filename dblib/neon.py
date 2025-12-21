from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from typing import Tuple
from dotenv import load_dotenv
import psycopg2
import requests

from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
from neon_api import NeonAPI
import dblib.result_collector as rc

load_dotenv()
API_KEY = os.environ.get("NEON_API_KEY_ORG", "")
neon = NeonAPI(api_key=API_KEY)
NEON_API_BASE_URL = "https://console.neon.tech/api/v2/"


class NeonToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a Neon database on a shared connection.
    """

    @classmethod
    def create_neon_project(cls, project_name: str) -> str:
        project_dict = {"project": {"pg_version": 17, "name": project_name}}
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
        print(f"Initial connection to Neon with URI: {uri}")
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
        cls, project_id: str, branch_id: str, db_name: str
    ) -> str:
        """
        Retrieves the connection URI for a specific Neon database branch.
        """
        endpoint = (
            f"projects/{project_id}/connection_uri?branch_id={branch_id}"
            f"&database_name={db_name}&role_name=neondb_owner"
        )
        response = cls._request("GET", endpoint)
        return response["uri"]

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
        self.current_branch_name = branch_name or "production"
        self.current_branch_id = branch_id
        self.autocommit = autocommit
        self._all_branches = {branch_name: (branch_id, None)}

    def _get_neon_branches(self) -> list[dict]:
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
        branch_id = self._all_branches[branch_name][0]
        uri = self._all_branches[branch_name][1]
        if not branch_id:
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
