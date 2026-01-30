from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from typing import Tuple
from dotenv import load_dotenv
import psycopg2
import requests

from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
import dblib.result_collector as rc

load_dotenv()
API_KEY = os.environ.get("XATA_API_KEY", "")

# Assuming everything runs in one organization. Pre-created.
XATA_ORGANIZATION_ID = os.environ.get("XATA_ORGANIZATION_ID", "")
XATA_API_BASE_URL = (
    f"https://api.xata.tech/organizations/{XATA_ORGANIZATION_ID}/"
)


class XataToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a Xata database on a shared connection.
    """

    @classmethod
    def add_db_name_to_connection_string(
        cls, connection_string: str, db_name: str
    ) -> str:
        """
        Processes the connection string to remove the database name.
        """
        conn_components = connection_string.split("/")
        conn_components[-1] = db_name
        return "/".join(conn_components) + "?sslmode=require"

    @classmethod
    def create_xata_project(
        cls, project_name: str
    ) -> Tuple[str, str, str, str]:
        project_dict = {"name": project_name}
        # TODO: Handle project creation failures.
        project_details = cls._request("POST", "projects", json=project_dict)

        # Create default branch within the project
        endpoint = f"projects/{project_details['id']}/branches"
        branch_payload = {
            "mode": "custom",
            "name": "main",
            "scaleToZero": {
                "enabled": True,
                "inactivityPeriodMinutes": 30,
            },
            "configuration": {
                "region": "us-east-1",
                "instanceType": "xata.medium",
                "image": "postgres:18.0",
                "replicas": 0,
            },
        }
        default_branch = cls._request("POST", endpoint, json=branch_payload)

        # Use the "postgres" database as the default.
        return (
            project_details["id"],
            default_branch["id"],
            default_branch["name"],
            cls.add_db_name_to_connection_string(
                default_branch["connectionString"], "postgres"
            ),
        )

    @classmethod
    def delete_project(cls, project_id: str) -> None:
        """
        Deletes a Xata project by its ID.
        """
        return cls._request("DELETE", f"projects/{project_id}")

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
        uri = cls._get_xata_connection_uri(project_id, branch_id, database_name)
        print(f"Initial connection to Xata with URI: {uri}")
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
        Helper method to make requests to the Xata API.
        """
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {API_KEY}"
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"

        r = requests.request(
            method, XATA_API_BASE_URL + endpoint, headers=headers, **kwargs
        )

        r.raise_for_status()

        return r.json()

    @classmethod
    def _get_xata_connection_uri(
        cls, project_id: str, branch_id: str, db_name: str
    ) -> str:
        """
        Retrieves the connection URI for a specific Xata databasse branch.
        """
        endpoint = f"projects/{project_id}/branches/{branch_id}"
        response = cls._request("GET", endpoint)
        print(response["status"]["statusType"])
        return cls.add_db_name_to_connection_string(
            response["connectionString"], db_name
        )

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

    def _get_xata_branches(self) -> list[dict]:
        """
        Lists all branches in the current Xata project.
        """
        endpoint = f"projects/{self.project_id}/branches"
        response = self.__class__._request("GET", endpoint)
        return {
            r["name"]: (r["id"], r.get("parent_id", None))
            for r in response["branches"]
        }

    def _delete_branch(self, branch_id: str) -> None:
        """
        Deletes the database from a specific branch in the Xata project.
        """
        endpoint = f"projects/{self.project_id}/branches/{branch_id}"
        self.__class__._request("DELETE", endpoint)

    def list_branches(self) -> list[str]:
        return list(self._get_xata_branches().keys())

    def delete_db(self, db_name: str) -> None:
        """
        Deletes the database from all branches in the Xata project.
        """
        for _, (branch_id, _) in self._get_xata_branches().items():
            print(f"Deleting database '{db_name}' on branch ID '{branch_id}'")
            self._delete_branch(branch_id)

    def _create_branch_impl(
        self, branch_name: str, parent_id: str = None
    ) -> None:
        """
        Creates a new branch in the Xata project.
        A branch can contain multiple databases, not the other way around.
        """
        endpoint = f"projects/{self.project_id}/branches"
        branch_payload = {
            "mode": "inherit",
            "name": branch_name,
            "parent_id": parent_id,
        }
        res = self.__class__._request("POST", endpoint, json=branch_payload)
        self._all_branches[branch_name] = (res["id"], res["connectionString"])

    def _connect_branch_impl(self, branch_name: str) -> None:
        """
        Connects to an existing branch and a specific database to allow reads
        and writes on that branch.
        """
        # Connecting to a specific branch involves establishing a new connection
        # to essentially a different database in Xata.
        #
        # Note that the first time we connect to a branch, we need to make an API
        # call to get the connection string, which may be add slight additional
        # overhead.
        branch_id = self._all_branches[branch_name][0]
        uri = self._all_branches[branch_name][1]
        if not branch_id:
            all_branches = self._get_xata_branches()
            if branch_name not in all_branches:
                raise ValueError(f"Branch '{branch_name}' does not exist.")
            branch_id = all_branches[branch_name][0]
        if not uri:
            uri = self.__class__._get_xata_connection_uri(
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

    # Xata API storage field name is not well-documented; try known candidates.
    _STORAGE_FIELD_CANDIDATES = ("storage_bytes", "data_storage", "synthetic_storage_size")

    def get_total_storage_bytes(self) -> int:
        """Get total storage used by the Xata project.

        Queries the Xata project API for storage information.

        Returns:
            Total storage in bytes, or 0 if unavailable.
        """
        try:
            endpoint = f"projects/{self.project_id}"
            response = self.__class__._request("GET", endpoint)

            for key in self._STORAGE_FIELD_CANDIDATES:
                value = response.get(key)
                if value is not None:
                    return int(value)

            print("Warning: No recognized storage field in Xata API response")
            return 0
        except Exception as e:
            print(f"Warning: Could not get Xata project storage: {e}")
            return 0
