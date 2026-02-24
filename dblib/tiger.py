import os
import time
import base64
from typing import Tuple, Dict

import requests
import psycopg2
from psycopg2.extensions import connection as _pgconn
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dblib.db_api import DBToolSuite
import dblib.result_collector as rc


TIGER_API_BASE = "https://console.cloud.timescale.com/public/api/v1"


class TigerToolSuite(DBToolSuite):
    """
    Tiger Cloud implementation of DBToolSuite.
    Branching is implemented via service forking.
    """

    ###########################################################################
    # Initialization
    ###########################################################################

    def __init__(
            self,
            connection: _pgconn,
            result_collector: rc.ResultCollector,
            project_id: str,
            service_id: str,
            service_name: str,
            password: str,
            region_code: str,
            autocommit: bool,
            ):
        super().__init__(connection, result_collector)

        self.project_id = project_id
        self.current_service_id = service_id
        self.current_service_name = service_name
        self.password = password
        self.region_code = region_code
        self.autocommit = autocommit

        self.access_key = os.environ.get("TIGER_ACCESS_KEY")
        self.secret_key = os.environ.get("TIGER_SECRET_KEY")

        if not self.access_key or not self.secret_key:
            raise ValueError("Tiger credentials not set in environment.")

        # Cache: branch_name -> service_id
        self._services: Dict[str, str] = {
                service_name: service_id
                }

    @classmethod
    def init_for_bench(
            cls,
            result_collector: rc.ResultCollector,
            project_id: str,
            service_id: str,
            service_name: str,
            password: str,
            region_code: str,
            autocommit: bool,
            ):
        """
        Mirrors NeonToolSuite.init_for_bench. Fetches the service info,
        builds the connection URI, and returns a ready TigerToolSuite instance.
        """
        access_key = os.environ.get("TIGER_ACCESS_KEY")
        secret_key = os.environ.get("TIGER_SECRET_KEY")

        if not access_key or not secret_key:
            raise ValueError("Tiger credentials not set in environment.")

        token = base64.b64encode(
                f"{access_key}:{secret_key}".encode()
                ).decode()
        headers = {
                "Authorization": f"Basic {token}",
                "Accept": "application/json",
                }

        r = requests.get(
                f"{TIGER_API_BASE}/projects/{project_id}/services/{service_id}",
                headers=headers,
                )
        if not r.ok:
            raise Exception(f"Tiger API error {r.status_code}: {r.text}")

        service_info = r.json()
        host = service_info["endpoint"]["host"]
        port = service_info["endpoint"]["port"]
        uri = f"postgresql://tsdbadmin:{password}@{host}:{port}/tsdb"

        conn = psycopg2.connect(uri)
        if autocommit:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        return cls(
                connection=conn,
                result_collector=result_collector,
                project_id=project_id,
                service_id=service_id,
                service_name=service_name,
                password=password,
                region_code=region_code,
                autocommit=autocommit,
                )

    ###########################################################################
    # Internal API Helpers
    ###########################################################################

    def _auth_header(self):
        token = base64.b64encode(
                f"{self.access_key}:{self.secret_key}".encode()
                ).decode()

        return {
                "Authorization": f"Basic {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                }

    def _request(self, method: str, endpoint: str, **kwargs):
        url = f"{TIGER_API_BASE}{endpoint}"
        headers = self._auth_header()

        r = requests.request(method, url, headers=headers, **kwargs)

        if not r.ok:
            raise Exception(f"Tiger API error {r.status_code}: {r.text}")

        if r.status_code == 204:
            return {}

        return r.json()

    def _wait_until_ready(self, service_id: str, timeout: int = 300):
        start = time.time()

        while True:
            service = self._request(
                    "GET",
                    f"/projects/{self.project_id}/services/{service_id}",
                    )

            status = service["status"]

            if status == "READY":
                return service

            if time.time() - start > timeout:
                raise TimeoutError(
                        f"Service {service_id} did not become READY"
                        )

            time.sleep(3)

    def _build_pg_uri(self, service_info: dict) -> str:
        host = service_info["endpoint"]["host"]
        port = service_info["endpoint"]["port"]

        return (
                f"postgresql://tsdbadmin:{self.password}"
                f"@{host}:{port}/tsdb"
                )

    ###########################################################################
    # Class Methods (Used by Runner)
    ###########################################################################

    @classmethod
    def wait_for_service(cls, project_id: str, service_id: str, timeout: int = 300) -> dict:
        """Polls until the service is READY and returns the final service dict."""
        access_key = os.environ.get("TIGER_ACCESS_KEY")
        secret_key = os.environ.get("TIGER_SECRET_KEY")
        token = base64.b64encode(f"{access_key}:{secret_key}".encode()).decode()
        headers = {"Authorization": f"Basic {token}", "Accept": "application/json"}
        start = time.time()
        while True:
            r = requests.get(
                f"{TIGER_API_BASE}/projects/{project_id}/services/{service_id}",
                headers=headers,
            )
            r.raise_for_status()
            service = r.json()
            if service["status"] == "READY":
                return service
            if time.time() - start > timeout:
                raise TimeoutError(f"Service {service_id} did not become READY within {timeout}s")
            time.sleep(5)

    @classmethod
    def create_tiger_service(
            cls,
            name: str,
            project_id: str = "tdzyl504xn",
            region_code: str = "us-east-1",  # FIX: removed erroneous leading space
            cpu_millis: int = 1000,
            memory_gbs: int = 4,
            ):
        access_key = os.environ.get("TIGER_ACCESS_KEY")
        secret_key = os.environ.get("TIGER_SECRET_KEY")

        token = base64.b64encode(
                f"{access_key}:{secret_key}".encode()
                ).decode()

        headers = {
                "Authorization": f"Basic {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                }

        payload = {
                "name": name,
                "addons": [],
                "region_code": region_code,
                "cpu_millis": cpu_millis,
                "memory_gbs": memory_gbs,
                }

        r = requests.post(
                f"{TIGER_API_BASE}/projects/{project_id}/services",
                headers=headers,
                json=payload,
                )

        if not r.ok:
            raise Exception(f"Tiger create failed: {r.text}")

        return r.json()

    @classmethod
    def get_service(cls, project_id: str, service_id: str):
        access_key = os.environ.get("TIGER_ACCESS_KEY")
        secret_key = os.environ.get("TIGER_SECRET_KEY")

        token = base64.b64encode(
                f"{access_key}:{secret_key}".encode()
                ).decode()

        headers = {
                "Authorization": f"Basic {token}",
                "Accept": "application/json",
                }

        r = requests.get(
                f"{TIGER_API_BASE}/projects/{project_id}/services/{service_id}",
                headers=headers,
                )

        if not r.ok:
            raise Exception(r.text)

        return r.json()

    ###########################################################################
    # Branching = Service Forking
    ###########################################################################

    def _create_branch_impl(
            self, branch_name: str, parent_id: str = None
            ) -> None:

        parent_service_id = parent_id or self.current_service_id

        payload = {
                "name": branch_name,
                "region_code": self.region_code,
                "cpu_millis": 1000,
                "memory_gbs": 4,
                }

        response = self._request(
                "POST",
                f"/projects/{self.project_id}/services/"
                f"{parent_service_id}/forkService",
                json=payload,
                )

        new_service_id = response["service_id"]

        # Wait for fork to be ready before returning so that callers can
        # immediately connect. Note: this wait time is included in the
        # BRANCH_CREATE timing recorded by the base class.
        self._wait_until_ready(new_service_id)

        self._services[branch_name] = new_service_id

    def _connect_branch_impl(self, branch_name: str) -> None:

        if branch_name not in self._services:
            raise ValueError(f"Unknown Tiger branch: {branch_name}")

        service_id = self._services[branch_name]

        service_info = self._request(
                "GET",
                f"/projects/{self.project_id}/services/{service_id}",
                )

        uri = self._build_pg_uri(service_info)

        self.conn.close()
        self.conn = psycopg2.connect(uri)

        if self.autocommit:
            self.conn.set_isolation_level(
                    ISOLATION_LEVEL_AUTOCOMMIT
                    )

        self.current_service_id = service_id
        self.current_service_name = branch_name

    def _get_current_branch_impl(self) -> Tuple[str, str]:
        return (self.current_service_name, self.current_service_id)

    def _delete_branch_impl(
            self, branch_name: str, branch_id: str
            ) -> None:

        service_id = branch_id or self._services.get(branch_name)

        if not service_id:
            raise ValueError(f"Unknown service: {branch_name}")

        self._request(
                "DELETE",
                f"/projects/{self.project_id}/services/{service_id}",
                )

        self._services.pop(branch_name, None)

    ###########################################################################
    # Storage Measurement
    ###########################################################################

    def get_total_storage_bytes(self) -> int:
        """
        Sums pg_database_size() across all known forked services, mirroring
        the multi-branch approach in NeonToolSuite.
        """
        total = 0
        db_name = "tsdb"

        for service_name, service_id in self._services.items():
            if service_id == self.current_service_id:
                # Reuse the live connection for the current service.
                try:
                    with self.conn.cursor() as cur:
                        cur.execute(
                                "SELECT pg_database_size(current_database())"
                                )
                        total += cur.fetchone()[0]
                except Exception as e:
                    print(
                            f"Warning: Could not get storage for current "
                            f"service '{service_name}': {e}"
                            )
                continue

            # Open a temporary connection for each other known service.
            try:
                service_info = self._request(
                        "GET",
                        f"/projects/{self.project_id}/services/{service_id}",
                        )
                uri = self._build_pg_uri(service_info)
                tmp_conn = psycopg2.connect(uri)
                try:
                    with tmp_conn.cursor() as cur:
                        cur.execute(
                                "SELECT pg_database_size(current_database())"
                                )
                        total += cur.fetchone()[0]
                finally:
                    tmp_conn.close()
            except Exception as e:
                print(
                        f"Warning: Could not get storage for service "
                        f"'{service_name}': {e}"
                        )

        return total
