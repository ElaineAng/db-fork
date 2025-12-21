import psycopg2
from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
import dblib.result_collector as rc
import dblib.util as dbutil

DOLT_USER = "postgres"
DOLT_PASSWORD = "password"
DOLT_HOST = "localhost"
DOLT_PORT = 5432


class DoltToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a Dolt database on a shared connection.
    """

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return dbutil.format_db_uri(
            DOLT_USER, DOLT_PASSWORD, DOLT_HOST, DOLT_PORT, "postgres"
        )

    @classmethod
    def init_for_bench(cls, collector: rc.ResultCollector, db_name: str):
        uri = dbutil.format_db_uri(
            DOLT_USER, DOLT_PASSWORD, DOLT_HOST, DOLT_PORT, db_name
        )

        conn = psycopg2.connect(uri)
        return cls(connection=conn, collector=collector)

    def __init__(self, connection: _pgconn, collector: rc.ResultCollector):
        super().__init__(connection, collector=collector)

    def list_branches(self) -> list[str]:
        cmd = "SELECT name FROM dolt_branches;"
        return [branch[0] for branch in super().execute_sql(cmd)]

    def _prepare_commit(self, message: str = "") -> None:
        try:
            cmd = "call dolt_add('.');"
            super().execute_sql(cmd)
            cmd = f"call dolt_commit('-m', '{message}');"
            super().execute_sql(cmd)
        except Exception as e:
            # Ignore commit errors (e.g., no changes to commit).
            print(f"Commit failed: {e}")

    def _create_branch_impl(self, branch_name: str) -> None:
        """
        Creates a new branch in the Dolt database.
        """
        cmd = f"call dolt_checkout('-b', '{branch_name}');"
        super().execute_sql(cmd)
        super().commit_changes(
            timed=True, message=f"Create branch {branch_name}"
        )

    def _connect_branch_impl(self, branch_name: str) -> None:
        """
        Connects to an existing branch in the Dolt database to allow reads and
        writes on that branch.
        """
        cmd = f"call dolt_checkout('{branch_name}');"
        super().execute_sql(cmd)

    def _get_current_branch_impl(self) -> tuple[str, str]:
        # TODO: Consider cache the current branch name to avoid querying.
        cmd = "SELECT active_branch();"
        result = super().execute_sql(cmd)
        # Dolt's branch name is unique and can be used as an ID.
        return (result[0][0], result[0][0])
