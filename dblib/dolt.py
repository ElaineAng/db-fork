import os

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
import dblib.result_collector as rc
import dblib.util as dbutil

DOLT_USER = os.environ.get("DOLT_USER", "postgres")
DOLT_PASSWORD = os.environ.get("DOLT_PASSWORD", "password")
DOLT_HOST = os.environ.get("DOLT_HOST", "localhost")
DOLT_PORT = int(os.environ.get("DOLT_PORT", "5432"))
DOLT_DATA_DIR = os.environ.get("DOLT_DATA_DIR", "/tmp/doltgres_data/databases")


def commit_dolt_schema(db_uri: str, message: str = "Load SQL schema") -> None:
    """Commit schema changes over db_uri.

    Args:
        db_uri: Connection URI for the Dolt database.
        message: Commit message.
    """
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(db_uri)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("SELECT dolt_add('-A');")
        cur.execute(f"SELECT dolt_commit('-m', '{message}');")
        print(f"Dolt schema committed: {message}")
    except Exception as e:
        print(f"Warning: Dolt schema commit failed (may be okay): {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


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
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return dbutil.format_db_uri(
            DOLT_USER, DOLT_PASSWORD, DOLT_HOST, DOLT_PORT, db_name
        )

    @classmethod
    def init_for_bench(
        cls,
        collector: rc.ResultCollector,
        db_name: str,
        autocommit: bool,
        default_branch_name: str,
    ):
        uri = cls.get_initial_connection_uri(db_name)

        conn = psycopg2.connect(uri)
        if autocommit:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return cls(
            connection=conn,
            collector=collector,
            autocommit=autocommit,
            default_branch_name=default_branch_name,
            db_name=db_name,
        )

    def __init__(
        self,
        connection: _pgconn,
        collector: rc.ResultCollector,
        autocommit: bool,
        default_branch_name: str,
        db_name: str = None,
    ):
        super().__init__(connection, result_collector=collector)
        self._connect_branch_impl(default_branch_name)
        self.autocommit = autocommit
        self.db_name = db_name

    def list_branches(self) -> list[str]:
        cmd = "SELECT name FROM dolt_branches;"
        return [branch[0] for branch in super().execute_sql(cmd)]

    def _prepare_commit(self, message: str = "") -> None:
        try:
            cmd = "SELECT dolt_add('-A');"
            super().execute_sql(cmd)
            cmd = f"SELECT dolt_commit('-m', '{message}');"
            super().execute_sql(cmd)
        except Exception as e:
            # Ignore commit errors (e.g., no changes to commit).
            print(f"Commit failed: {e}")

    def _create_branch_impl(self, branch_name: str, parent_id: str = None) -> None:
        """
        Creates a new branch in the Dolt database.
        """
        # Only checkout to parent if specified, otherwise create from current branch
        if parent_id:
            self._connect_branch_impl(parent_id)
        cmd = f"SELECT dolt_checkout('-b', '{branch_name}');"
        super().execute_sql(cmd)

    def _connect_branch_impl(self, branch_name: str) -> None:
        """
        Connects to an existing branch in the Dolt database to allow reads and
        writes on that branch.
        """
        cmd = f"SELECT dolt_checkout('{branch_name}');"
        super().execute_sql(cmd)

    def _get_current_branch_impl(self) -> tuple[str, str]:
        # TODO: Consider cache the current branch name to avoid querying.
        cmd = "SELECT active_branch();"
        result = super().execute_sql(cmd)
        # Dolt's branch name is unique and can be used as an ID.
        return (result[0][0], result[0][0])

    def get_total_storage_bytes(self) -> int:
        """Get total storage by measuring the Dolt data directory on disk.

        Dolt uses content-addressable Prolly trees with structural sharing
        across branches. DoltgreSQL doesn't support pg_database_size(), so we
        measure physical disk usage directly from the data directory.
        Reference: https://docs.dolthub.com/architecture/storage-engine/prolly-tree
        """
        if not self.db_name:
            return 0
        return dbutil.get_directory_size_bytes(
            os.path.join(DOLT_DATA_DIR, self.db_name)
        )
