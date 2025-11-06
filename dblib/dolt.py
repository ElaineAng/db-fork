import psycopg2
from psycopg2.extensions import connection as _pgconn
from psycopg2.extensions import cursor as _pgcursor
from dblib.db_api import DBToolSuite
import dblib.timer as dbtimer
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
    def init_for_bench(cls, timer, db_name: str):
        uri = dbutil.format_db_uri(
            DOLT_USER, DOLT_PASSWORD, DOLT_HOST, DOLT_PORT, db_name
        )

        # Timed connection and tools to measure timing.
        conn = psycopg2.connect(
            uri,
            connection_factory=lambda *args, **kwargs: dbtimer.TimerConnection(
                *args, **kwargs, timer=timer
            ),
        )
        return cls(
            connection=conn,
            timed_cursor=lambda *args, **kwargs: dbtimer.TimerCursor(
                *args, **kwargs, timer=timer
            ),
        )

    def __init__(self, connection: _pgconn, timed_cursor: _pgcursor = None):
        super().__init__(connection, timed_cursor=timed_cursor)

    def create_db_branch(
        self, branch_name: str, timed: bool = False, parent_id: str = None
    ) -> bool:
        """
        Creates a new branch in the Dolt database.
        """
        cmd = f"call dolt_checkout('-b', '{branch_name}');"
        super().run_sql_query(cmd, timed=timed)
        super().commit_changes()
        return True

    def connect_db_branch(self, branch_name: str, timed: bool = False) -> None:
        """
        Connects to an existing branch in the Dolt database to allow reads and
        writes on that branch.
        """
        cmd = f"call dolt_checkout('{branch_name}');"
        super().run_sql_query(cmd, timed=timed)

    def list_db_branches(self, timed: bool = False) -> list[str]:
        cmd = "SELECT name FROM dolt_branches;"
        return [branch[0] for branch in super().run_sql_query(cmd, timed=timed)]

    def get_current_db_branch(self, timed: bool = False) -> str:
        cmd = "SELECT active_branch();"
        result = super().run_sql_query(cmd, timed=timed)
        return result[0][0]

    def commit_changes(self, message: str = "", timed: bool = False) -> None:
        try:
            cmd = "call dolt_add('.');"
            super().run_sql_query(cmd, timed=timed)
            cmd = f"call dolt_commit('-m', '{message}');"
            super().run_sql_query(cmd, timed=timed)
            super().commit_changes()
        except Exception as e:
            # Ignore commit errors (e.g., no changes to commit).
            print(f"Commit failed: {e}")
