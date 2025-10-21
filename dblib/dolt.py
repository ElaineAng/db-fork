from psycopg2.extensions import connection as _pgconn
from psycopg2.extensions import cursor as _pgcursor
from dblib.db_api import DBToolSuite


class DoltToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a Dolt database on a shared connection.
    """

    def __init__(self, connection: _pgconn, timed_cursor: _pgcursor = None):
        super().__init__(connection, timed_cursor=timed_cursor)

    def create_db_branch(self, branch_name: str, timed: bool = False) -> None:
        """
        Creates a new branch in the Dolt database.
        """
        cmd = f"call dolt_checkout('-b', '{branch_name}');"
        super().run_sql_query(cmd, timed=timed)
        super().commit_changes()

    def connect_db_branch(self, branch_name: str, timed: bool = False) -> None:
        """
        Connects to an existing branch in the Dolt database to allow reads and
        writes on that branch.
        """
        cmd = f"call dolt_checkout('{branch_name}');"
        super().run_sql_query(cmd, timed=timed)
        # print(
        #     f"... Connected to branch: {self.get_current_db_branch(timed=False)}"
        # )

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
