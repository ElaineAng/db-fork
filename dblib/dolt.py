from psycopg2.extensions import connection as pgconn
from dblib.db_api import DBToolSuite


class DoltToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a Dolt database on a shared connection.
    """

    def __init__(self, connection: pgconn):
        super().__init__(connection)

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
        super().run_sql_query(cmd)
        super().commit_changes()

    def list_db_branches(self, timed: bool = False) -> list[str]:
        cmd = "SELECT name FROM dolt_branches;"
        return [branch[0] for branch in super().run_sql_query(cmd, timed=timed)]

    def commit_changes(self, message: str = "", timed: bool = False) -> None:
        cmd = "call dolt_add('.');"
        super().run_sql_query(cmd, timed=timed)
        cmd = f"call dolt_commit('-m', '{message}');"
        super().run_sql_query(cmd, timed=timed)
        super().commit_changes()
