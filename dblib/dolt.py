from psycopg2.extensions import connection as pgconn
from dblib.db_api import DBToolSuite


class DoltToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a Dolt database on a shared connection.
    """

    def __init__(self, connection: pgconn):
        super().__init__(connection)

    def create_db_branch(self, branch_name: str) -> str:
        """
        Creates a new branch in the Dolt database and allow reading and writing
        data to that branch.
        """
        cur = self.conn.cursor()
        try:
            cur.execute(f"call dolt_checkout('-b', '{branch_name}');")
            return f"Branch '{branch_name}' created successfully."
        except Exception as e:
            return f"Failed to create branch: {str(e)}"
        finally:
            cur.close()

    def connect_db_branch(self, branch_name: str) -> str:
        cur = self.conn.cursor()
        try:
            cur.execute(f"call dolt_checkout('{branch_name}');")
            return f"Switched to branch '{branch_name}' successfully."
        except Exception as e:
            return f"Failed to switch branch: {str(e)}"
        finally:
            cur.close()

    def list_db_branches(self) -> list[str]:
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT name FROM dolt_branches;")
            branches = cur.fetchall()
            return {branch[0] for branch in branches}
        except Exception as e:
            return {f"Failed to list branches: {str(e)}"}
        finally:
            cur.close()
