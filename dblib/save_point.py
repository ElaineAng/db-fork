from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
import dblib.result_collector as rc
import dblib.util as dbutil

PGSQL_USER = "postgres"
PGSQL_PASSWORD = "password"
PGSQL_HOST = "localhost"
PGSQL_PORT = 5432

class SavePointToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a PGSQL database on a shared connection.
    """

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return dbutil.format_db_uri(
            PGSQL_USER, PGSQL_PASSWORD, PGSQL_HOST, PGSQL_PORT, "postgres"
        )

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return dbutil.format_db_uri(
            PGSQL_USER, PGSQL_PASSWORD, PGSQL_HOST, PGSQL_PORT, db_name
        )

    @classmethod
    def get_connection(cls, cur_conn: _pgconn, db_name: str) -> _pgconn:
        """ Returns a psycopg2 connection object with an open transaction """
        if cur_conn and not cur_conn.closed:
            return cur_conn
        uri = cls.get_initial_connection_uri(db_name)
        try:
            conn = psycopg2.connect(uri)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            try:
                cur = conn.cursor()
                cur.execute("BEGIN;")
            except Exception as e:
                raise Exception(f"Failed to begin transaction: {e}") 
        except Exception as e:
            raise Exception(f"Failed to open initial connection: {e}")
        return conn

    @classmethod
    def init_for_bench(
        cls,
        collector: rc.ResultCollector,
        db_name: str,
        autocommit: bool,
        default_branch_name: str,
        conn: _pgconn = None
    ):
        return cls(
            connection=conn,
            collector=collector,
            autocommit=autocommit,
            default_branch_name=default_branch_name,
        )

    def __init__(
        self,
        connection: _pgconn,
        collector: rc.ResultCollector,
        autocommit: bool,
        default_branch_name: str,
    ):
        print("IN CTOR")
        super().__init__(connection, result_collector=collector)
        self.autocommit = autocommit

        self._save_points: list[str] = list() # TODO ordered list of savepoints ?
        self._create_branch_impl(default_branch_name)
        self._connect_branch_impl(default_branch_name)

    def list_branches(self) -> list[str]:
        return self._save_points

    def _create_branch_impl(self, branch_name: str, parent_id: str = None) -> None:
        """
        Creates a new SAVEPOINT in the PGSQL database transaction.
        """
        if parent_id and parent_id != self._save_points[-1]:
            raise Exception("Tried to branch from earlier save point, spine shape only allowed")
        if not self._in_transaction():
            raise Exception("Tried to create save point without being in transaction")
        cmd = f"SAVEPOINT {branch_name};"
        print(cmd)
        res = super().execute_sql(cmd)
        self._save_points.append(branch_name)

    def _connect_branch_impl(self, branch_name: str) -> None:
        """
        Connects to an existing branch in the PGSQL database to allow reads and
        writes on that branch.
        """
        if not self._in_transaction():
            raise Exception("Tried to ROLLBACK without being in transaction")
        cmd = f"ROLLBACK TO SAVEPOINT {branch_name};"
        super().execute_sql(cmd)

    def _get_current_branch_impl(self) -> tuple[str, str]:
        return (self._save_points[-1], self._save_points[-1])

    def _in_transaction(self):
        # source : https://dba.stackexchange.com/questions/208363/how-to-check-if-the-current-connection-is-in-a-transaction
        res = super().execute_sql("SELECT now() = statement_timestamp();")
        return not res[0][0]
