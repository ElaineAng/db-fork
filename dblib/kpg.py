from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
import dblib.result_collector as rc
import dblib.util as dbutil

KPG_USER = "elaineang"
KPG_HOST = "localhost"
KPG_PORT = 5433


class KpgToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a KPG database on a shared connection.
    """

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return dbutil.format_db_uri(
            KPG_USER, "", KPG_HOST, KPG_PORT, "postgres"
        )

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return dbutil.format_db_uri(KPG_USER, "", KPG_HOST, KPG_PORT, db_name)

    @classmethod
    def init_for_bench(
        cls,
        collector: rc.ResultCollector,
        db_name: str,
        autocommit: bool,
    ):
        uri = cls.get_initial_connection_uri(db_name)

        conn = psycopg2.connect(uri)
        if autocommit:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return cls(
            connection=conn,
            collector=collector,
            autocommit=autocommit,
        )

    def __init__(
        self,
        connection: _pgconn,
        collector: rc.ResultCollector,
        autocommit: bool,
    ):
        super().__init__(connection, result_collector=collector)
        self.autocommit = autocommit
        self._fork_name_to_id = {"main": 0}
        self._fork_id_to_name = {0: "main"}
        self._current_fork_id = 0

    def delete_db(self, db_name: str) -> None:
        """
        Connects to the default postgres database and drops the benchmark
        database.
        """
        # Close current connection first
        if self.conn:
            self.conn.close()
            self.conn = None

        # Connect to the default postgres database
        default_uri = self.__class__.get_default_connection_uri()
        conn = psycopg2.connect(default_uri)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
        finally:
            conn.close()

    def list_branches(self) -> list[str]:
        # KPG currently does not support listing branches.
        raise NotImplementedError

    def _create_branch_impl(self, branch_name: str, parent_id: str) -> None:
        """
        Creates a new branch in the KPG database. This is currently hacky and
        might be slightly expensive than it should be since the CREATE DBFORK
        command prints out the fork id in the info log.
        """
        cmd = "CREATE DBFORK"
        super().execute_sql(cmd)

        # Parse fork ID from notices (format: "Current fork id globally: <id>")
        fork_id = None
        for notice in self.conn.notices:
            if "Current fork id globally:" in notice:
                # Extract the ID from the notice
                parts = notice.split("Current fork id globally:")
                if len(parts) > 1:
                    fork_id = int(parts[1].strip())
                    break

        if fork_id is None:
            raise ValueError("Failed to get fork ID from CREATE DBFORK")

        self._fork_name_to_id[branch_name] = fork_id
        self._fork_id_to_name[fork_id] = branch_name
        # print(f"Created branch {branch_name} with ID {fork_id}")

    def _connect_branch_impl(self, branch_name: str) -> None:
        """
        Connects to an existing branch in the Dolt database to allow reads and
        writes on that branch.
        """
        self._current_fork_id = self._fork_name_to_id[branch_name]
        # DROP DBFORK is currently a misnomer.
        cmd = f"DROP DBFORK {self._current_fork_id}"
        super().execute_sql(cmd)

    def _get_current_branch_impl(self) -> tuple[str, str]:
        return (
            self._fork_id_to_name[self._current_fork_id],
            self._current_fork_id,
        )
