from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
import dblib.result_collector as rc
import dblib.util as dbutil

PGSQL_USER = "postgres"
PGSQL_PASSWORD = "password" #TODO env variable?
PGSQL_HOST = "localhost"
PGSQL_PORT = 5432


class PgsqlToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a PGSQL database on a shared connection.
    """

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return dbutil.format_db_uri(
            PGSQL_USER, PGSQL_PASSWORD, PGSQL_HOST, PGSQL_PORT, "postgres"
        )

    @classmethod
    def get_branch_uri(cls, branch_name) -> str:
        return dbutil.format_db_uri(
            PGSQL_USER, PGSQL_PASSWORD, PGSQL_HOST, PGSQL_PORT, branch_name
        )

    @classmethod
    def init_for_bench(
        cls,
        collector: rc.ResultCollector,
        db_name: str,
        autocommit: bool,
    ):
        uri = PgsqlToolSuite.get_branch_uri(db_name)

        conn = psycopg2.connect(uri)
        if autocommit:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return cls(
            connection=conn,
            collector=collector,
            connection_uri=uri,
            autocommit=autocommit,
        )

    def __init__(
        self,
        connection: _pgconn,
        collector: rc.ResultCollector,
        connection_uri: str,
        autocommit: bool,
    ):
        super().__init__(connection, result_collector=collector)
        self._connection_uri = connection_uri
        self.autocommit = autocommit

        cmd = "SELECT CURRENT_DATABASE();"
        res = super().execute_sql(cmd)
        self.current_branch_name = res[0][0]
        self.main_branch_name = self.current_branch_name
        self._all_branches = {self.current_branch_name: connection_uri}

        # Create a uri for "postgres" database to have somewhere to switch 
        # during cleanup to delete all created databases
        self._all_branches["postgres"] = PgsqlToolSuite.get_default_connection_uri()

    def get_uri_for_db_setup(self) -> str:
        """Returns the connection URI for database setup operations (e.g., PGSQL)."""
        return self._connection_uri

    def delete_db(self, db_name: str) -> None:
        """
        Deletes all or a single database depending on db_name.
        If db_name == main branch, delete all branch databases.
        Else delete the database associated with db_name.
        """
        try:
            # This function gets called with config.db_name in the __exit__
            # cleanup function, so delete all branches in that case
            if db_name == self.main_branch_name:
                self._connect_branch_impl("postgres")
                for branch, _ in self._all_branches.items():
                    if branch == "postgres":
                        continue
                    cmd = f"DROP DATABASE {branch};"
                    super().execute_sql(cmd)
            # Other wise just delete the specfic branch this function was 
            # called on
            else:
                cmd = f"DROP DATABASE {db_name}"
                super().execute_sql(cmd)
        except Exception as e:
            raise Exception(f"Error deleting database: {e}")
        

    # Use parent_name instead of parent_id since there's no inherent id 
    # so it is simpler to just use names
    def _create_branch_impl(self, branch_name: str, parent_name: str) -> None:
        cmd = f"CREATE DATABASE {branch_name} TEMPLATE {parent_name} STRATEGY = FILE_COPY"
        super().execute_sql(cmd)
        self.current_branch_name = branch_name
        self._all_branches[branch_name] = PgsqlToolSuite.get_branch_uri(branch_name)

    def _connect_branch_impl(self, branch_name: str) -> None:
        if branch_name not in self._all_branches:
            raise ValueError(f"Branch '{branch_name}' does not exist.")
        uri = self._all_branches[branch_name]
        if not uri:
            uri = PgsqlToolSuite.get_branch_uri(branch_name)
            # Cache the URI
            self._all_branches[branch_name] = uri

        self.conn.close()
        self.conn = psycopg2.connect(uri)
        if self.autocommit:
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.current_branch_name = branch_name

    def _get_current_branch_impl(self) -> tuple[str, str]:
        return (self.current_branch_name, self.current_branch_name) # branch_id not implemented

