from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
import dblib.result_collector as rc
import dblib.util as dbutil
from collections import deque #TODO needed??

PGSQL_USER = "postgres"
PGSQL_PASSWORD = "password" #TODO env variable?
PGSQL_HOST = "localhost"
PGSQL_PORT = 5432


class FileCopyToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a PGSQL database on a shared connection.
    """

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return dbutil.format_db_uri(
            PGSQL_USER, PGSQL_PASSWORD, PGSQL_HOST, PGSQL_PORT, "postgres"
        )

    # TODO superfluous now with get_initial_connection_uri ??
    @classmethod
    def get_branch_uri(cls, branch_name) -> str:
        return dbutil.format_db_uri(
            PGSQL_USER, PGSQL_PASSWORD, PGSQL_HOST, PGSQL_PORT, branch_name
        )

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return dbutil.format_db_uri(
            PGSQL_USER, PGSQL_PASSWORD, PGSQL_HOST, PGSQL_PORT, db_name
        )

    @classmethod
    def init_for_bench(
        cls,
        collector: rc.ResultCollector,
        db_name: str,
        autocommit: bool,
        default_branch_name: str,
        shared_branches: deque,
    ):
        uri = FileCopyToolSuite.get_branch_uri(db_name)

        conn = psycopg2.connect(uri)
        if autocommit:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return cls(
            connection=conn,
            collector=collector,
            connection_uri=uri,
            autocommit=autocommit,
            default_branch_name=default_branch_name,
            shared_branches=shared_branches,
        )

    def __init__(
        self,
        connection: _pgconn,
        collector: rc.ResultCollector,
        connection_uri: str,
        autocommit: bool,
        default_branch_name: str,
        shared_branches: deque,
    ):
        super().__init__(connection, result_collector=collector)
        self._connection_uri = connection_uri
        self.autocommit = autocommit
        self.shared_branches = shared_branches

        self.old_file_copy_method = self.change_file_copy_method("clone")

        '''
        self.current_branch_name = default_branch_name
        self.main_branch_name = default_branch_name
        self._all_branches = {}
        self._create_branch_impl(default_branch_name, None)
        self._connect_branch_impl(default_branch_name)
        '''

        cmd = "SELECT CURRENT_DATABASE();"
        res = super().execute_sql(cmd)
        self.current_branch_name = res[0][0]
        self.main_branch_name = self.current_branch_name
        self.shared_branches.append(self.current_branch_name)
        self._all_branches = {self.current_branch_name: connection_uri}

        # Create a uri for "postgres" database to have somewhere to switch 
        # during cleanup to delete all created databases
        self._all_branches["postgres"] = FileCopyToolSuite.get_default_connection_uri()

    def change_file_copy_method(self, method: str) -> str:
        """Changes file_copy_method and returns old method"""
        res = super().execute_sql("SHOW file_copy_method;")
        prev_mode = res[0][0]
        super().execute_sql(f"ALTER SYSTEM SET file_copy_method = '{method}';")
        super().execute_sql("SELECT pg_reload_conf();")
        return prev_mode


    def get_uri_for_db_setup(self) -> str:
        """Returns the connection URI for database setup operations (e.g., PGSQL)."""
        return self._connection_uri

    @classmethod
    def cleanup(cls, shared_branches: deque):
        # TODO change file_copy_method back to copy?
        conn = None
        cur = None
        try:
            uri = FileCopyToolSuite.get_default_connection_uri()
            conn = psycopg2.connect(uri)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            for branch in shared_branches:
                cur.execute(f"DROP DATABASE IF EXISTS {branch};")
                # TODO remove after testing
                print(f"Database '{branch}' deleted successfully.")
        except Exception as e:
            print(f"Error deleting database: {e}")
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    def delete_db(self, db_name: str) -> None:
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

            # Change system file copy method back to what it was before run
        except Exception as e:
            raise Exception(f"Error deleting database: {e}")
        finally:
            conn.close()
        

    # Use parent_name instead of parent_id since there's no inherent id 
    # so it is simpler to just use names
    def _create_branch_impl(self, branch_name: str, parent_name: str) -> None:
        if parent_name:
            cmd = f"CREATE DATABASE {branch_name} TEMPLATE {parent_name} STRATEGY = FILE_COPY"
        else:
            cmd = f"CREATE DATABASE {branch_name}"
        super().execute_sql(cmd)
        self.current_branch_name = branch_name
        self._all_branches[branch_name] = FileCopyToolSuite.get_branch_uri(branch_name)
        self.shared_branches.append(branch_name)

    def _connect_branch_impl(self, branch_name: str) -> None:
        # TODO threading issue? kpg doesn't check at all
        if branch_name in self._all_branches:
            uri = self._all_branches[branch_name]
        else:
            uri = FileCopyToolSuite.get_branch_uri(branch_name)
            # Cache the URI
            self._all_branches[branch_name] = uri

        self.conn.close()
        self.conn = psycopg2.connect(uri)
        if self.autocommit:
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.current_branch_name = branch_name

    def _get_current_branch_impl(self) -> tuple[str, str]:
        return (self.current_branch_name, self.current_branch_name) # branch_id not implemented

