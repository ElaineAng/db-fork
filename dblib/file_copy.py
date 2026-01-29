from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
import dblib.result_collector as rc
import dblib.util as dbutil
from collections import deque
from enum import Enum

PGSQL_USER = "postgres"
PGSQL_PASSWORD = "password"
PGSQL_HOST = "localhost"
PGSQL_PORT = 5432

class LockType(Enum):
    SHARED = 1
    EXCLUSIVE = 2

class FileCopyToolSuite(DBToolSuite):
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

        # TODO do we need to change back to old method?
        #self.old_file_copy_method = self.change_file_copy_method("clone")
        self.change_file_copy_method("clone")

        # Initial connection to default database
        cmd = "SELECT CURRENT_DATABASE();"
        res = super().execute_sql(cmd)

        self.current_branch_name = res[0][0]
        self.current_branch_id   = hash(self.current_branch_name)
        self.shared_branches.append(self.current_branch_name)
        self._all_branches = {
                self.current_branch_name:
                (self.current_branch_id, connection_uri)
                 }
        self._ids_to_names = {self.current_branch_id: self.current_branch_name}

        # Get lock
        self.lock = None
        self.get_shared_lock(self.current_branch_id)

        # Switch to main
        # TODO need a faster solution
        if default_branch_name in self.shared_branches:
            self._connect_branch_impl(default_branch_name)
        else:
            self._create_branch_impl(default_branch_name, self.current_branch_id)
            self._connect_branch_impl(default_branch_name)

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
        main_branch = shared_branches[0] if len(shared_branches) > 0 else None
        try:
            uri = FileCopyToolSuite.get_default_connection_uri()
            conn = psycopg2.connect(uri)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            for branch in shared_branches:
                cur.execute(f"DROP DATABASE IF EXISTS {branch};")
            if main_branch:
                print(f"Database '{main_branch}' deleted successfully.")
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
                locked = False
                if db_name in self._all_branches:
                    self.get_exclusive_lock(self._all_branches[db_name][1])
                    locked = True
                cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
                if locked:
                    self.drop_exclusive_lock(self._all_branches[db_name][1])
        except Exception as e:
            raise Exception(f"Error deleting database: {e}")
        finally:
            conn.close()
        
    def _create_branch_impl(self, branch_name: str, parent_id: str) -> None:
        parent_name = None
        if parent_id in self._ids_to_names:
            parent_name = self._ids_to_names[parent_id]
            # Get exclusive lock
            self.get_exclusive_lock(parent_id)
        if parent_name:
            cmd = f"CREATE DATABASE {branch_name} TEMPLATE {parent_name} STRATEGY = FILE_COPY"
            super().execute_sql(cmd)
            # Drop exclusive lock
            self.drop_exclusive_lock(parent_id)
        else:
            cmd = f"CREATE DATABASE {branch_name}"
            super().execute_sql(cmd)
        branch_id = hash(branch_name)
        self._all_branches[branch_name] = (
                branch_id, FileCopyToolSuite.get_branch_uri(branch_name)
                )
        self._ids_to_names[branch_id] = branch_name
        self.shared_branches.append(branch_name)

    def _connect_branch_impl(self, branch_name: str) -> None:

        if branch_name in self._all_branches:
            uri = self._all_branches[branch_name][1]
        else:
            uri = FileCopyToolSuite.get_branch_uri(branch_name)
            # Cache the URI
            self._all_branches[branch_name] = (hash(branch_name), uri)

        # Drop lock
        self.drop_shared_lock(self.current_branch_id)
        self.conn.close()

        self.current_branch_name = branch_name
        self.current_branch_id   = hash(branch_name)

        self.conn = psycopg2.connect(uri)
        # Get lock
        self.get_shared_lock(self.current_branch_id)
        if self.autocommit:
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    def _get_current_branch_impl(self) -> tuple[str, str]:
        # branch_name substituted for branch_id, allows _create_branch_impl to 
        # work correctly with the way the API is called in runner.py
        return (self.current_branch_name, self.current_branch_id)

    def get_shared_lock(self, branch_id: str):
        try:
            shared = f"SELECT pg_try_advisory_lock_shared({branch_id});"
            super().execute_sql(shared)
        except Exception as e:
            print(f"Error obtaining shared lock: {e}")
        self.lock = (LockType.SHARED, branch_id)

    def drop_shared_lock(self, branch_id: str):
        try:
            shared = f"SELECT pg_advisory_unlock_shared({branch_id});"
            super().execute_sql(shared)
        except Exception as e:
            print(f"Error dropping shared lock: {e}")
        self.lock = None

    def get_exclusive_lock(self, branch_id: str):
        if self.lock:
            if self.lock[0] == LockType.SHARED:
                self.drop_shared_lock(self.lock[1])
            elif self.lock[1] == branch_id:
                return
            else:
                pass 
                # TODO what to do if already hold other exclusive lock?
        try:
            exclusive = f"SELECT pg_try_advisory_lock({branch_id});"
            super().execute_sql(exclusive)
        except Exception as e:
            print(f"Error obtaining exclusive lock: {e}")

    def drop_exclusive_lock(self, branch_id: str):
        try:
            exclusive = f"SELECT pg_advisory_unlock({branch_id});"
            super().execute_sql(exclusive)
        except Exception as e:
            print(f"Error dropping exclusive lock: {e}")
        self.lock = None
