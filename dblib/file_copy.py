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
        return cls.get_branch_uri(db_name)

    @classmethod
    def init_for_bench(
            cls,
            collector: rc.ResultCollector,
            db_name: str,
            autocommit: bool,
            default_branch_name: str,
            shared_branches: set,
            ):
        uri = cls.get_branch_uri(db_name)

        conn = psycopg2.connect(uri)
        if autocommit:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return cls(
                connection=conn,
                collector=collector,
                db_name=db_name,
                connection_uri=uri,
                autocommit=autocommit,
                default_branch_name=default_branch_name,
                shared_branches=shared_branches,
                )

    def __init__(
            self,
            connection: _pgconn,
            collector: rc.ResultCollector,
            db_name: str,
            connection_uri: str,
            autocommit: bool,
            default_branch_name: str,
            shared_branches: set,
            ):
        super().__init__(connection, result_collector=collector)
        self._connection_uri        = connection_uri
        self.autocommit             = autocommit
        self.shared_branches        = shared_branches
        shared_branches.add(db_name)
        self._all_branches          = dict()
        self.current_branch_name    = ""

        # Initial connection is to template db (e.g. "microbench")
        # Switch to main
        if default_branch_name in self.shared_branches:
            self._connect_branch_impl(default_branch_name)
        else:
            self._create_branch_impl(default_branch_name, db_name)
            self._connect_branch_impl(default_branch_name)

    def list_branches(self) -> list[str]:
        return list(shared_branches)


    def _create_branch_impl(self, branch_name: str, parent_name: str) -> None:
        if parent_name:
            cmd = f"CREATE DATABASE {branch_name} TEMPLATE {parent_name} STRATEGY = FILE_COPY"
        else:
            cmd = f"CREATE DATABASE {branch_name}"
        try:
            super().execute_sql(cmd)
        except psycopg2.errors.DuplicateDatabase as e:
            raise Exception(f"Cannot create branch {branch_name}, already exists: {e}")
        self.current_branch_name = branch_name
        self._all_branches[branch_name] = self.__class__.get_branch_uri(branch_name)
        self.shared_branches.add(branch_name)

    def _connect_branch_impl(self, branch_name: str) -> None:
        if branch_name in self._all_branches:
            uri = self._all_branches[branch_name]
        else:
            uri = self.__class__.get_branch_uri(branch_name)
            self._all_branches[branch_name] = uri

        self.conn.close()
        self.conn = psycopg2.connect(uri)
        if self.autocommit:
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.current_branch_name = branch_name

    def _get_current_branch_impl(self) -> tuple[str, str]:
        # branch_name substituted for branch_id, allows _create_branch_impl to
        # work correctly with the way the API is called in runner.py
        return (self.current_branch_name, self.current_branch_name)

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
        except Exception as e:
            raise Exception(f"Error deleting database: {e}")
        finally:
            conn.close()

    @classmethod
    def cleanup(cls, info):
        conn    = None
        cur     = None
        db_name = info.db_name
        try:
            # Change back to original file copy method
            info.change_file_copy_method(info.prev_method)

            uri = cls.get_default_connection_uri()
            conn = psycopg2.connect(uri)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            for branch in info.branches:
                cur.execute(f"DROP DATABASE IF EXISTS {branch};")
            print(f"Database '{db_name}' deleted successfully.")
        except Exception as e:
            print(f"Error deleting database: {e}")
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()


    class FileCopyInfo:
        def __init__(self, db_name: str):
            # Meta object stores branch names for cleanup outside of FCTS class
            self.branches       = set() # enforces unique name on branches
            self.db_name        = db_name
            self.prev_method    = ""
            self.change_file_copy_method("clone")

        def change_file_copy_method(self, method: str) -> None:
            """Changes file_copy_method and stores old method"""
            conn = None
            cur = None
            uri = FileCopyToolSuite.get_default_connection_uri()
            try:
                conn = psycopg2.connect(uri)
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor()
                cur.execute("SHOW file_copy_method;")
                res = cur.fetchone()
                self.prev_method = res[0]
                cur.execute(f"ALTER SYSTEM SET file_copy_method = '{method}';")
                cur.execute("SELECT pg_reload_conf();")
                print(f"Changed file copy method to {method}")
            except Exception as e:
                print(f"Error changing file copy method: {e}")
            finally:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
