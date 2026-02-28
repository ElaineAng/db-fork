import os
import threading

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
import dblib.result_collector as rc
import dblib.util as dbutil

PGSQL_USER = os.environ.get("PGSQL_USER", "elaineang")
PGSQL_PASSWORD = os.environ.get("PGSQL_PASSWORD", "")
PGSQL_HOST = os.environ.get("PGSQL_HOST", "localhost")
PGSQL_PORT = int(os.environ.get("PGSQL_PORT", "5433"))
PGSQL_DATA_DIR = os.environ.get("PGSQL_DATA_DIR", "")


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
        shared_branches_lock: threading.Lock,
        create_db_lock: threading.Lock,
    ):
        # Connect to the actual database for initial setup (loading SQL dump, etc.)
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
            shared_branches_lock=shared_branches_lock,
            create_db_lock=create_db_lock,
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
        shared_branches_lock: threading.Lock,
        create_db_lock: threading.Lock,
    ):
        super().__init__(connection, result_collector=collector)
        self._connection_uri = connection_uri
        self.autocommit = autocommit
        self.shared_branches = shared_branches
        self._shared_branches_lock = shared_branches_lock
        self._create_db_lock = create_db_lock
        self._all_branches_lock = threading.Lock()  # Per-instance lock for _all_branches

        # Thread-safe add to shared branches
        with self._shared_branches_lock:
            shared_branches.add(db_name)

        self._all_branches = dict()
        self._template_db = db_name  # Track the template database for creating branches

        # Set current branch to db_name since we're connected to it for setup
        self.current_branch_name = db_name

    def get_total_storage_bytes(self) -> int:
        """Get total physical storage across all branch databases.

        On macOS, PGSQL_DATA_DIR is required — it must point to an isolated
        volume (sparse disk image) so shutil.disk_usage() measures only DB
        files. See db_setup/setup_pg_volume.sh.

        On Linux, falls back to per-OID st_blocks (accurate without CoW).
        """
        if PGSQL_DATA_DIR:
            return dbutil.get_volume_usage_bytes(PGSQL_DATA_DIR)
        return self._get_storage_via_st_blocks()

    def _get_storage_via_st_blocks(self) -> int:
        """Fallback: sum st_blocks across per-OID directories.

        Accurate on Linux ext4/XFS where FILE_COPY does a real copy (no CoW).
        Would overcount on btrfs or XFS with reflinks — use PGSQL_DATA_DIR
        with an isolated volume in that case.
        """
        with self._shared_branches_lock:
            branch_names = list(self.shared_branches)
        if not branch_names:
            return 0

        cur = self.conn.cursor()
        cur.execute("SHOW data_directory;")
        pg_data_dir = cur.fetchone()[0]

        cur.execute(
            "SELECT oid FROM pg_database WHERE datname = ANY(%s);",
            (branch_names,),
        )
        oids = [str(row[0]) for row in cur.fetchall()]
        cur.close()

        base_dir = os.path.join(pg_data_dir, "base")
        total = 0
        for oid in oids:
            total += dbutil.get_directory_size_bytes(
                os.path.join(base_dir, oid)
            )
        return total

    def list_branches(self) -> list[str]:
        with self._shared_branches_lock:
            return list(self.shared_branches)

    def _create_branch_impl(self, branch_name: str, parent_name: str) -> None:
        # CREATE DATABASE ... TEMPLATE requires no active connections to the template database.
        # Close current connection if it's connected to the parent we want to use as template.
        temp_conn = None
        try:
            # Use template database as parent if no parent specified
            if not parent_name:
                parent_name = self._template_db

            # If we're currently connected to the parent database, disconnect first
            # (PostgreSQL requirement: template database must have 0 active connections)
            if self.current_branch_name == parent_name and self.conn and not self.conn.closed:
                self.conn.close()
                # Reconnect to neutral "postgres" database to maintain valid connection
                neutral_uri = self.__class__.get_default_connection_uri()
                self.conn = psycopg2.connect(neutral_uri)
                if self.autocommit:
                    self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            cmd = f"CREATE DATABASE {branch_name} TEMPLATE {parent_name} STRATEGY = FILE_COPY"

            # Serialize CREATE DATABASE operations to avoid PostgreSQL contention
            with self._create_db_lock:
                # Create temporary connection to neutral database
                temp_uri = self.__class__.get_default_connection_uri()
                temp_conn = psycopg2.connect(temp_uri)
                temp_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

                # Execute CREATE DATABASE on temporary connection
                cur = temp_conn.cursor()
                cur.execute(cmd)
                cur.close()

        except psycopg2.errors.DuplicateDatabase as e:
            raise Exception(
                f"Cannot create branch {branch_name}, already exists: {e}"
            )
        finally:
            if temp_conn:
                temp_conn.close()

        self.current_branch_name = branch_name

        # Thread-safe updates to shared state
        with self._all_branches_lock:
            self._all_branches[branch_name] = self.__class__.get_branch_uri(
                branch_name
            )

        with self._shared_branches_lock:
            self.shared_branches.add(branch_name)

    def _connect_branch_impl(self, branch_name: str) -> None:
        with self._all_branches_lock:
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
        conn = None
        cur = None
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
            self.branches = set()  # enforces unique name on branches
            self.db_name = db_name
            self.prev_method = ""

            # Thread synchronization locks for multi-threaded scenarios
            self.branches_lock = threading.Lock()  # Protects self.branches set
            self.create_db_lock = threading.Lock()  # Serializes CREATE DATABASE operations

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
