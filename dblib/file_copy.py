"""Plain PostgreSQL backend where a branch is a database copy.

    branch   CREATE DATABASE <name> TEMPLATE <parent> STRATEGY = FILE_COPY
    connect  open a connection to the branch's database
    delete   DROP DATABASE <name>

Postgres has no commits and no cross-database queries, so everything else
is unsupported. CREATE DATABASE ... TEMPLATE needs no open connections on
the template, so branching from the connected database first moves the
connection to the neutral "postgres" database.
"""

import os
import threading

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite, Ref
import dblib.result_collector as rc
import dblib.util as dbutil

PGSQL_USER = os.environ.get("PGSQL_USER", "elaineang")
PGSQL_PASSWORD = os.environ.get("PGSQL_PASSWORD", "")
PGSQL_HOST = os.environ.get("PGSQL_HOST", "localhost")
PGSQL_PORT = int(os.environ.get("PGSQL_PORT", "5433"))
PGSQL_DATA_DIR = os.environ.get("PGSQL_DATA_DIR", "")


class FileCopyToolSuite(DBToolSuite):
    BACKEND_NAME = "file_copy"
    SUPPORTS_COMMIT_REFS = False
    SUPPORTS_MULTI_REF_EXEC = False

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return dbutil.format_db_uri(PGSQL_USER, PGSQL_PASSWORD, PGSQL_HOST, PGSQL_PORT, "postgres")

    @classmethod
    def get_branch_uri(cls, branch_name) -> str:
        return dbutil.format_db_uri(PGSQL_USER, PGSQL_PASSWORD, PGSQL_HOST, PGSQL_PORT, branch_name)

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return cls.get_branch_uri(db_name)

    @classmethod
    def init_for_bench(
        cls,
        collector: rc.ResultCollector,
        db_name: str,
        default_branch_name: str,
        shared_branches: set,
        shared_branches_lock: threading.Lock,
        create_db_lock: threading.Lock,
        measure_storage: bool = False,
    ):
        conn = psycopg2.connect(cls.get_branch_uri(default_branch_name or db_name))
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return cls(
            connection=conn,
            collector=collector,
            db_name=db_name,
            default_branch_name=default_branch_name or db_name,
            shared_branches=shared_branches,
            shared_branches_lock=shared_branches_lock,
            create_db_lock=create_db_lock,
            measure_storage=measure_storage,
        )

    def __init__(
        self,
        connection: _pgconn,
        collector: rc.ResultCollector,
        db_name: str,
        default_branch_name: str,
        shared_branches: set,
        shared_branches_lock: threading.Lock,
        create_db_lock: threading.Lock,
        measure_storage: bool = False,
    ):
        super().__init__(connection, collector, measure_storage)
        self.db_name = db_name
        self.default_branch = default_branch_name
        # Branch databases known across all workers (for storage accounting
        # and cleanup), and a lock serialising CREATE DATABASE.
        self.shared_branches = shared_branches
        self._shared_branches_lock = shared_branches_lock
        self._create_db_lock = create_db_lock
        with self._shared_branches_lock:
            shared_branches.add(db_name)
        self._current_ref = Ref(default_branch_name)

    def _connect_to(self, uri: str) -> None:
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = psycopg2.connect(uri)
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    def _go_neutral(self) -> None:
        """Move the connection off any branch database."""
        self._connect_to(self.get_default_connection_uri())
        self._current_ref = None

    def list_branches(self) -> list:
        with self._shared_branches_lock:
            return list(self.shared_branches)

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def _storage_bytes(self) -> int:
        """Physical storage of all branch databases.

        With PGSQL_DATA_DIR set (an isolated volume, see
        db_setup/setup_pg_volume.sh) this is the volume's usage, which is
        right under copy-on-write. Otherwise it sums st_blocks over the
        per-database directories, which is accurate without CoW.
        """
        if PGSQL_DATA_DIR:
            return dbutil.get_volume_usage_bytes(PGSQL_DATA_DIR)
        with self._shared_branches_lock:
            branch_names = list(self.shared_branches)
        if not branch_names or not self.conn:
            return 0
        with self.conn.cursor() as cur:
            cur.execute("SHOW data_directory;")
            pg_data_dir = cur.fetchone()[0]
            cur.execute("SELECT oid FROM pg_database WHERE datname = ANY(%s);", (branch_names,))
            oids = [str(row[0]) for row in cur.fetchall()]
        base_dir = os.path.join(pg_data_dir, "base")
        return sum(dbutil.get_directory_size_bytes(os.path.join(base_dir, oid)) for oid in oids)

    def _connect_impl(self, ref: Ref) -> None:
        self._connect_to(self.get_branch_uri(ref.branch))

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        parent = from_ref.branch
        if self._current_ref and self._current_ref.branch == parent:
            # The template must have no connections.
            self._go_neutral()
        temp_conn = None
        try:
            with self._create_db_lock:
                temp_conn = psycopg2.connect(self.get_default_connection_uri())
                temp_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                with temp_conn.cursor() as cur:
                    cur.execute(
                        f"CREATE DATABASE {name} TEMPLATE {parent} STRATEGY = FILE_COPY"
                    )
        except psycopg2.errors.DuplicateDatabase as e:
            raise Exception(f"Cannot create branch {name}, already exists: {e}")
        finally:
            if temp_conn:
                temp_conn.close()
        with self._shared_branches_lock:
            self.shared_branches.add(name)

    def _delete_impl(self, ref: Ref) -> None:
        if self._current_ref and self._current_ref.branch == ref.branch:
            self._go_neutral()
        temp_conn = psycopg2.connect(self.get_default_connection_uri())
        temp_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            with temp_conn.cursor() as cur:
                cur.execute(f"DROP DATABASE {ref.branch};")
        finally:
            temp_conn.close()
        with self._shared_branches_lock:
            self.shared_branches.discard(ref.branch)

    # ------------------------------------------------------------------
    # Async: a psycopg pool on the branch the suite is on when opened; a
    # script on another branch gets its own connection.
    # ------------------------------------------------------------------

    async def open_async_pool(self, size: int) -> None:
        from psycopg_pool import AsyncConnectionPool

        if self._current_ref is None:
            raise ValueError("Not connected to a branch")
        self._pool_branch = self._current_ref.branch
        self.async_pool = AsyncConnectionPool(
            self.get_branch_uri(self._pool_branch),
            min_size=size, max_size=size, kwargs={"autocommit": True}, open=False,
        )
        await self.async_pool.open(wait=True)

    async def _connect_impl_async(self, conn, ref: Ref):
        if ref.branch == self._pool_branch:
            return conn
        import psycopg

        return await psycopg.AsyncConnection.connect(
            self.get_branch_uri(ref.branch), autocommit=True
        )

    # ------------------------------------------------------------------
    # Cleanup helpers used by the runners
    # ------------------------------------------------------------------

    @classmethod
    def cleanup(cls, info):
        conn = None
        cur = None
        try:
            info.change_file_copy_method(info.prev_method)
            conn = psycopg2.connect(cls.get_default_connection_uri())
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            for branch in info.branches:
                cur.execute(f"DROP DATABASE IF EXISTS {branch};")
            print(f"Database '{info.db_name}' deleted successfully.")
        except Exception as e:
            print(f"Error deleting database: {e}")
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    class FileCopyInfo:
        """Branch names and locks shared by every worker's suite."""

        def __init__(self, db_name: str):
            self.branches = set()
            self.db_name = db_name
            self.prev_method = ""
            self.branches_lock = threading.Lock()
            self.create_db_lock = threading.Lock()
            self.change_file_copy_method("clone")

        def change_file_copy_method(self, method: str) -> None:
            """Set file_copy_method server-wide, remembering the old value."""
            conn = None
            cur = None
            try:
                conn = psycopg2.connect(FileCopyToolSuite.get_default_connection_uri())
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor()
                cur.execute("SHOW file_copy_method;")
                self.prev_method = cur.fetchone()[0]
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
