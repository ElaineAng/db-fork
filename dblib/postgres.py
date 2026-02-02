import os

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
import dblib.result_collector as rc
import dblib.util as dbutil

PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "")
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DATA_DIR = os.environ.get("PG_DATA_DIR", "")


class PostgresToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a PostgreSQL database with
    FILE_COPY branching support.

    Each "branch" is a cloned PostgreSQL database created via
    ``CREATE DATABASE ... STRATEGY=FILE_COPY`` (PostgreSQL 18+).
    With ``file_copy_method=clone`` configured on the server, this
    uses OS-level copy-on-write for near-instant cloning.
    """

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return dbutil.format_db_uri(
            PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, "postgres"
        )

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return dbutil.format_db_uri(
            PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, db_name
        )

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
            db_name=db_name,
        )

    def __init__(
        self,
        connection: _pgconn,
        collector: rc.ResultCollector,
        autocommit: bool,
        db_name: str = None,
    ):
        super().__init__(connection, result_collector=collector)
        self.autocommit = autocommit
        self._original_db_name = db_name or "postgres"
        self._current_branch_name = "main"
        self._current_db_name = self._original_db_name
        self._all_branches: dict[str, str] = {
            "main": self._original_db_name,
        }

    def list_branches(self) -> list[str]:
        return list(self._all_branches.keys())

    def _create_branch_impl(self, branch_name: str, parent_id: str = None) -> None:
        """Create a new branch by cloning a database via FILE_COPY strategy.

        Args:
            branch_name: Name of the new branch.
            parent_id: Database name of the parent to clone from.
                       If None, clones from the current database.
        """
        template_db = parent_id if parent_id else self._current_db_name
        branch_db_name = f"{self._original_db_name}_{branch_name}"

        # Close the current connection so the template has no active connections.
        self.conn.close()
        self.conn = None

        try:
            # Open a maintenance connection to the 'postgres' database.
            maint_uri = self.__class__.get_default_connection_uri()
            maint_conn = psycopg2.connect(maint_uri)
            maint_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            try:
                with maint_conn.cursor() as cur:
                    cur.execute(
                        f"CREATE DATABASE {branch_db_name} "
                        f"TEMPLATE {template_db} "
                        f"STRATEGY=FILE_COPY;"
                    )
            finally:
                maint_conn.close()

            # Cache the new branch.
            self._all_branches[branch_name] = branch_db_name
        finally:
            # Always reconnect to the current database, even if CREATE failed.
            current_uri = self.__class__.get_initial_connection_uri(
                self._current_db_name
            )
            self.conn = psycopg2.connect(current_uri)
            if self.autocommit:
                self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    def _connect_branch_impl(self, branch_name: str) -> None:
        """Connect to an existing branch by switching to its database.

        Args:
            branch_name: Name of the branch to connect to.
        """
        if branch_name not in self._all_branches:
            raise ValueError(f"Branch '{branch_name}' does not exist.")

        target_db = self._all_branches[branch_name]

        self.conn.close()
        self.conn = None

        uri = self.__class__.get_initial_connection_uri(target_db)
        self.conn = psycopg2.connect(uri)
        if self.autocommit:
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        self._current_branch_name = branch_name
        self._current_db_name = target_db

    def _get_current_branch_impl(self) -> tuple[str, str]:
        return (self._current_branch_name, self._current_db_name)

    def get_total_storage_bytes(self) -> int:
        """Get total storage by measuring the PostgreSQL data directory on disk.

        Uses ``dbutil.get_directory_size_bytes()`` on ``PG_DATA_DIR`` to capture
        actual physical disk usage including copy-on-write sharing effects.

        Returns:
            Total storage in bytes, or 0 if the directory doesn't exist.
        """
        return dbutil.get_directory_size_bytes(PG_DATA_DIR)

    def delete_db(self, db_name: str) -> None:
        """Drop all branch databases and the main database.

        Connects to the ``postgres`` maintenance database, drops every
        cloned branch database, then drops the main database.
        """
        if self.conn:
            self.conn.close()
            self.conn = None

        maint_uri = self.__class__.get_default_connection_uri()
        maint_conn = psycopg2.connect(maint_uri)
        maint_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            with maint_conn.cursor() as cur:
                # Drop all branch databases (skip main db, drop it last).
                for branch_name, branch_db in self._all_branches.items():
                    if branch_db == db_name:
                        continue
                    cur.execute(f"DROP DATABASE IF EXISTS {branch_db};")
                # Drop the main database.
                cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
        finally:
            maint_conn.close()
