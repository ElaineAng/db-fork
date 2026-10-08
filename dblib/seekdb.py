"""SeekDB backend.

SeekDB (OceanBase) is MySQL-compatible, but it branches by forking whole
databases rather than keeping named branches inside one database. Each of our
branches is therefore its own database:

    main branch      -> <db_name>
    branch "X"       -> <db_name>__X

The mapping is derived from the names alone, so every worker (each with its own
connection) resolves the same branch to the same database with no shared state.

    branch     FORK DATABASE <parent db> TO <branch db>
    connect    USE <branch db>
    merge      MERGE TABLE <src db>.t INTO <target db>.t STRATEGY OURS, per table
    delete     DROP DATABASE <branch db>

SeekDB has no commits (commit/diff/log/rebase/revert/reset are unsupported).
Because every branch is a database on the same server, a multi-ref script can
address another branch as `<db>__<branch>`.`<table>`.

Merging is per table and, unlike Dolt's, in effect insert-only (the source's
updates and deletes aren't merged). Tables whose schemas differ between the
branches, or that have no primary key, are skipped. See _merge_impl.
"""

import os

import aiomysql
import pymysql
from pymysql.constants import CLIENT

from dblib.db_api import DBToolSuite, Ref
from dblib import mysql_common
import dblib.result_collector as rc
import dblib.util as dbutil

SEEKDB_USER = os.environ.get("SEEKDB_USER", "root")
SEEKDB_PASSWORD = os.environ.get("SEEKDB_PASSWORD", "")
SEEKDB_HOST = os.environ.get("SEEKDB_HOST", "127.0.0.1")
SEEKDB_PORT = int(os.environ.get("SEEKDB_PORT", "2881"))
# Where the SeekDB server keeps its data (the brew install's --base-dir/data).
SEEKDB_DATA_DIR = os.path.expanduser(
    os.environ.get("SEEKDB_DATA_DIR", "/opt/homebrew/var/seekdb/data")
)

# Branch name that maps to the root database itself.
MAIN_BRANCH = "main"

# Separates the root database name from the branch name in a branch database.
_BRANCH_SEP = "__"

# MySQL's limit on database name length.
_MAX_DB_NAME_LEN = 64

# Error SeekDB returns when MERGE TABLE's two tables have different columns or
# primary keys ("Schema error").
_ER_MERGE_SCHEMA_MISMATCH = 4029


def connect(db_name: str = None, autocommit: bool = True, **kwargs):
    """Open a PyMySQL connection to the SeekDB server."""
    return pymysql.connect(
        host=SEEKDB_HOST,
        port=SEEKDB_PORT,
        user=SEEKDB_USER,
        password=SEEKDB_PASSWORD,
        database=db_name,
        autocommit=autocommit,
        **kwargs,
    )


async def create_pool_async(db_name: str, size: int, autocommit: bool = True):
    """Open an aiomysql pool of `size` connections to the SeekDB server."""
    return await aiomysql.create_pool(
        minsize=size,
        maxsize=size,
        host=SEEKDB_HOST,
        port=SEEKDB_PORT,
        user=SEEKDB_USER,
        password=SEEKDB_PASSWORD,
        db=db_name,
        autocommit=autocommit,
    )


def _quote(ident: str) -> str:
    """Quote a MySQL identifier (database or table name)."""
    return "`" + ident.replace("`", "``") + "`"


def branch_db_name(db_name: str, branch_name: str) -> str:
    """Database that holds branch_name of the root database db_name."""
    if branch_name == MAIN_BRANCH:
        return db_name
    name = f"{db_name}{_BRANCH_SEP}{branch_name}"
    if len(name) > _MAX_DB_NAME_LEN:
        raise ValueError(
            f"SeekDB database name '{name}' for branch '{branch_name}' is longer "
            f"than {_MAX_DB_NAME_LEN} characters"
        )
    return name


def _branch_databases(cur, db_name: str) -> list:
    """Names of all branch databases forked from db_name (not db_name itself)."""
    prefix = db_name + _BRANCH_SEP
    cur.execute("SHOW DATABASES;")
    return [name for (name,) in cur.fetchall() if name.startswith(prefix)]


def load_sql_dump(db_name: str, sql_path: str) -> None:
    """Load a SQL file (plain SQL or pg_dump output) into db_name."""
    conn = connect(db_name, client_flag=CLIENT.MULTI_STATEMENTS)
    try:
        mysql_common.load_sql_dump(conn, sql_path)
    finally:
        conn.close()


def setup_database(db_name: str, sql_path: str) -> None:
    """(Re)create db_name, with no branches, and load sql_path into it."""
    drop_database(db_name)
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {_quote(db_name)};")
        print("Database created successfully.")
    finally:
        conn.close()
    load_sql_dump(db_name, sql_path)


def drop_database(db_name: str) -> None:
    """Drop db_name and every branch database forked from it."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            for branch_db in _branch_databases(cur, db_name):
                cur.execute(f"DROP DATABASE IF EXISTS {_quote(branch_db)};")
            cur.execute(f"DROP DATABASE IF EXISTS {_quote(db_name)};")
        print(f"Database '{db_name}' deleted successfully.")
    finally:
        conn.close()


class SeekDBToolSuite(DBToolSuite):
    BACKEND_NAME = "seekdb"
    SUPPORTS_COMMIT_REFS = False
    SUPPORTS_MULTI_REF_EXEC = True

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return cls.get_initial_connection_uri("")

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return f"mysql://{SEEKDB_USER}:{SEEKDB_PASSWORD}@{SEEKDB_HOST}:{SEEKDB_PORT}/{db_name}"

    @classmethod
    def init_for_bench(
        cls,
        collector: rc.ResultCollector,
        db_name: str,
        default_branch_name: str = MAIN_BRANCH,
        measure_storage: bool = False,
    ):
        return cls(connect(db_name), collector, db_name, default_branch_name,
                   measure_storage)

    def __init__(
        self,
        connection,
        collector: rc.ResultCollector,
        db_name: str,
        default_branch_name: str = MAIN_BRANCH,
        measure_storage: bool = False,
    ):
        super().__init__(connection, collector, measure_storage)
        # Root database; every branch database name is derived from it.
        self.db_name = db_name
        self.default_branch = default_branch_name
        self._current_db = None
        # (table, reason) pairs already warned about, so a table skipped on
        # every merge is reported once.
        self._merge_skip_warned = set()
        self._connect_impl(Ref(default_branch_name))
        self._current_ref = Ref(default_branch_name)

    # ------------------------------------------------------------------
    # Branch name <-> database name
    # ------------------------------------------------------------------

    def _db_for(self, branch: str) -> str:
        return branch_db_name(self.db_name, branch)

    def _use(self, db: str) -> None:
        self._execute(f"USE {_quote(db)};")
        self._current_db = db

    def _get_table_columns(self, table_name: str) -> list:
        rows = self._execute(mysql_common.TABLE_COLUMNS_QUERY, (table_name,))
        return mysql_common.normalize_column_types(rows)

    def list_branches(self) -> list:
        with self.conn.cursor() as cur:
            branch_dbs = _branch_databases(cur, self.db_name)
        prefix = self.db_name + _BRANCH_SEP
        return [MAIN_BRANCH] + [db[len(prefix):] for db in branch_dbs]

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def _storage_bytes(self) -> int:
        """Size of the server's data directory. SeekDB keeps all databases
        in one store, so this covers the whole server."""
        return dbutil.get_directory_size_bytes(SEEKDB_DATA_DIR)

    def _connect_impl(self, ref: Ref) -> None:
        self._use(self._db_for(ref.branch))

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        self._execute(
            f"FORK DATABASE {_quote(self._db_for(from_ref.branch))} "
            f"TO {_quote(branch_db_name(self.db_name, name))};"
        )

    def _merge_impl(self, into: Ref, source: Ref, message: str) -> dict:
        """Merge source into target one table at a time.

        DRAFT. SeekDB's merge differs from Dolt's, so results aren't directly
        comparable:

        - Per table: SeekDB has no database-level merge, so each table is
          merged with its own MERGE TABLE statement, in name order. The merge
          as a whole isn't atomic.
        - Insert only, in effect: MERGE TABLE compares the two tables
          directly, with no common ancestor, so every differing row is a
          conflict and STRATEGY OURS keeps the target's version. Only rows
          whose primary key is missing from the target are added.
        - SeekDB doesn't report how many conflicts it resolved.
        - Tables whose schemas differ (error 4029), that exist only on the
          source, or that have no primary key are skipped with a warning.
        - Not transactional: each MERGE TABLE takes effect at once.
        - ``message`` is unused (no commits).

        Returns {"fast_forward": False, "conflicts": None, "hash": "",
                 "merged_tables": [...], "skipped_tables": {table: reason}}.
        """
        source_db = self._db_for(source.branch)
        target_db = self._db_for(into.branch)
        if source_db == target_db:
            raise ValueError(f"Cannot merge branch '{source.branch}' into itself")

        source_tables = self._tables_with_pk_flag(source_db)
        target_tables = self._tables_with_pk_flag(target_db)

        merged, skipped = [], {}
        for table, source_has_pk in source_tables.items():
            if table not in target_tables:
                skipped[table] = "not in the target branch"
            elif not (source_has_pk and target_tables[table]):
                skipped[table] = "no primary key"
            elif self._merge_table(source_db, target_db, table):
                merged.append(table)
            else:
                skipped[table] = "schemas differ between branches"

        for table, reason in skipped.items():
            if (table, reason) not in self._merge_skip_warned:
                self._merge_skip_warned.add((table, reason))
                print(f"Warning: SeekDB merge skips table '{table}' ({reason})")

        return {
            "fast_forward": False,
            "conflicts": None,
            "hash": "",
            "merged_tables": merged,
            "skipped_tables": skipped,
        }

    def _merge_table(self, source_db: str, target_db: str, table: str) -> bool:
        """MERGE TABLE source_db.table INTO target_db.table keeping the
        target's rows on conflict. False if the schemas differ."""
        sql = (
            f"MERGE TABLE {_quote(source_db)}.{_quote(table)} "
            f"INTO {_quote(target_db)}.{_quote(table)} STRATEGY OURS;"
        )
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)
        except pymysql.MySQLError as e:
            if e.args and e.args[0] == _ER_MERGE_SCHEMA_MISMATCH:
                return False
            raise
        return True

    def _tables_with_pk_flag(self, db: str) -> dict:
        rows = self._execute(
            """
            SELECT t.table_name, c.constraint_name IS NOT NULL
            FROM information_schema.tables t
            LEFT JOIN information_schema.table_constraints c
                ON c.table_schema = t.table_schema
                AND c.table_name = t.table_name
                AND c.constraint_type = 'PRIMARY KEY'
            WHERE t.table_schema = %s AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name;
            """,
            (db,),
        )
        return {table: bool(has_pk) for table, has_pk in rows or []}

    def _delete_impl(self, ref: Ref) -> None:
        """Drop the branch's database. Forks don't depend on the database
        they were forked from, so even main can be dropped."""
        db = self._db_for(ref.branch)
        if db == self._current_db:
            self._use(self._db_for(self.default_branch))
            self._current_ref = Ref(self.default_branch)
        self._execute(f"DROP DATABASE {_quote(db)};")

    def _qualified_table(self, ref: Ref, table: str) -> str:
        return f"{_quote(self._db_for(ref.branch))}.{_quote(table)}"

    # ------------------------------------------------------------------
    # Async: an aiomysql pool; each pool connection remembers its database.
    # ------------------------------------------------------------------

    async def open_async_pool(self, size: int) -> None:
        self.async_pool = await create_pool_async(self.db_name, size)

    def _pool_connection(self):
        return mysql_common.aiomysql_pool_connection(self.async_pool)

    async def close_async_pool(self) -> None:
        if self.async_pool:
            self.async_pool.close()
            await self.async_pool.wait_closed()
            self.async_pool = None

    async def _connect_impl_async(self, conn, ref: Ref):
        db = self._db_for(ref.branch)
        if getattr(conn, "_seekdb_db", None) != db:
            async with conn.cursor() as cur:
                await cur.execute(f"USE {_quote(db)};")
            conn._seekdb_db = db
        return conn
