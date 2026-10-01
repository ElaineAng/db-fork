import os
import re
import weakref
from contextlib import asynccontextmanager

import aiomysql
import pymysql
from pymysql.constants import CLIENT

from dblib.db_api import DBToolSuite
import dblib.result_collector as rc
import dblib.util as dbutil

DOLT_MYSQL_USER = os.environ.get("DOLT_MYSQL_USER", "root")
DOLT_MYSQL_PASSWORD = os.environ.get("DOLT_MYSQL_PASSWORD", "")
DOLT_MYSQL_HOST = os.environ.get("DOLT_MYSQL_HOST", "127.0.0.1")
DOLT_MYSQL_PORT = int(os.environ.get("DOLT_MYSQL_PORT", "3306"))
DOLT_MYSQL_DATA_DIR = os.path.expanduser(
    os.environ.get("DOLT_MYSQL_DATA_DIR", "~/dolt/databases")
)

# Rows per multi-row INSERT when converting pg_dump COPY blocks.
_LOAD_BATCH_ROWS = 2000

# Postgres type names (as written by pg_dump) -> MySQL equivalents.
_PG_TYPE_MAP = [
    (re.compile(r"\bcharacter varying\b", re.I), "VARCHAR"),
    (re.compile(r"\bcharacter\b", re.I), "CHAR"),
    (re.compile(r"\btimestamp without time zone\b", re.I), "DATETIME(6)"),
    (re.compile(r"\bdouble precision\b", re.I), "DOUBLE"),
]

_COPY_RE = re.compile(r"^COPY\s+(?:public\.)?(\w+)\s*\(([^)]*)\)\s+FROM\s+stdin;", re.I)
_COPY_ESCAPES = {"t": "\t", "n": "\n", "r": "\r", "b": "\b", "f": "\f", "v": "\v", "\\": "\\"}


def connect(db_name: str = None, autocommit: bool = True, **kwargs):
    """Open a PyMySQL connection to the Dolt sql-server."""
    return pymysql.connect(
        host=DOLT_MYSQL_HOST,
        port=DOLT_MYSQL_PORT,
        user=DOLT_MYSQL_USER,
        password=DOLT_MYSQL_PASSWORD,
        database=db_name,
        autocommit=autocommit,
        **kwargs,
    )


async def create_pool_async(db_name: str, size: int, autocommit: bool = True):
    """Open an aiomysql pool of `size` connections to the Dolt sql-server."""
    return await aiomysql.create_pool(
        minsize=size,
        maxsize=size,
        host=DOLT_MYSQL_HOST,
        port=DOLT_MYSQL_PORT,
        user=DOLT_MYSQL_USER,
        password=DOLT_MYSQL_PASSWORD,
        db=db_name,
        autocommit=autocommit,
    )


def _unescape_copy_field(field: str):
    """Decode one field of pg_dump's COPY text format (\\N is NULL)."""
    if field == "\\N":
        return None
    if "\\" not in field:
        return field
    return re.sub(r"\\(.)", lambda m: _COPY_ESCAPES.get(m.group(1), m.group(1)), field)


def _pg_to_mysql_ddl(stmt: str) -> str:
    """Translate a pg_dump DDL statement to MySQL syntax."""
    stmt = stmt.replace("public.", "").replace("ALTER TABLE ONLY ", "ALTER TABLE ")
    for pattern, replacement in _PG_TYPE_MAP:
        stmt = pattern.sub(replacement, stmt)
    return stmt


def _is_pg_only(stmt: str) -> bool:
    """Session settings and psql meta-commands that have no MySQL meaning."""
    s = stmt.lstrip().upper()
    return s.startswith(("SET ", "SELECT PG_CATALOG.", "\\"))


def load_sql_dump(db_name: str, sql_path: str) -> None:
    """Load a SQL file into db_name.

    Accepts plain SQL (e.g. ch_benchmark_seed.sql) as well as pg_dump
    output (e.g. ch-w1.sql): COPY blocks become batched INSERTs, Postgres
    type names are translated, and Postgres-only settings are skipped.
    """
    conn = connect(db_name, client_flag=CLIENT.MULTI_STATEMENTS)
    cur = conn.cursor()
    try:
        stmt_lines = []
        with open(sql_path) as f:
            for line in f:
                copy_match = _COPY_RE.match(line) if not stmt_lines else None
                if copy_match:
                    table, cols = copy_match.groups()
                    _load_copy_block(cur, f, table, cols)
                    continue
                if not stmt_lines and (not line.strip() or line.startswith("--")):
                    continue
                stmt_lines.append(line)
                if line.rstrip().endswith(";"):
                    stmt = "".join(stmt_lines)
                    stmt_lines = []
                    if not _is_pg_only(stmt):
                        cur.execute(_pg_to_mysql_ddl(stmt))
                        while cur.nextset():
                            pass
    finally:
        cur.close()
        conn.close()


def _load_copy_block(cur, f, table: str, cols: str) -> None:
    """Consume a COPY ... FROM stdin block from f and insert its rows."""
    num_cols = len(cols.split(","))
    placeholders = "(" + ", ".join(["%s"] * num_cols) + ")"
    sql = f"INSERT INTO {table} ({cols}) VALUES {placeholders}"
    batch = []
    for line in f:
        if line.rstrip("\n") == "\\.":
            break
        batch.append([_unescape_copy_field(v) for v in line.rstrip("\n").split("\t")])
        if len(batch) >= _LOAD_BATCH_ROWS:
            cur.executemany(sql, batch)
            batch = []
    if batch:
        cur.executemany(sql, batch)


def setup_database(db_name: str, sql_path: str) -> None:
    """(Re)create db_name, load sql_path into it, and commit it on main."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
            cur.execute(f"CREATE DATABASE {db_name};")
        print("Database created successfully.")
    finally:
        conn.close()

    load_sql_dump(db_name, sql_path)

    conn = connect(db_name)
    try:
        with conn.cursor() as cur:
            cur.execute("CALL DOLT_COMMIT('-Am', 'Load SQL schema');")
        print("Dolt schema committed: Load SQL schema")
    finally:
        conn.close()


def drop_database(db_name: str) -> None:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
        print(f"Database '{db_name}' deleted successfully.")
    finally:
        conn.close()


class DoltMySQLToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with Dolt's MySQL-compatible sql-server
    on a shared connection.
    """

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return cls.get_initial_connection_uri("")

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return (
            f"mysql://{DOLT_MYSQL_USER}:{DOLT_MYSQL_PASSWORD}"
            f"@{DOLT_MYSQL_HOST}:{DOLT_MYSQL_PORT}/{db_name}"
        )

    @classmethod
    def init_for_bench(
        cls,
        collector: rc.ResultCollector,
        db_name: str,
        autocommit: bool,
        default_branch_name: str,
    ):
        conn = connect(db_name, autocommit=autocommit)
        return cls(
            connection=conn,
            collector=collector,
            autocommit=autocommit,
            default_branch_name=default_branch_name,
            db_name=db_name,
        )

    def __init__(
        self,
        connection,
        collector: rc.ResultCollector,
        autocommit: bool,
        default_branch_name: str,
        db_name: str = None,
    ):
        super().__init__(connection, result_collector=collector)
        self._connect_branch_impl(default_branch_name)
        self.autocommit = autocommit
        self.db_name = db_name
        # Set by open_async_pool().
        self._async_branch = None
        self._configured_conns = None

    def _get_table_columns(self, table_name: str) -> list[tuple]:
        # MySQL's information_schema spans every database on the server, so
        # filter to the current one; DATA_TYPE is MySQL's udt_name equivalent.
        query = """
        SELECT
            column_name,
            data_type,
            is_nullable,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM
            information_schema.columns
        WHERE
            table_schema = DATABASE() AND table_name = %s
        ORDER BY
            ordinal_position;
        """
        rows = super().execute_sql(query, (table_name,))
        # The data generator knows Postgres type names; MySQL reports
        # TIMESTAMP WITHOUT TIME ZONE columns (loaded as DATETIME) as datetime.
        return [
            (name, "timestamp" if dtype == "datetime" else dtype, *rest)
            for name, dtype, *rest in rows
        ]

    def list_branches(self) -> list[str]:
        cmd = "SELECT name FROM dolt_branches;"
        return [branch[0] for branch in super().execute_sql(cmd)]

    def _prepare_commit(self, message: str = "") -> None:
        try:
            super().execute_sql("CALL DOLT_ADD('-A');")
            super().execute_sql("CALL DOLT_COMMIT('-m', %s);", (message,))
        except Exception as e:
            # Ignore commit errors (e.g., no changes to commit).
            print(f"Commit failed: {e}")

    def _create_branch_impl(
        self, branch_name: str, parent_id: str = None
    ) -> None:
        """
        Creates a new branch in the Dolt database.
        """
        # Only checkout to parent if specified, otherwise create from current branch
        if parent_id:
            self._connect_branch_impl(parent_id)
        super().execute_sql("CALL DOLT_CHECKOUT('-b', %s);", (branch_name,))

    def _connect_branch_impl(self, branch_name: str) -> None:
        """
        Connects to an existing branch in the Dolt database to allow reads and
        writes on that branch.
        """
        super().execute_sql("CALL DOLT_CHECKOUT(%s);", (branch_name,))

    def _get_current_branch_impl(self) -> tuple[str, str]:
        result = super().execute_sql("SELECT active_branch();")
        # Dolt's branch name is unique and can be used as an ID.
        return (result[0][0], result[0][0])

    def _merge_branch_impl(self, source_branch: str, message: str = "") -> dict:
        """Merge source_branch into the currently checked-out branch.

        Mirrors DoltToolSuite._merge_branch_impl. Returns
        {"fast_forward": bool, "conflicts": int, "hash": str}.
        """
        # Ensure any pending changes are committed before merge.
        try:
            super().execute_sql("CALL DOLT_ADD('-A');")
            super().execute_sql(
                "CALL DOLT_COMMIT('-m', 'pre-merge commit', '--allow-empty');"
            )
        except Exception:
            pass  # No pending changes is fine.

        # With autocommit on, Dolt rolls back a merge that produces conflicts
        # unless this is set, so we could never reach the resolution below.
        super().execute_sql("SET @@dolt_allow_commit_conflicts = 1;")

        if message:
            result = super().execute_sql(
                "CALL DOLT_MERGE(%s, '-m', %s);", (source_branch, message)
            )
        else:
            result = super().execute_sql("CALL DOLT_MERGE(%s);", (source_branch,))

        # DOLT_MERGE returns (hash, fast_forward, conflicts, message)
        info = {}
        if result and result[0]:
            row = result[0]
            info["hash"] = row[0] if len(row) > 0 else ""
            info["fast_forward"] = bool(row[1]) if len(row) > 1 else False
            info["conflicts"] = int(row[2]) if len(row) > 2 else 0

        # Auto-resolve conflicts with --ours strategy if any.
        if info.get("conflicts", 0) > 0:
            try:
                tables = super().execute_sql("SELECT `table` FROM dolt_conflicts;")
                for (table_name,) in tables or []:
                    super().execute_sql(
                        "CALL DOLT_CONFLICTS_RESOLVE('--ours', %s);", (table_name,)
                    )
                super().execute_sql("CALL DOLT_ADD('-A');")
                super().execute_sql(
                    "CALL DOLT_COMMIT('-m', 'resolved merge conflicts');"
                )
            except Exception as e:
                print(f"Warning: conflict resolution failed: {e}")

        return info

    def _delete_branch_impl(self, branch_name: str, branch_id: str) -> None:
        """Delete a branch; must NOT be on the branch being deleted."""
        super().execute_sql("CALL DOLT_BRANCH('-D', %s);", (branch_name,))

    def get_total_storage_bytes(self) -> int:
        """Get total storage by measuring the Dolt data directory on disk."""
        if not self.db_name:
            return 0
        return dbutil.get_directory_size_bytes(
            os.path.join(DOLT_MYSQL_DATA_DIR, self.db_name)
        )

    # ========================================================================
    # Async implementations
    # ========================================================================

    async def open_async_pool(self, size: int, branch_name: str) -> None:
        """Open an aiomysql pool of `size` connections on branch_name.

        A new session starts on the default branch, so each pool connection
        checks out branch_name before its first use. Otherwise async ops
        would run on main instead of the worker's branch.
        """
        self._async_branch = branch_name
        self._configured_conns = weakref.WeakSet()
        self.async_pool = await create_pool_async(self.db_name, size)

        # Check out the branch on every connection now, so the first timed
        # requests don't pay for it.
        conns = [await self.async_pool.acquire() for _ in range(size)]
        try:
            for conn in conns:
                await self._configure_async_conn(conn)
        finally:
            for conn in conns:
                self.async_pool.release(conn)

    async def _configure_async_conn(self, conn) -> None:
        async with conn.cursor() as cur:
            await cur.execute("CALL DOLT_CHECKOUT(%s);", (self._async_branch,))
        self._configured_conns.add(conn)

    @asynccontextmanager
    async def _pool_connection(self):
        """Borrow a pool connection, checked out on the worker's branch.

        aiomysql has no configure hook, so a connection the pool opened to
        replace a dropped one is configured here on first use.
        """
        async with self.async_pool.acquire() as conn:
            if conn not in self._configured_conns:
                await self._configure_async_conn(conn)
            yield conn

    async def close_connection_async(self) -> None:
        if self.async_pool:
            self.async_pool.close()
            await self.async_pool.wait_closed()
            self.async_pool = None

    # Branch hooks run on one pinned pool connection (see
    # DBToolSuite._acquire_async_conn), so checking out the parent and
    # creating the child happen in the same session.

    async def _create_branch_impl_async(
        self, branch_name: str, parent_id: str = None
    ) -> None:
        """Async version of _create_branch_impl."""
        # Only checkout to parent if specified, otherwise create from current branch
        if parent_id:
            await self.execute_sql_async("CALL DOLT_CHECKOUT(%s);", (parent_id,))
        await self.execute_sql_async("CALL DOLT_CHECKOUT('-b', %s);", (branch_name,))

    async def _connect_branch_impl_async(self, branch_name: str) -> None:
        """Async version of _connect_branch_impl."""
        await self.execute_sql_async("CALL DOLT_CHECKOUT(%s);", (branch_name,))

    async def _get_current_branch_impl_async(self) -> tuple[str, str]:
        """Async version of _get_current_branch_impl."""
        result = await self.execute_sql_async("SELECT active_branch();")
        # Dolt's branch name is unique and can be used as an ID.
        return (result[0][0], result[0][0])

    async def _delete_branch_impl_async(
        self, branch_name: str, branch_id: str
    ) -> None:
        """Async version of _delete_branch_impl."""
        await self.execute_sql_async("CALL DOLT_BRANCH('-D', %s);", (branch_name,))
