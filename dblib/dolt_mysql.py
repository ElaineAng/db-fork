"""Dolt backend over the MySQL wire protocol (dolt sql-server).

Same model as dblib/dolt.py with CALL syntax; multi-ref scripts address
other branches as `<db>/<branch>`.<table>.
"""

import os

import aiomysql
import pymysql
from pymysql.constants import CLIENT

from dblib.db_api import DBToolSuite, Ref
from dblib import mysql_common
import dblib.result_collector as rc
import dblib.util as dbutil

DOLT_MYSQL_USER = os.environ.get("DOLT_MYSQL_USER", "root")
DOLT_MYSQL_PASSWORD = os.environ.get("DOLT_MYSQL_PASSWORD", "")
DOLT_MYSQL_HOST = os.environ.get("DOLT_MYSQL_HOST", "127.0.0.1")
DOLT_MYSQL_PORT = int(os.environ.get("DOLT_MYSQL_PORT", "3306"))
DOLT_MYSQL_DATA_DIR = os.path.expanduser(
    os.environ.get("DOLT_MYSQL_DATA_DIR", "~/dolt/databases")
)


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


def load_sql_dump(db_name: str, sql_path: str) -> None:
    """Load a SQL file (plain SQL or pg_dump output) into db_name."""
    conn = connect(db_name, client_flag=CLIENT.MULTI_STATEMENTS)
    try:
        mysql_common.load_sql_dump(conn, sql_path)
    finally:
        conn.close()


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
    BACKEND_NAME = "dolt_mysql"
    SUPPORTS_COMMIT_REFS = True
    SUPPORTS_MULTI_REF_EXEC = True

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
        default_branch_name: str = "main",
        measure_storage: bool = False,
    ):
        return cls(connect(db_name), collector, db_name, default_branch_name,
                   measure_storage)

    def __init__(
        self,
        connection,
        collector: rc.ResultCollector,
        db_name: str,
        default_branch_name: str = "main",
        measure_storage: bool = False,
    ):
        super().__init__(connection, collector, measure_storage)
        self.db_name = db_name
        self.default_branch = default_branch_name
        self._temp_branches: set = set()
        self._connect_impl(Ref(default_branch_name))
        self._current_ref = Ref(default_branch_name)

    # ------------------------------------------------------------------

    def _checkout(self, branch: str) -> None:
        self._execute("CALL DOLT_CHECKOUT(%s);", (branch,))

    def _temp_branch_name(self, commit: str) -> str:
        return f"_at_{commit[:12]}_t{rc.get_current_thread_id()}"

    def _on(self, ref: Ref) -> None:
        if self._current_ref != Ref(ref.branch):
            self._checkout(ref.branch)
            self._current_ref = Ref(ref.branch)

    @staticmethod
    def _spec(ref: Ref) -> str:
        return ref.commit or ref.branch

    def _rows_as_dicts(self, query: str, vars=None) -> list:
        with self.conn.cursor() as cur:
            cur.execute(query, vars)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def _get_table_columns(self, table_name: str) -> list:
        rows = self._execute(mysql_common.TABLE_COLUMNS_QUERY, (table_name,))
        return mysql_common.normalize_column_types(rows)

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def _storage_bytes(self) -> int:
        if not self.db_name:
            return 0
        return dbutil.get_directory_size_bytes(
            os.path.join(DOLT_MYSQL_DATA_DIR, self.db_name)
        )

    def _connect_impl(self, ref: Ref) -> None:
        if not ref.commit:
            self._checkout(ref.branch)
            return
        tmp = self._temp_branch_name(ref.commit)
        if tmp in self._temp_branches:
            self._checkout(tmp)
            return
        try:
            self._execute("CALL DOLT_CHECKOUT(%s, '-b', %s);", (ref.commit, tmp))
        except Exception:
            self._checkout(tmp)
        self._temp_branches.add(tmp)

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        self._execute("CALL DOLT_BRANCH(%s, %s);", (name, self._spec(from_ref)))

    def _commit_impl(self, ref: Ref, message: str) -> str:
        self._on(ref)
        self._execute("CALL DOLT_ADD('-A');")
        try:
            rows = self._execute("CALL DOLT_COMMIT('-m', %s);", (message,))
            return rows[0][0]
        except Exception as e:
            if "nothing to commit" not in str(e).lower():
                raise
            return self._execute("SELECT HASHOF('HEAD');")[0][0]

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        tables = self._rows_as_dicts(
            "SELECT * FROM DOLT_DIFF_STAT(%s, %s);",
            (self._spec(ref_a), self._spec(ref_b)),
        )
        return {
            "tables": tables,
            "rows_added": sum(t.get("rows_added") or 0 for t in tables),
            "rows_deleted": sum(t.get("rows_deleted") or 0 for t in tables),
            "rows_modified": sum(t.get("rows_modified") or 0 for t in tables),
        }

    def _log_impl(self, ref: Ref, limit: int) -> list:
        return self._rows_as_dicts(
            "SELECT commit_hash, committer, email, date, message "
            "FROM DOLT_LOG(%s) LIMIT %s;",
            (self._spec(ref), int(limit)),
        )

    def _merge_impl(self, into: Ref, source: Ref, message: str) -> dict:
        self._on(into)
        self._execute("CALL DOLT_ADD('-A');")
        try:
            self._execute("CALL DOLT_COMMIT('-m', 'pre-merge commit');")
        except Exception as e:
            if "nothing to commit" not in str(e).lower():
                raise
        # With autocommit on, Dolt rolls back a conflicting merge unless
        # this is set, so the resolution below could never run.
        self._execute("SET @@dolt_allow_commit_conflicts = 1;")
        if message:
            rows = self._execute(
                "CALL DOLT_MERGE(%s, '-m', %s);", (self._spec(source), message)
            )
        else:
            rows = self._execute("CALL DOLT_MERGE(%s);", (self._spec(source),))
        row = rows[0] if rows else ()
        info = {
            "hash": row[0] if len(row) > 0 else "",
            "fast_forward": bool(row[1]) if len(row) > 1 else False,
            "conflicts": int(row[2]) if len(row) > 2 else 0,
            "message": row[3] if len(row) > 3 else "",
        }
        if info["conflicts"] > 0:
            tables = self._execute("SELECT `table` FROM dolt_conflicts;")
            for (table_name,) in tables or []:
                self._execute(
                    "CALL DOLT_CONFLICTS_RESOLVE('--ours', %s);", (table_name,)
                )
            self._execute("CALL DOLT_ADD('-A');")
            self._execute("CALL DOLT_COMMIT('-m', 'resolved merge conflicts');")
            info["resolved"] = "ours"
        return info

    def _rebase_impl(self, ref: Ref, onto: Ref) -> None:
        self._on(ref)
        self._execute("CALL DOLT_REBASE('-i', %s);", (self._spec(onto),))
        self._execute("CALL DOLT_REBASE('--continue');")

    def _revert_impl(self, ref: Ref, commit: str) -> None:
        self._on(ref)
        self._execute("CALL DOLT_REVERT(%s);", (commit,))

    def _reset_impl(self, ref: Ref, to: str) -> None:
        self._on(ref)
        self._execute("CALL DOLT_RESET('--hard', %s);", (to,))

    def _delete_impl(self, ref: Ref) -> None:
        if self._current_ref and self._current_ref.branch == ref.branch:
            self._checkout(self.default_branch)
            self._current_ref = Ref(self.default_branch)
        self._execute("CALL DOLT_BRANCH('-D', %s);", (ref.branch,))

    def _qualified_table(self, ref: Ref, table: str) -> str:
        return f"`{self.db_name}/{self._spec(ref)}`.`{table}`"

    def list_branches(self) -> list:
        rows = self._execute("SELECT name FROM dolt_branches;")
        return [r[0] for r in rows or []]

    def close_connection(self) -> None:
        if self.conn and self._temp_branches:
            try:
                self._checkout(self.default_branch)
                for tmp in list(self._temp_branches):
                    self._execute("CALL DOLT_BRANCH('-D', %s);", (tmp,))
            except Exception as e:
                print(f"Warning: could not drop temporary Dolt branches: {e}")
            self._temp_branches.clear()
        super().close_connection()

    # ------------------------------------------------------------------
    # Async
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
        target = ref.branch
        if ref.commit:
            target = self._temp_branch_name(ref.commit)
            if target not in self._temp_branches:
                async with conn.cursor() as cur:
                    try:
                        await cur.execute(
                            "CALL DOLT_CHECKOUT(%s, '-b', %s);", (ref.commit, target)
                        )
                    except Exception:
                        await cur.execute("CALL DOLT_CHECKOUT(%s);", (target,))
                self._temp_branches.add(target)
                conn._dolt_branch = target
                return conn
        if getattr(conn, "_dolt_branch", None) != target:
            async with conn.cursor() as cur:
                await cur.execute("CALL DOLT_CHECKOUT(%s);", (target,))
            conn._dolt_branch = target
        return conn
