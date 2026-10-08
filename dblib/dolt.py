"""DoltgreSQL backend (Dolt over the Postgres wire protocol).

Branches, commits, diff, log, merge, rebase, revert and reset all map to
Dolt's SQL functions. A commit ref ("branch@hash") cannot be checked out
directly (Dolt has no detached HEAD), so connecting to one creates a
throwaway branch at that commit; those branches are deleted when the
connection closes. Multi-ref scripts address other branches as
"<db>/<branch>".public.<table>.
"""

import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extensions import connection as _pgconn

from dblib.db_api import DBToolSuite, Ref
import dblib.result_collector as rc
import dblib.util as dbutil

DOLT_USER = os.environ.get("DOLT_USER", "postgres")
DOLT_PASSWORD = os.environ.get("DOLT_PASSWORD", "password")
DOLT_HOST = os.environ.get("DOLT_HOST", "localhost")
DOLT_PORT = int(os.environ.get("DOLT_PORT", "5432"))
DOLT_DATA_DIR = os.environ.get("DOLT_DATA_DIR", "~/doltgres/databases")


def commit_dolt_schema(db_uri: str, message: str = "Load SQL schema") -> None:
    """Commit everything in the working set over db_uri (setup helper)."""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(db_uri)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("SELECT dolt_add('-A');")
        cur.execute("SELECT dolt_commit('-m', %s);", (message,))
        print(f"Dolt schema committed: {message}")
    except Exception as e:
        print(f"Warning: Dolt schema commit failed (may be okay): {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def _first(value):
    """First element of a Dolt function result cell, which psycopg2 may
    hand back as a list or as the text form of an array ("{a,b}")."""
    if isinstance(value, (list, tuple)):
        return value[0] if value else None
    if isinstance(value, str) and value.startswith("{"):
        return value.strip("{}").split(",")[0].strip('"')
    return value


def _cells(value) -> list:
    if isinstance(value, (list, tuple)):
        return list(value)
    if isinstance(value, str) and value.startswith("{"):
        return [c.strip('"') for c in value.strip("{}").split(",")]
    return [value]


class DoltToolSuite(DBToolSuite):
    BACKEND_NAME = "dolt"
    SUPPORTS_COMMIT_REFS = True
    SUPPORTS_MULTI_REF_EXEC = True

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return dbutil.format_db_uri(
            DOLT_USER, DOLT_PASSWORD, DOLT_HOST, DOLT_PORT, "postgres"
        )

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return dbutil.format_db_uri(
            DOLT_USER, DOLT_PASSWORD, DOLT_HOST, DOLT_PORT, db_name
        )

    @classmethod
    def init_for_bench(
        cls,
        collector: rc.ResultCollector,
        db_name: str,
        default_branch_name: str = "main",
        measure_storage: bool = False,
    ):
        conn = psycopg2.connect(cls.get_initial_connection_uri(db_name))
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return cls(conn, collector, db_name, default_branch_name, measure_storage)

    def __init__(
        self,
        connection: _pgconn,
        collector: rc.ResultCollector,
        db_name: str,
        default_branch_name: str = "main",
        measure_storage: bool = False,
    ):
        super().__init__(connection, collector, measure_storage)
        self.db_name = db_name
        self.default_branch = default_branch_name
        # Throwaway branches created to read at a commit hash.
        self._temp_branches: set = set()
        self._connect_impl(Ref(default_branch_name))
        self._current_ref = Ref(default_branch_name)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _checkout(self, branch: str) -> None:
        self._execute("SELECT dolt_checkout(%s);", (branch,))

    def _temp_branch_name(self, commit: str) -> str:
        return f"_at_{commit[:12]}_t{rc.get_current_thread_id()}"

    def _on(self, ref: Ref) -> None:
        """Make sure the sync connection is checked out on ref's branch.
        Verbs that act on a checked-out branch (commit, merge, ...) call
        this; the switch counts towards the verb's latency."""
        if self._current_ref != Ref(ref.branch):
            self._checkout(ref.branch)
            self._current_ref = Ref(ref.branch)

    @staticmethod
    def _spec(ref: Ref) -> str:
        """What to pass to a Dolt function for this ref."""
        return ref.commit or ref.branch

    def _rows_as_dicts(self, query: str, vars=None) -> list:
        with self.conn.cursor() as cur:
            cur.execute(query, vars)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def _storage_bytes(self) -> int:
        """Physical size of the database's directory. Dolt's prolly trees
        share structure across branches, so this is the real footprint."""
        if not self.db_name:
            return 0
        return dbutil.get_directory_size_bytes(
            os.path.join(os.path.expanduser(DOLT_DATA_DIR), self.db_name)
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
            self._execute("SELECT dolt_checkout(%s, '-b', %s);", (ref.commit, tmp))
        except Exception:
            # Another connection may have created it already.
            self._checkout(tmp)
        self._temp_branches.add(tmp)

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        self._execute("SELECT dolt_branch(%s, %s);", (name, self._spec(from_ref)))

    def _commit_impl(self, ref: Ref, message: str) -> str:
        self._on(ref)
        self._execute("SELECT dolt_add('-A');")
        try:
            rows = self._execute("SELECT dolt_commit('-m', %s);", (message,))
            return _first(rows[0][0])
        except Exception as e:
            if "nothing to commit" not in str(e).lower():
                raise
            rows = self._execute("SELECT dolt_hashof('HEAD');")
            return rows[0][0]

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        tables = self._rows_as_dicts(
            "SELECT * FROM dolt_diff_stat(%s, %s);",
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
            "FROM dolt_log(%s) LIMIT %s;",
            (self._spec(ref), int(limit)),
        )

    def _merge_impl(self, into: Ref, source: Ref, message: str) -> dict:
        self._on(into)
        # dolt_merge needs a clean working set.
        self._execute("SELECT dolt_add('-A');")
        try:
            self._execute("SELECT dolt_commit('-m', 'pre-merge commit');")
        except Exception as e:
            if "nothing to commit" not in str(e).lower():
                raise
        if message:
            rows = self._execute(
                "SELECT dolt_merge(%s, '-m', %s);", (self._spec(source), message)
            )
        else:
            rows = self._execute("SELECT dolt_merge(%s);", (self._spec(source),))
        cells = _cells(rows[0][0]) if rows else []
        info = {
            "hash": cells[0] if len(cells) > 0 else "",
            "fast_forward": bool(int(cells[1])) if len(cells) > 1 else False,
            "conflicts": int(cells[2]) if len(cells) > 2 else 0,
            "message": cells[3] if len(cells) > 3 else "",
        }
        if info["conflicts"] > 0:
            # Resolve in favour of the target branch.
            tables = self._execute("SELECT table_name FROM dolt_conflicts;")
            for (table_name,) in tables or []:
                self._execute(
                    "SELECT dolt_conflicts_resolve('--ours', %s);", (table_name,)
                )
            self._execute("SELECT dolt_add('-A');")
            self._execute("SELECT dolt_commit('-m', 'resolved merge conflicts');")
            info["resolved"] = "ours"
        return info

    def _rebase_impl(self, ref: Ref, onto: Ref) -> None:
        self._on(ref)
        self._execute("SELECT dolt_rebase('-i', %s);", (self._spec(onto),))
        self._execute("SELECT dolt_rebase('--continue');")

    def _revert_impl(self, ref: Ref, commit: str) -> None:
        self._on(ref)
        self._execute("SELECT dolt_revert(%s);", (commit,))

    def _reset_impl(self, ref: Ref, to: str) -> None:
        self._on(ref)
        self._execute("SELECT dolt_reset('--hard', %s);", (to,))

    def _delete_impl(self, ref: Ref) -> None:
        if self._current_ref and self._current_ref.branch == ref.branch:
            # Dolt refuses to delete the checked-out branch.
            self._checkout(self.default_branch)
            self._current_ref = Ref(self.default_branch)
        self._execute("SELECT dolt_branch('-D', %s);", (ref.branch,))

    def _qualified_table(self, ref: Ref, table: str) -> str:
        return f'"{self.db_name}/{self._spec(ref)}".public.{table}'

    def list_branches(self) -> list:
        rows = self._execute("SELECT name FROM dolt_branches;")
        return [r[0] for r in rows or []]

    def close_connection(self) -> None:
        if self.conn and self._temp_branches:
            try:
                self._checkout(self.default_branch)
                for tmp in list(self._temp_branches):
                    self._execute("SELECT dolt_branch('-D', %s);", (tmp,))
            except Exception as e:
                print(f"Warning: could not drop temporary Dolt branches: {e}")
            self._temp_branches.clear()
        super().close_connection()

    # ------------------------------------------------------------------
    # Async (exec_async): a psycopg pool on the database; each pool
    # connection remembers the branch it has checked out.
    # ------------------------------------------------------------------

    async def open_async_pool(self, size: int) -> None:
        import psycopg
        from psycopg_pool import AsyncConnectionPool

        # Client-side parameter binding (like psycopg2): Doltgres rejects
        # the binary-format numerics psycopg3 sends server-side.
        self.async_pool = AsyncConnectionPool(
            self.get_initial_connection_uri(self.db_name),
            min_size=size,
            max_size=size,
            kwargs={"autocommit": True, "cursor_factory": psycopg.AsyncClientCursor},
            open=False,
        )
        await self.async_pool.open(wait=True)

    async def _connect_impl_async(self, conn, ref: Ref):
        target = ref.branch
        if ref.commit:
            target = self._temp_branch_name(ref.commit)
            if target not in self._temp_branches:
                try:
                    await conn.execute(
                        "SELECT dolt_checkout(%s, '-b', %s);", (ref.commit, target)
                    )
                except Exception:
                    await conn.execute("SELECT dolt_checkout(%s);", (target,))
                self._temp_branches.add(target)
                conn._dolt_branch = target
                return conn
        if getattr(conn, "_dolt_branch", None) != target:
            await conn.execute("SELECT dolt_checkout(%s);", (target,))
            conn._dolt_branch = target
        return conn
