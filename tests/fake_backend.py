"""An in-memory sqlite backend for testing DBToolSuite without a server.

Each branch is its own sqlite database. Commits are snapshots, so the
fake supports commit, log, diff (row-count based), reset and delete, and
leaves merge, rebase and revert unsupported to exercise that path.
"""

import hashlib
import sqlite3
import time
from contextlib import asynccontextmanager, contextmanager

from dblib.db_api import DBToolSuite, Ref


class _Cursor:
    """sqlite3 cursor proxy: context manager, and execute(sql, None) works."""

    def __init__(self, cur):
        self._cur = cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self._cur.close()

    def execute(self, q, vars=None):
        # Scripts use DB-API "%s" placeholders (psycopg2/pymysql); sqlite
        # wants "?".
        if vars is not None and "%s" in q:
            q = q.replace("%s", "?")
        self._cur.execute(q, vars if vars is not None else ())

    def fetchall(self):
        return self._cur.fetchall()

    def fetchone(self):
        return self._cur.fetchone()

    @property
    def description(self):
        return self._cur.description

    def close(self):
        self._cur.close()


class Conn:
    """sqlite3 connection whose cursor() works as a context manager."""

    def __init__(self, raw=None):
        self.raw = raw or sqlite3.connect(":memory:", check_same_thread=False)
        self.raw.isolation_level = None  # autocommit
        self.closed = False

    def cursor(self):
        return _Cursor(self.raw.cursor())

    def close(self):
        self.closed = True

    def copy(self) -> "Conn":
        dst = Conn()
        self.raw.backup(dst.raw)
        return dst


class _AsyncCursor:
    def __init__(self, cur):
        self._cur = cur
        self.description = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        self._cur.close()

    async def execute(self, q, vars=None):
        if vars is not None and "%s" in q:
            q = q.replace("%s", "?")
        self._cur.execute(q, vars if vars is not None else ())
        self.description = self._cur.description

    async def fetchall(self):
        return self._cur.fetchall()


class AsyncConn:
    def __init__(self, conn: Conn):
        self.conn = conn
        self.closed = False

    def cursor(self):
        return _AsyncCursor(self.conn.raw.cursor())

    async def close(self):
        self.closed = True


class FakePool:
    def __init__(self, suite):
        self.suite = suite
        self.closed = False
        self.borrowed = 0

    @asynccontextmanager
    async def connection(self):
        self.borrowed += 1
        yield AsyncConn(self.suite.branches[self.suite.pool_branch])

    async def close(self):
        self.closed = True


class FakeSuite(DBToolSuite):
    BACKEND_NAME = "fake"
    SUPPORTS_COMMIT_REFS = True
    SUPPORTS_MULTI_REF_EXEC = False

    def __init__(self, result_collector=None, measure_storage=False):
        self.branches = {"main": Conn()}
        self.commits = {}  # hash -> (branch, Conn snapshot, message, ts)
        self.history = {"main": []}  # branch -> [hash, ...] newest last
        self.pool_branch = "main"
        super().__init__(self.branches["main"], result_collector,
                         measure_storage)
        self._connect_impl(Ref("main"))
        self._current_ref = Ref("main")

    # --- hooks ---------------------------------------------------------

    def _storage_bytes(self) -> int:
        total = 0
        for c in self.branches.values():
            cur = c.raw.cursor()
            cur.execute("PRAGMA page_count")
            pages = cur.fetchone()[0]
            cur.execute("PRAGMA page_size")
            total += pages * cur.fetchone()[0]
        return total

    def _connect_impl(self, ref: Ref) -> None:
        if ref.commit:
            if ref.commit not in self.commits:
                raise ValueError(f"unknown commit {ref.commit}")
            self.conn = self.commits[ref.commit][1].copy()
            return
        if ref.branch not in self.branches:
            raise ValueError(f"unknown branch {ref.branch}")
        self.conn = self.branches[ref.branch]

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        if name in self.branches:
            raise ValueError(f"branch {name} exists")
        src = (self.commits[from_ref.commit][1] if from_ref.commit
               else self.branches[from_ref.branch])
        self.branches[name] = src.copy()
        self.history[name] = list(self.history.get(from_ref.branch, []))

    def _commit_impl(self, ref: Ref, message: str) -> str:
        snap = self.branches[ref.branch].copy()
        h = hashlib.sha1(f"{ref.branch}{message}{time.time()}".encode()).hexdigest()[:12]
        self.commits[h] = (ref.branch, snap, message, time.time())
        self.history[ref.branch].append(h)
        return h

    def _log_impl(self, ref: Ref, limit: int) -> list:
        hashes = list(reversed(self.history[ref.branch]))[:limit]
        return [{"hash": h, "message": self.commits[h][2]} for h in hashes]

    def _diff_impl(self, a: Ref, b: Ref):
        def count(ref):
            c = (self.commits[ref.commit][1] if ref.commit
                 else self.branches[ref.branch])
            cur = c.raw.cursor()
            cur.execute("SELECT count(*) FROM t")
            return cur.fetchone()[0]
        return {"rows_a": count(a), "rows_b": count(b)}

    def _reset_impl(self, ref: Ref, to: str) -> None:
        self.branches[ref.branch] = self.commits[to][1].copy()
        if self._current_ref and self._current_ref.branch == ref.branch:
            self.conn = self.branches[ref.branch]

    def _delete_impl(self, ref: Ref) -> None:
        if ref.branch == "main":
            raise ValueError("cannot delete the default branch")
        if self._current_ref and self._current_ref.branch == ref.branch:
            # Like Dolt: move the connection off the branch first.
            self._connect_impl(Ref("main"))
            self._current_ref = Ref("main")
        del self.branches[ref.branch]

    def list_branches(self):
        return list(self.branches)

    # --- async ---------------------------------------------------------

    async def open_async_pool(self, size: int) -> None:
        self.async_pool = FakePool(self)

    async def _connect_impl_async(self, conn, ref: Ref):
        if ref.branch == self.pool_branch and not ref.commit:
            return conn
        target = (self.commits[ref.commit][1].copy() if ref.commit
                  else self.branches[ref.branch])
        return AsyncConn(target)
