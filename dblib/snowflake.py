"""Snowflake backend.

Snowflake has no branches or commits. It has zero-copy CLONE of a database
(optionally AT a point in its Time Travel history), ALTER DATABASE ... SWAP
WITH, and queries across databases in one session. Each of our branches is
therefore its own transient database:

    main branch      -> <DB_NAME>
    branch "x"       -> <DB_NAME>__X

Names are upper-cased: Snowflake stores unquoted identifiers in upper case,
so the unquoted, lower-case names in workload SQL resolve to them.

A commit is a row in the branch database's _BB_COMMITS table. Its snapshot
is the Time Travel point of the INSERT that wrote the row
(AT(STATEMENT => query id)), so the snapshot includes the row itself and a
clone taken there carries the log as it was. A statement's query id is only
known once it has run, so the snapshots are kept outside the branch
databases, in <DB_NAME>__BBMETA.SNAPSHOTS: commit id -> (database whose
history holds it, query id or timestamp). A clone has no history from
before it was created, so a commit inherited from the parent is read from
the parent's database.

    branch     CREATE TRANSIENT DATABASE <db> CLONE <parent> [AT(snapshot)]
    commit     INSERT INTO _BB_COMMITS (plus the SNAPSHOTS row)
    log        SELECT FROM _BB_COMMITS
    diff       joins on the primary key between the two states
    reset      clone the commit's snapshot, SWAP it in for the branch
    revert     apply the inverse of the commit's (before, after) snapshots
    merge      SQL three-way merge: base = the newest log row both sides
               have, or the fork point; ours = the target database,
               theirs = the source database
    rebase     clone the upstream, three-way merge the branch's changes into
               the clone, SWAP it in for the branch
    delete     DROP DATABASE <branch db>

reset and rebase replace the branch's database with SWAP WITH. The old
database, renamed <DB_NAME>__ARCH_<n>, keeps the history earlier snapshots
point at (their SNAPSHOTS rows are repointed to it). Writes another session
makes to the branch during the swap are lost, so the branch must be
quiesced (the scenarios do, around spine resets).

The three-way merge matches rows by primary key. Snowflake does not enforce
primary keys but records them, which is all the merge needs; tables without
one (history) only receive the other side's new rows. Conflicts are rows
both sides changed differently, resolved per ``on_conflict`` ("ours" |
"theirs" | callable, see dblib.db_api). In a rebase, as in Dolt and git,
"ours" is the upstream and "theirs" the branch being replayed. A table
added on the source is cloned into the target, added columns are added,
and a differing primary key is a schema conflict that fails the verb
before any data changes.

Time Travel reads use a table's current columns (a column added later
reads as NULL), so merge and diff compare the columns both sides have.
Clones taken at a past point have that point's tables and columns.

Every session turns the result cache off (USE_CACHED_RESULT) so repeated
reads run instead of being served from the cache.
"""

import contextlib
import os
import re
import time
import uuid

import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.errors import ProgrammingError

from dblib.db_api import DBToolSuite, Ref
import dblib.result_collector as rc
from util.sql_parse import get_sql_operation_keyword

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER", "")
SNOWFLAKE_PRIVATE_KEY_PATH = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "")
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
# Instead of a key pair: a password or a programmatic access token.
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_ROLE = os.environ.get("SNOWFLAKE_ROLE", "")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "")
# Time Travel retention of the run's databases; snapshots older than this
# can no longer be read. Transient databases allow 0 or 1 day.
SNOWFLAKE_RETENTION_DAYS = int(os.environ.get("SNOWFLAKE_RETENTION_DAYS", "1"))

_SESSION_PARAMETERS = {
    # Run repeated reads instead of serving them from the result cache.
    "USE_CACHED_RESULT": False,
    "TIMEZONE": "UTC",
    "QUERY_TAG": "branchbench",
    # Seconds a statement waits for a table lock (the default is 12 hours).
    "LOCK_TIMEOUT": 120,
}

# Branch name that maps to the root database itself.
MAIN_BRANCH = "main"

# Separates the root database name from the rest of a run database's name.
_BRANCH_SEP = "__"
_SCHEMA = "PUBLIC"

# Run databases (after the separator) that are not branches: temporaries
# (commit refs, reset and rebase clones), archives of swapped-out branch
# databases, and the metadata database.
_TEMP_PREFIX = "TMP_"
_ARCHIVE_PREFIX = "ARCH_"
_META_SUFFIX = "BBMETA"

_IDENT_RE = re.compile(r"^[A-Z0-9_]+$")
_MAX_IDENT_LEN = 255

# Per-database bookkeeping tables; never merged, diffed or restored.
META_PREFIX = "_BB_"
COMMITS_TABLE = "_BB_COMMITS"
KEYS_TABLE = "_BB_KEYS"
# In the metadata database: commit id -> snapshot.
SNAPSHOTS_TABLE = "SNAPSHOTS"

_COMMITS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    seq NUMBER(38, 0) NOT NULL,
    id VARCHAR NOT NULL,
    kind VARCHAR NOT NULL,
    message VARCHAR,
    parent VARCHAR,
    prev_id VARCHAR,
    ts TIMESTAMP_LTZ
)
"""

# Scratch rows for the three-way merge: (tag, table, key string, side).
_KEYS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    tag VARCHAR NOT NULL,
    tbl VARCHAR NOT NULL,
    k VARCHAR NOT NULL,
    side VARCHAR(1) NOT NULL
)
"""

_SNAPSHOTS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    id VARCHAR NOT NULL,
    db VARCHAR NOT NULL,
    qid VARCHAR,
    snap_ts VARCHAR,
    seq NUMBER(38, 0) NOT NULL
)
"""

_LOG_COLS = ("seq", "id", "kind", "message", "parent", "prev_id", "ts")


def _log_cols(alias: str) -> str:
    # Unquoted: the bookkeeping tables are created with unquoted names.
    return ", ".join(f"{alias}.{c}" for c in _LOG_COLS)

# Format of the timestamps a snapshot can be taken at.
_TS_FORMAT = "YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM"

# Conflict rows handed to a resolve callable, per table.
_CONFLICT_ROW_LIMIT = 1000

# Merge-base candidates tried, newest first, for one whose history is
# still readable.
_BASE_CANDIDATES = 20

# Time Travel data unavailable (before the object existed, or past the
# retention period), and object does not exist.
_ER_NO_TIME_TRAVEL = 707
_ER_NO_OBJECT = 2003

_DML = ("INSERT", "UPDATE", "DELETE", "MERGE")


class _Cursor:
    """Snowflake cursor that reports DML like the other drivers. Snowflake
    answers INSERT/UPDATE/DELETE/MERGE with a row of counts, which
    Session.sql would take for a result set; hiding it makes callers use
    rowcount (the rows affected) instead."""

    def __init__(self, cur):
        self._cur = cur
        self._dml = False

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self._cur.close()

    def execute(self, query, vars=None):
        self._dml = get_sql_operation_keyword(query) in _DML
        self._cur.execute(query, vars)
        return self

    @property
    def description(self):
        return None if self._dml else self._cur.description

    @property
    def rowcount(self):
        return self._cur.rowcount

    @property
    def sfqid(self):
        return self._cur.sfqid

    def fetchall(self):
        return self._cur.fetchall()

    def fetchone(self):
        return self._cur.fetchone()

    def close(self):
        self._cur.close()


class _Connection:
    """Snowflake connection whose cursors are _Cursor."""

    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return _Cursor(self._conn.cursor())

    def __getattr__(self, name):
        return getattr(self._conn, name)


def connect(db_name: str = None) -> _Connection:
    """Open a connection with the benchmark's session parameters."""
    if not (SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER):
        raise RuntimeError("SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER must be set "
                           "(see the Snowflake backend section of the README)")
    kwargs = dict(account=SNOWFLAKE_ACCOUNT, user=SNOWFLAKE_USER,
                  session_parameters=dict(_SESSION_PARAMETERS))
    if SNOWFLAKE_PRIVATE_KEY_PATH:
        kwargs["private_key_file"] = os.path.expanduser(SNOWFLAKE_PRIVATE_KEY_PATH)
        if SNOWFLAKE_PRIVATE_KEY_PASSPHRASE:
            kwargs["private_key_file_pwd"] = SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
    elif SNOWFLAKE_PASSWORD:
        kwargs["password"] = SNOWFLAKE_PASSWORD
    if SNOWFLAKE_ROLE:
        kwargs["role"] = SNOWFLAKE_ROLE
    if SNOWFLAKE_WAREHOUSE:
        kwargs["warehouse"] = SNOWFLAKE_WAREHOUSE
    if db_name:
        kwargs["database"] = _ident(db_name)
        kwargs["schema"] = _SCHEMA
    return _Connection(snowflake.connector.connect(**kwargs))


def _ident(name: str) -> str:
    """Snowflake name (upper case) for a database, table or column."""
    out = name.upper()
    if not _IDENT_RE.match(out) or len(out) > _MAX_IDENT_LEN:
        raise ValueError(f"'{name}' cannot be used as a Snowflake name "
                         "(letters, digits and _ only)")
    return out


def _quote(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _qt(db: str, table: str) -> str:
    return f"{_quote(db)}.{_SCHEMA}.{_quote(table)}"


def branch_db_name(db_name: str, branch_name: str) -> str:
    """Database that holds branch_name of the root database db_name."""
    root = _ident(db_name)
    if branch_name == MAIN_BRANCH:
        return root
    return _ident(f"{root}{_BRANCH_SEP}{branch_name}")


def _meta_db(db_name: str) -> str:
    return f"{_ident(db_name)}{_BRANCH_SEP}{_META_SUFFIX}"


def _run_databases(cur, db_name: str) -> list:
    """Every database of the run except the root: branches, temporaries,
    archives and the metadata database."""
    prefix = _ident(db_name) + _BRANCH_SEP
    # "_" is a LIKE wildcard, so the pattern may match more; filter exactly.
    cur.execute(f"SHOW DATABASES LIKE '{prefix}%'")
    return [r[1] for r in cur.fetchall() if r[1].startswith(prefix)]


def setup_database(db_name: str, sql_path: str = None) -> None:
    """(Re)create db_name, with no branches, and its metadata database."""
    if sql_path:
        raise NotImplementedError(
            "The Snowflake backend does not load SQL dumps yet; "
            "use database_setup { generated {} }")
    drop_database(db_name)
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TRANSIENT DATABASE {_quote(_ident(db_name))} "
                        f"DATA_RETENTION_TIME_IN_DAYS = {SNOWFLAKE_RETENTION_DAYS}")
            cur.execute(f"CREATE TRANSIENT DATABASE IF NOT EXISTS {_quote(_meta_db(db_name))} "
                        "DATA_RETENTION_TIME_IN_DAYS = 0")
            cur.execute(_SNAPSHOTS_DDL.format(table=_qt(_meta_db(db_name), SNAPSHOTS_TABLE)))
        print("Database created successfully.")
    finally:
        conn.close()


def drop_database(db_name: str) -> None:
    """Drop db_name and every database of the run."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            for db in _run_databases(cur, db_name):
                cur.execute(f"DROP DATABASE IF EXISTS {_quote(db)}")
            cur.execute(f"DROP DATABASE IF EXISTS {_quote(_ident(db_name))}")
        print(f"Database '{db_name}' deleted successfully.")
    finally:
        conn.close()


def _type_sql(dtype: str, char_len, precision, scale) -> str:
    """Column type for ALTER TABLE ADD COLUMN, from INFORMATION_SCHEMA."""
    if dtype == "NUMBER" and precision is not None:
        return f"NUMBER({precision}, {scale or 0})"
    if dtype == "TEXT":
        return f"VARCHAR({char_len})" if char_len else "VARCHAR"
    return dtype


class _State:
    """A database at a Time Travel point (a statement or a timestamp), or
    its head when neither is set."""

    __slots__ = ("db", "qid", "ts")

    def __init__(self, db: str, qid: str = None, ts: str = None):
        self.db = db
        self.qid = qid
        self.ts = ts

    @property
    def is_snapshot(self) -> bool:
        return bool(self.qid or self.ts)

    def at(self) -> str:
        if self.qid:
            return f" AT(STATEMENT => '{self.qid}')"
        if self.ts:
            return f" AT(TIMESTAMP => TO_TIMESTAMP_TZ('{self.ts}', '{_TS_FORMAT}'))"
        return ""

    def src(self, table: str) -> str:
        """Table expression, with the Time Travel clause for a snapshot."""
        return _qt(self.db, table) + self.at()


# SQL fragments. Aliases name a row source; None means the bare table.

def _col(alias, c: str) -> str:
    return f"{alias}.{_quote(c)}" if alias else _quote(c)


def _cols(alias, cols: list) -> str:
    return ", ".join(_col(alias, c) for c in cols)


def _same(a: str, b: str, cols: list) -> str:
    if not cols:
        return "TRUE"
    return " AND ".join(f"EQUAL_NULL({_col(a, c)}, {_col(b, c)})" for c in cols)


def _join(a: str, b: str, pk: list) -> str:
    return " AND ".join(f"{_col(a, c)} = {_col(b, c)}" for c in pk)


def _key(alias, pk: list) -> str:
    parts = ", ".join(f"TO_VARCHAR({_col(alias, c)})" for c in pk)
    return f"CONCAT_WS('|', {parts})"


def _merge_rows(dst: str, cols: list, pk: list, select: str) -> str:
    """MERGE the rows ``select`` returns into dst, matched by primary key."""
    rest = [c for c in cols if c not in pk]
    sql = f"MERGE INTO {dst} d USING ({select}) s ON {_join('d', 's', pk)} "
    if rest:
        sets = ", ".join(f"d.{_quote(c)} = s.{_quote(c)}" for c in rest)
        sql += f"WHEN MATCHED THEN UPDATE SET {sets} "
    return (sql + f"WHEN NOT MATCHED THEN INSERT ({_cols(None, cols)}) "
            f"VALUES ({_cols('s', cols)})")


class SnowflakeToolSuite(DBToolSuite):
    BACKEND_NAME = "snowflake"
    SUPPORTS_COMMIT_REFS = True
    SUPPORTS_MULTI_REF_EXEC = True
    # For the report: CLONE, DROP and cross-database queries are native;
    # commits, reset and rebase drive clones, Time Travel and SWAP from
    # here; the rest is SQL over Time Travel reads.
    IMPLEMENTATION = {
        "branch": "native",
        "commit": "composed",
        "diff": "simulated",
        "log": "simulated",
        "merge": "simulated",
        "rebase": "composed",
        "revert": "simulated",
        "reset": "composed",
        "delete": "native",
        "commit_refs": "composed",
        "multi_ref_exec": "native",
    }
    IMPLEMENTATION_NOTES = {
        "branch": "CREATE DATABASE ... CLONE (AT the snapshot for a commit ref)",
        "commit": "_bb_commits row; the snapshot is that INSERT's Time Travel point",
        "diff": "primary-key joins between Time Travel reads",
        "log": "SELECT from the _bb_commits bookkeeping table",
        "merge": "SQL three-way merge against the newest common log row or fork point",
        "rebase": "clone of the upstream + SQL three-way merge of the branch, SWAP WITH",
        "revert": "inverse of the commit's (before, after) Time Travel reads applied with SQL",
        "reset": "CLONE AT the snapshot, ALTER DATABASE ... SWAP WITH",
        "delete": ("DROP DATABASE; renamed to an archive instead while another "
                   "branch's log has its commits"),
        "commit_refs": "Time Travel reads AT(STATEMENT => query id)",
        "multi_ref_exec": "cross-database queries",
    }

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return cls.get_initial_connection_uri("")

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return f"snowflake://{SNOWFLAKE_USER}@{SNOWFLAKE_ACCOUNT}/{_ident(db_name) if db_name else ''}"

    @classmethod
    def init_for_bench(
        cls,
        collector: rc.ResultCollector,
        db_name: str,
        default_branch_name: str = MAIN_BRANCH,
        measure_storage: bool = False,
    ):
        return cls(connect(), collector, db_name, default_branch_name, measure_storage)

    def __init__(
        self,
        connection,
        collector: rc.ResultCollector,
        db_name: str,
        default_branch_name: str = MAIN_BRANCH,
        measure_storage: bool = False,
    ):
        super().__init__(connection, collector, measure_storage)
        # Root database; every run database name is derived from it.
        self.db_name = _ident(db_name)
        self.meta_db = _meta_db(db_name)
        self.default_branch = default_branch_name
        self._current_db = None
        # Commit-ref temporaries this suite created: Ref string -> database.
        self._temp_dbs: dict = {}
        # Commit refs resolved to their snapshot (immutable).
        self._ref_states: dict = {}
        # Table metadata per database, cleared at the start of every verb
        # (workload DDL may have changed it).
        self._cols_cache: dict = {}
        self._pk_cache: dict = {}
        self._ensure_meta(self._db_for(default_branch_name))
        self._connect_impl(Ref(default_branch_name))
        self._current_ref = Ref(default_branch_name)

    # ------------------------------------------------------------------
    # Names and the session's database
    # ------------------------------------------------------------------

    def _db_for(self, branch: str) -> str:
        return branch_db_name(self.db_name, branch)

    def _run_db(self, suffix: str) -> str:
        return _ident(f"{self.db_name}{_BRANCH_SEP}{suffix}")

    def _new_temp(self, kind: str) -> str:
        return self._run_db(f"{_TEMP_PREFIX}{kind}_{uuid.uuid4().hex[:12]}")

    def _use(self, db: str) -> None:
        self._execute(f"USE DATABASE {_quote(db)}")
        self._current_db = db

    def _on(self, ref: Ref) -> str:
        """Point the connection at the branch's database; returns it."""
        db = self._db_for(ref.branch)
        if self._current_db != db:
            self._use(db)
        self._current_ref = Ref(ref.branch)
        return db

    def _execute_qid(self, query: str, vars=None) -> str:
        """Run a statement; returns its query id."""
        with self.conn.cursor() as cur:
            cur.execute(query, vars)
            return cur.sfqid

    def _execute_count(self, query: str, vars=None) -> int:
        """Run a DML statement; returns the rows it affected."""
        with self.conn.cursor() as cur:
            cur.execute(query, vars)
            return max(int(cur.rowcount or 0), 0)

    def _create_database(self, sql: str) -> None:
        """Run a CREATE DATABASE. Snowflake makes the new database the
        session's current one, so switch back."""
        self._execute(sql)
        if self._current_db:
            self._execute(f"USE DATABASE {_quote(self._current_db)}")

    def _clone(self, db: str, state: _State) -> None:
        self._create_database(
            f"CREATE TRANSIENT DATABASE {_quote(db)} CLONE {_quote(state.db)}{state.at()}")

    def _drop_quietly(self, db: str) -> None:
        try:
            self._execute(f"DROP DATABASE IF EXISTS {_quote(db)}")
        except Exception:
            pass

    def _now(self) -> str:
        return self._execute(f"SELECT TO_VARCHAR(CURRENT_TIMESTAMP(), '{_TS_FORMAT}')")[0][0]

    def list_branches(self) -> list:
        with self.conn.cursor() as cur:
            dbs = _run_databases(cur, self.db_name)
        prefix = self.db_name + _BRANCH_SEP
        names = [db[len(prefix):] for db in dbs]
        return [MAIN_BRANCH] + [
            n.lower() for n in names
            if not n.startswith((_TEMP_PREFIX, _ARCHIVE_PREFIX)) and n != _META_SUFFIX
        ]

    # ------------------------------------------------------------------
    # Commit log and snapshots
    # ------------------------------------------------------------------

    def _log(self, db: str) -> str:
        return _qt(db, COMMITS_TABLE)

    def _snapshots(self) -> str:
        return _qt(self.meta_db, SNAPSHOTS_TABLE)

    def _ensure_meta(self, db: str) -> None:
        self._create_database(f"CREATE TRANSIENT DATABASE IF NOT EXISTS {_quote(self.meta_db)} "
                              "DATA_RETENTION_TIME_IN_DAYS = 0")
        self._execute(_SNAPSHOTS_DDL.format(table=self._snapshots()))
        self._execute(_COMMITS_DDL.format(table=self._log(db)))
        self._execute(_KEYS_DDL.format(table=_qt(db, KEYS_TABLE)))
        if not self._execute(f"SELECT COUNT(*) FROM {self._log(db)}")[0][0]:
            self._add_commit(db, "root", "initial state")

    def _add_commit(self, db: str, kind: str, message: str, parent: str = None,
                    snapshot: _State = None) -> str:
        """Append a row to db's log and record its snapshot: by default the
        INSERT itself (so the snapshot includes the row), else ``snapshot``.
        ``parent`` is the database a fork/rebase/merge row descends from;
        prev_id is the head before the row. Returns the commit id."""
        seq = time.time_ns()
        commit_id = f"{seq:x}"
        log = self._log(db)
        qid = self._execute_qid(
            f"INSERT INTO {log} ({', '.join(_LOG_COLS)}) "
            f"SELECT %s, %s, %s, %s, %s, "
            f"(SELECT id FROM {log} ORDER BY seq DESC LIMIT 1), CURRENT_TIMESTAMP()",
            (seq, commit_id, kind, message or "", parent),
        )
        snap = snapshot or _State(db, qid)
        self._execute(
            f"INSERT INTO {self._snapshots()} (id, db, qid, snap_ts, seq) "
            "VALUES (%s, %s, %s, %s, %s)",
            (commit_id, snap.db, snap.qid, snap.ts, seq),
        )
        return commit_id

    def _log_rows(self, db: str, limit: int = None, max_seq: int = None) -> list:
        sql = f"SELECT {', '.join(_LOG_COLS)} FROM {self._log(db)}"
        args = None
        if max_seq is not None:
            sql += " WHERE seq <= %s"
            args = (int(max_seq),)
        sql += " ORDER BY seq DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return [dict(zip(_LOG_COLS, r)) for r in self._execute(sql, args) or []]

    def _find_commit(self, db: str, commit_id: str) -> dict:
        """The log row of commit_id on db, with its snapshot as ``state``."""
        rows = self._execute(
            f"SELECT {_log_cols('l')}, s.db, s.qid, s.snap_ts "
            f"FROM {self._log(db)} l LEFT JOIN {self._snapshots()} s ON s.id = l.id "
            "WHERE l.id = %s ORDER BY l.seq DESC LIMIT 1",
            (str(commit_id),),
        )
        if not rows:
            raise ValueError(f"unknown commit '{commit_id}' on {db}")
        r = rows[0]
        row = dict(zip(_LOG_COLS, r[:len(_LOG_COLS)]))
        snap_db, qid, ts = r[len(_LOG_COLS):]
        if snap_db is None:
            raise ValueError(f"commit '{commit_id}' has no recorded snapshot")
        row["state"] = _State(snap_db, qid, ts)
        return row

    def _snapshot_of(self, commit_id: str):
        rows = self._execute(
            f"SELECT db, qid, snap_ts FROM {self._snapshots()} WHERE id = %s "
            "ORDER BY seq DESC LIMIT 1", (str(commit_id),))
        return _State(*rows[0]) if rows else None

    def _state(self, ref: Ref) -> _State:
        db = self._db_for(ref.branch)
        if not ref.commit:
            return _State(db)
        key = str(ref)
        if key not in self._ref_states:
            self._ref_states[key] = self._find_commit(db, ref.commit)["state"]
        return self._ref_states[key]

    def _swap_in(self, db: str, tmp: str) -> str:
        """Make tmp the database db. The old database is kept, renamed to
        an archive, since it holds the history of db's earlier snapshots;
        those snapshots are repointed to it. Returns the archive's name."""
        self._execute(f"ALTER DATABASE {_quote(db)} SWAP WITH {_quote(tmp)}")
        seq = time.time_ns()
        archive = self._run_db(f"{_ARCHIVE_PREFIX}{seq:x}")
        self._execute(f"ALTER DATABASE {_quote(tmp)} RENAME TO {_quote(archive)}")
        self._execute(
            f"UPDATE {self._snapshots()} SET db = %s WHERE db = %s AND seq < %s",
            (archive, db, seq))
        self._forget(db, tmp)
        self._ref_states.clear()
        return archive

    # ------------------------------------------------------------------
    # Table metadata
    # ------------------------------------------------------------------

    def _fresh(self) -> None:
        self._cols_cache.clear()
        self._pk_cache.clear()

    def _forget(self, *dbs) -> None:
        for db in dbs:
            self._cols_cache.pop(db, None)
            self._pk_cache.pop(db, None)

    def _table_meta(self, db: str) -> dict:
        """{table: [(column, type, default)]} of db's data tables."""
        if db not in self._cols_cache:
            rows = self._execute(
                "SELECT c.table_name, c.column_name, c.data_type, "
                "c.character_maximum_length, c.numeric_precision, c.numeric_scale, "
                "c.column_default "
                f"FROM {_quote(db)}.INFORMATION_SCHEMA.COLUMNS c "
                f"JOIN {_quote(db)}.INFORMATION_SCHEMA.TABLES t "
                "ON t.table_schema = c.table_schema AND t.table_name = c.table_name "
                f"WHERE c.table_schema = '{_SCHEMA}' AND t.table_type = 'BASE TABLE' "
                f"AND NOT STARTSWITH(c.table_name, '{META_PREFIX}') "
                "ORDER BY c.table_name, c.ordinal_position")
            out = {}
            for table, col, dtype, clen, prec, scale, default in rows or []:
                out.setdefault(table, []).append((col, _type_sql(dtype, clen, prec, scale), default))
            self._cols_cache[db] = out
        return self._cols_cache[db]

    def _pks(self, db: str) -> dict:
        """{table: [primary key columns]} of db."""
        if db not in self._pk_cache:
            rows = self._execute(f"SHOW PRIMARY KEYS IN SCHEMA {_quote(db)}.{_SCHEMA}")
            out = {}
            # (created_on, database_name, schema_name, table_name, column_name,
            #  key_sequence, ...)
            for r in sorted(rows or [], key=lambda r: (r[3], int(r[5]))):
                out.setdefault(r[3], []).append(r[4])
            self._pk_cache[db] = out
        return self._pk_cache[db]

    def _tables(self, db: str) -> list:
        return sorted(self._table_meta(db))

    def _columns(self, db: str, table: str) -> list:
        return [c[0] for c in self._table_meta(db).get(table, [])]

    def _pk(self, db: str, table: str) -> list:
        return self._pks(db).get(table, [])

    def _readable(self, state: _State, table: str) -> bool:
        """Whether table can be read at state: False when the snapshot
        predates the table, or its database has been dropped."""
        if not state.is_snapshot:
            return table in self._table_meta(state.db)
        try:
            self._execute(f"SELECT 1 FROM {state.src(table)} LIMIT 1")
            return True
        except ProgrammingError as e:
            if e.errno in (_ER_NO_TIME_TRAVEL, _ER_NO_OBJECT):
                return False
            raise

    def _require_history(self, state: _State, what: str) -> None:
        """Raise unless the snapshot can still be read (its database was
        dropped, or it is past the Time Travel retention)."""
        if state.is_snapshot and not self._readable(state, COMMITS_TABLE):
            raise RuntimeError(f"{what}: its snapshot in {state.db} can no longer be read "
                               "(database dropped, or past Time Travel retention)")

    def _proj(self, state: _State, table: str, cols: list, marker: bool = False) -> str:
        """``cols`` of table at state, as a subquery; NULL for columns its
        database lacks. ``marker`` adds a non-null _BB_P column."""
        have = set(self._columns(state.db, table))
        sel = [_quote(c) if c in have else f"NULL AS {_quote(c)}" for c in cols]
        if marker:
            sel.insert(0, "TRUE AS _BB_P")
        return f"(SELECT {', '.join(sel)} FROM {state.src(table)})"

    @contextlib.contextmanager
    def _bulk(self):
        """One transaction, so a failed reset/revert/merge leaves the
        branch as it was. No DDL inside: DDL commits the transaction."""
        self._execute("BEGIN")
        try:
            yield
        except BaseException:
            self._execute("ROLLBACK")
            raise
        self._execute("COMMIT")

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def _storage_bytes(self) -> int:
        """Active, Time Travel and clone-retained bytes of the run's
        databases. Snowflake updates these figures with a delay (up to a
        couple of hours), so they lag recent operations."""
        rows = self._execute(
            "SELECT COALESCE(SUM(active_bytes + time_travel_bytes + retained_for_clone_bytes), 0) "
            f"FROM {_quote(self.db_name)}.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS "
            "WHERE table_catalog = %s OR STARTSWITH(table_catalog, %s)",
            (self.db_name, self.db_name + _BRANCH_SEP))
        return int(rows[0][0] or 0)

    def _connect_impl(self, ref: Ref) -> None:
        if ref.commit:
            self._use(self._temp_db_for(ref))
            return
        self._use(self._db_for(ref.branch))

    def _temp_db_for(self, ref: Ref) -> str:
        """A database holding ``ref``'s branch as of its commit, created on
        first use and dropped when the connection closes."""
        key = str(ref)
        db = self._temp_dbs.get(key)
        if db:
            return db
        db = self._new_temp("REF")
        self._clone(db, self._state(ref))
        self._temp_dbs[key] = db
        return db

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        src = self._db_for(from_ref.branch)
        db = branch_db_name(self.db_name, name)
        self._clone(db, self._state(from_ref))
        self._add_commit(db, "fork", f"fork from {from_ref}", parent=src)

    def _commit_impl(self, ref: Ref, message: str) -> str:
        return self._add_commit(self._on(ref), "commit", message)

    def _log_impl(self, ref: Ref, limit: int) -> list:
        db = self._db_for(ref.branch)
        max_seq = int(self._find_commit(db, ref.commit)["seq"]) if ref.commit else None
        return [
            {"commit_hash": r["id"], "kind": r["kind"], "committer": "snowflake",
             "email": "", "date": r["ts"], "message": r["message"]}
            for r in self._log_rows(db, limit=limit, max_seq=max_seq)
        ]

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        self._fresh()
        a, b = self._state(ref_a), self._state(ref_b)
        self._require_history(a, str(ref_a))
        self._require_history(b, str(ref_b))
        tables_a ={t for t in self._tables(a.db) if self._readable(a, t)}
        tables_b = {t for t in self._tables(b.db) if self._readable(b, t)}
        out = []
        for table in sorted(tables_a | tables_b):
            entry = {"table_name": table.lower(), "rows_added": 0, "rows_deleted": 0,
                     "rows_modified": 0}
            if table not in tables_b:
                entry["rows_deleted"] = int(self._execute(f"SELECT COUNT(*) FROM {a.src(table)}")[0][0])
            elif table not in tables_a:
                entry["rows_added"] = int(self._execute(f"SELECT COUNT(*) FROM {b.src(table)}")[0][0])
            else:
                cols_b = set(self._columns(b.db, table))
                cols = [c for c in self._columns(a.db, table) if c in cols_b]
                pk = [c for c in self._pk(a.db, table) if c in cols]
                rest = [c for c in cols if c not in pk]
                on = _join("x", "y", pk) if pk else _same("x", "y", cols)
                modified = f"COUNT_IF(x._BB_P AND y._BB_P AND NOT ({_same('x', 'y', rest)}))" \
                    if pk and rest else "0"
                added, deleted, mod = self._execute(
                    f"SELECT COUNT_IF(x._BB_P IS NULL), COUNT_IF(y._BB_P IS NULL), {modified} "
                    f"FROM {self._proj(a, table, cols, marker=True)} x "
                    f"FULL OUTER JOIN {self._proj(b, table, cols, marker=True)} y ON {on}")[0]
                entry.update(rows_added=int(added), rows_deleted=int(deleted),
                             rows_modified=int(mod))
            out.append(entry)
        return {
            "tables": out,
            "rows_added": sum(t["rows_added"] for t in out),
            "rows_deleted": sum(t["rows_deleted"] for t in out),
            "rows_modified": sum(t["rows_modified"] for t in out),
        }

    def _reset_impl(self, ref: Ref, to: str) -> dict:
        db = self._db_for(ref.branch)
        state = self._find_commit(db, to)["state"]
        tmp = self._new_temp("RESET")
        self._clone(tmp, state)
        try:
            archive = self._swap_in(db, tmp)
        except BaseException:
            self._drop_quietly(tmp)
            raise
        return {"archive": archive}

    def _revert_impl(self, ref: Ref, commit: str) -> None:
        self._fresh()
        db = self._on(ref)
        row = self._find_commit(db, commit)
        before = self._snapshot_of(row["prev_id"]) if row["prev_id"] else None
        if before is None:
            raise ValueError(f"commit '{commit}' has no parent to revert to")
        self._require_history(row["state"], f"commit '{commit}'")
        self._require_history(before, f"parent of commit '{commit}'")
        with self._bulk():
            self._revert_tables(db, before, row["state"])
        self._add_commit(db, "commit", f"revert {commit}")

    def _revert_tables(self, db: str, before: _State, after: _State) -> None:
        for table in self._tables(db):
            if not (self._readable(after, table) and self._readable(before, table)):
                continue  # absent before or at the commit
            cols = self._columns(db, table)
            pk = self._pk(db, table)
            if not pk:
                continue
            rest = [c for c in cols if c not in pk]
            dst = _qt(db, table)
            S, E = self._proj(before, table, cols), self._proj(after, table, cols)
            # Rows the commit added: delete them.
            self._execute(
                f"DELETE FROM {dst} WHERE {_key(None, pk)} IN "
                f"(SELECT {_key('e', pk)} FROM {E} e LEFT JOIN {S} s "
                f"ON {_join('s', 'e', pk)} WHERE s.{_quote(pk[0])} IS NULL)")
            # Rows the commit deleted or modified: put the earlier version back.
            differs = f"e.{_quote(pk[0])} IS NULL"
            if rest:
                differs += f" OR NOT ({_same('s', 'e', rest)})"
            self._execute(_merge_rows(
                dst, cols, pk,
                f"SELECT {_cols('s', cols)} FROM {S} s LEFT JOIN {E} e "
                f"ON {_join('s', 'e', pk)} WHERE {differs}"))

    # ------------------------------------------------------------------
    # Three-way merge (merge and rebase)
    # ------------------------------------------------------------------

    def _merge_base(self, ours_db: str, theirs_db: str):
        """(state, exact) for the common ancestor of two databases: the
        newest of the log rows both have and the fork/rebase rows by which
        one descends from the other. When that candidate's history is gone
        (its database was dropped), older candidates are tried and exact is
        False; (None, False) when none is readable."""
        O, T = self._log(ours_db), self._log(theirs_db)
        rows = self._execute(
            f"SELECT seq, id FROM {T} WHERE id IN (SELECT id FROM {O}) "
            f"UNION SELECT seq, id FROM {T} WHERE kind IN ('fork', 'rebase') AND parent = %s "
            f"UNION SELECT seq, id FROM {O} WHERE kind IN ('fork', 'rebase') AND parent = %s "
            f"ORDER BY seq DESC LIMIT {_BASE_CANDIDATES}",
            (ours_db, theirs_db))
        ids = [r[1] for r in rows or []]
        if not ids:
            return None, False
        snaps = {
            r[0]: _State(r[1], r[2], r[3]) for r in self._execute(
                f"SELECT id, db, qid, snap_ts FROM {self._snapshots()} "
                f"WHERE id IN ({', '.join(['%s'] * len(ids))})", tuple(ids)) or []
        }
        for i, cid in enumerate(ids):
            state = snaps.get(cid)
            if state is not None and self._readable(state, COMMITS_TABLE):
                return state, i == 0
        return None, False

    def _add_column(self, db: str, table: str, name: str, ctype: str, default) -> None:
        sql = f"ALTER TABLE {_qt(db, table)} ADD COLUMN {_quote(name)} {ctype}"
        if default is not None:
            try:
                # Carry the default so rows written later on this side get
                # the same value as on the other.
                self._execute(f"{sql} DEFAULT {default}")
                return
            except ProgrammingError:
                pass  # only constant defaults can be set when adding a column
        self._execute(sql)

    def _reconcile_schema(self, ours_db: str, theirs: _State) -> dict:
        """Give ``ours_db`` the tables and columns ``theirs`` has and it
        lacks. A differing primary key is a schema conflict (raises before
        anything is changed). Returns the tables and columns added."""
        ours = set(self._tables(ours_db))
        add_tables, add_cols = [], []
        for table in self._tables(theirs.db):
            if not self._readable(theirs, table):
                continue
            if table not in ours:
                add_tables.append(table)
                continue
            if self._pk(ours_db, table) != self._pk(theirs.db, table):
                raise RuntimeError(
                    f"schema conflict on {table.lower()}: primary keys differ between "
                    f"{ours_db} and {theirs.db}")
            have = set(self._columns(ours_db, table))
            for name, ctype, default in self._table_meta(theirs.db)[table]:
                if name not in have:
                    add_cols.append((table, name, ctype, default))
        for table in add_tables:
            self._execute(f"CREATE TABLE {_qt(ours_db, table)} CLONE {theirs.src(table)}")
        for table, name, ctype, default in add_cols:
            self._add_column(ours_db, table, name, ctype, default)
        if add_tables or add_cols:
            self._forget(ours_db)
        return {"tables": add_tables, "columns": [(t, c) for t, c, _, _ in add_cols]}

    @staticmethod
    def _schema_report(added: dict) -> dict:
        return {"added_tables": [t.lower() for t in added["tables"]],
                "added_columns": [f"{t}.{c}".lower() for t, c in added["columns"]]}

    def _three_way(self, ours_db: str, theirs: _State, base, on_conflict,
                   ref: Ref, tag: str, new_tables: list) -> dict:
        """Apply theirs' changes since base onto ours_db, in place.

        Returns conflict counts; rows both sides changed differently are
        resolved per ``on_conflict`` ("ours" keeps ours_db's version)."""
        info = {"conflicts": 0, "conflict_tables": [], "ours_changes": 0,
                "theirs_changes": 0, "merged_tables": [], "skipped_tables": {}}
        conflicts = []  # {"table", "rows"} for a callable
        tags = (f"{tag}T", f"{tag}O", f"{tag}C")
        with self._bulk():
            self._three_way_tables(ours_db, theirs, base, on_conflict, tags,
                                   new_tables, info, conflicts)
            if conflicts:
                info["resolution"] = on_conflict(self._conflict_session(ref), conflicts)
                info["resolved"] = "custom"
            elif info["conflicts"]:
                info["resolved"] = on_conflict
            self._execute(f"DELETE FROM {_qt(ours_db, KEYS_TABLE)} WHERE tag IN (%s, %s, %s)",
                          tags)
        return info

    def _three_way_tables(self, ours_db, theirs, base, on_conflict, tags,
                          new_tables, info, conflicts) -> None:
        keys_t = _qt(ours_db, KEYS_TABLE)
        tag_t, tag_o, tag_c = tags
        for table in sorted(set(self._tables(ours_db)) & set(self._tables(theirs.db))):
            name = table.lower()
            if table in new_tables:
                # Created on their side: it was cloned whole.
                info["merged_tables"].append(name)
                continue
            if not self._readable(theirs, table):
                continue
            cols_t = set(self._columns(theirs.db, table))
            cols = [c for c in self._columns(ours_db, table) if c in cols_t]
            pk = [c for c in self._pk(ours_db, table) if c in cols]
            O, T = _qt(ours_db, table), self._proj(theirs, table, cols)
            if base is not None and self._readable(base, table):
                B = self._proj(base, table, cols)
            else:
                # No base (or the table is newer than it): every row counts
                # as added on both sides, so neither side's deletes apply.
                B = f"(SELECT {_cols(None, cols)} FROM {T} z WHERE 1 = 0)"
            if not pk:
                # No key to match rows on: take their new rows only.
                self._execute(
                    f"INSERT INTO {O} ({_cols(None, cols)}) "
                    f"SELECT {_cols('t', cols)} FROM {T} t WHERE NOT EXISTS "
                    f"(SELECT 1 FROM {B} b WHERE {_same('b', 't', cols)})")
                info["merged_tables"].append(name)
                info["skipped_tables"][name] = "no primary key: new rows only"
                continue
            rest = [c for c in cols if c not in pk]
            kt, ko, kb = _key("t", pk), _key("o", pk), _key("b", pk)
            p0 = _quote(pk[0])
            differs_tb = f"b.{p0} IS NULL" + (f" OR NOT ({_same('t', 'b', rest)})" if rest else "")
            differs_ob = f"b.{p0} IS NULL" + (f" OR NOT ({_same('o', 'b', rest)})" if rest else "")
            ins = f"INSERT INTO {keys_t} (tag, tbl, k, side) "
            # Their changes since base, then ours.
            n_t = self._execute_count(
                f"{ins}SELECT %s, %s, {kt}, 'u' FROM {T} t LEFT JOIN {B} b "
                f"ON {_join('t', 'b', pk)} WHERE {differs_tb}", (tag_t, table))
            n_t += self._execute_count(
                f"{ins}SELECT %s, %s, {kb}, 'd' FROM {B} b LEFT JOIN {T} t "
                f"ON {_join('t', 'b', pk)} WHERE t.{p0} IS NULL", (tag_t, table))
            n_o = self._execute_count(
                f"{ins}SELECT %s, %s, {ko}, 'u' FROM {O} o LEFT JOIN {B} b "
                f"ON {_join('o', 'b', pk)} WHERE {differs_ob}", (tag_o, table))
            n_o += self._execute_count(
                f"{ins}SELECT %s, %s, {kb}, 'd' FROM {B} b LEFT JOIN {O} o "
                f"ON {_join('o', 'b', pk)} WHERE o.{p0} IS NULL", (tag_o, table))
            info["theirs_changes"] += n_t
            info["ours_changes"] += n_o
            if not n_t:
                continue
            # Conflicts: keys both changed, unless both made the same change.
            same_ot = _same("o", "t", rest)
            n_c = self._execute_count(
                f"{ins}SELECT %s, %s, a.k, 'c' FROM {keys_t} a JOIN {keys_t} b "
                "ON b.tag = %s AND b.tbl = a.tbl AND b.k = a.k "
                f"LEFT JOIN {O} o ON {ko} = a.k LEFT JOIN {T} t ON {kt} = a.k "
                "WHERE a.tag = %s AND a.tbl = %s "
                "AND NOT (a.side = 'd' AND b.side = 'd') "
                f"AND NOT (a.side = 'u' AND b.side = 'u' AND {same_ot})",
                (tag_c, table, tag_o, tag_t, table))
            not_conflict = (f"NOT EXISTS (SELECT 1 FROM {keys_t} c WHERE c.tag = '{tag_c}' "
                            f"AND c.tbl = '{table}' AND c.k = s.k)")
            # Apply their non-conflicting changes.
            self._execute(
                f"DELETE FROM {O} WHERE {_key(None, pk)} IN "
                f"(SELECT s.k FROM {keys_t} s WHERE s.tag = %s AND s.tbl = %s "
                f"AND s.side = 'd' AND {not_conflict})", (tag_t, table))
            self._execute(_merge_rows(
                O, cols, pk,
                f"SELECT {_cols('t', cols)} FROM {T} t WHERE {kt} IN "
                f"(SELECT s.k FROM {keys_t} s WHERE s.tag = %s AND s.tbl = %s "
                f"AND s.side = 'u' AND {not_conflict})"), (tag_t, table))
            info["merged_tables"].append(name)
            if n_c:
                info["conflicts"] += n_c
                info["conflict_tables"].append(name)
                if callable(on_conflict):
                    conflicts.append({"table": name, "rows": self._conflict_rows(
                        keys_t, O, T, B, cols, pk, tag_c, table)})
                elif on_conflict == "theirs":
                    self._apply_theirs(keys_t, O, T, cols, pk, tag_t, tag_c, table)

    def _apply_theirs(self, keys_t, O, T, cols, pk, tag_t, tag_c, table) -> None:
        conflict = (f"EXISTS (SELECT 1 FROM {keys_t} c WHERE c.tag = '{tag_c}' "
                    f"AND c.tbl = '{table}' AND c.k = s.k)")
        self._execute(
            f"DELETE FROM {O} WHERE {_key(None, pk)} IN "
            f"(SELECT s.k FROM {keys_t} s WHERE s.tag = %s AND s.tbl = %s "
            f"AND s.side = 'd' AND {conflict})", (tag_t, table))
        self._execute(_merge_rows(
            O, cols, pk,
            f"SELECT {_cols('t', cols)} FROM {T} t WHERE {_key('t', pk)} IN "
            f"(SELECT s.k FROM {keys_t} s WHERE s.tag = %s AND s.tbl = %s "
            f"AND s.side = 'u' AND {conflict})"), (tag_t, table))

    def _conflict_rows(self, keys_t, O, T, B, cols, pk, tag_c, table) -> list:
        """Conflict rows in Dolt's dolt_conflicts_<table> shape: base_*,
        our_*, their_* columns (lower case) plus our_diff_type /
        their_diff_type."""
        p0 = _quote(pk[0])
        rows = self._execute(
            f"SELECT {_cols('b', cols)}, {_cols('o', cols)}, {_cols('t', cols)}, "
            f"b.{p0} IS NULL, o.{p0} IS NULL, t.{p0} IS NULL FROM {keys_t} c "
            f"LEFT JOIN {B} b ON {_key('b', pk)} = c.k "
            f"LEFT JOIN {O} o ON {_key('o', pk)} = c.k "
            f"LEFT JOIN {T} t ON {_key('t', pk)} = c.k "
            f"WHERE c.tag = %s AND c.tbl = %s LIMIT {_CONFLICT_ROW_LIMIT}", (tag_c, table))
        lower = [c.lower() for c in cols]
        names = ([f"base_{c}" for c in lower] + [f"our_{c}" for c in lower]
                 + [f"their_{c}" for c in lower])
        out = []
        for r in rows or []:
            d = dict(zip(names, r[:len(names)]))
            no_base, no_ours, no_theirs = r[len(names):]

            def diff_type(missing):
                if missing:
                    return "removed"
                return "added" if no_base else "modified"
            d["our_diff_type"] = diff_type(no_ours)
            d["their_diff_type"] = diff_type(no_theirs)
            out.append(d)
        return out

    def _copy_log(self, into_db: str, from_db: str) -> None:
        """Add from_db's commits that into_db does not have to its log."""
        self._execute(
            f"INSERT INTO {self._log(into_db)} ({', '.join(_LOG_COLS)}) "
            f"SELECT {_log_cols('s')} FROM {self._log(from_db)} s "
            f"WHERE s.kind = 'commit' AND s.id NOT IN (SELECT id FROM {self._log(into_db)})")

    def _merge_impl(self, into: Ref, source: Ref, message: str,
                    on_conflict="ours") -> dict:
        self._fresh()
        ours_db = self._on(into)
        source_db = self._db_for(source.branch)
        if source_db == ours_db:
            raise ValueError(f"Cannot merge branch '{source.branch}' into itself")
        base, exact = self._merge_base(ours_db, source_db)
        theirs = self._state(source)
        added = self._reconcile_schema(ours_db, theirs)
        info = self._three_way(ours_db, theirs, base, on_conflict, into,
                               f"M{uuid.uuid4().hex[:10]}", added["tables"])
        self._copy_log(ours_db, source_db)
        info["hash"] = self._add_commit(
            ours_db, "merge", message or f"Merge {source.branch} into {into.branch}",
            parent=source_db)
        info["fast_forward"] = info["ours_changes"] == 0
        info["schema_changes"] = self._schema_report(added)
        if base is None:
            info["two_way"] = True
        elif not exact:
            info["fallback_base"] = True
        return info

    def _rebase_impl(self, ref: Ref, onto: Ref, on_conflict="ours") -> dict:
        self._fresh()
        ours_db = self._on(ref)
        onto_db = self._db_for(onto.branch)
        if onto_db == ours_db:
            raise ValueError(f"Cannot rebase branch '{ref.branch}' onto itself")
        base, exact = self._merge_base(ours_db, onto_db)
        # The upstream at one point: the clone is taken there, and it is the
        # branch's new fork point.
        upstream = self._state(onto) if onto.commit else _State(onto_db, ts=self._now())
        tmp = self._new_temp("REBASE")
        self._clone(tmp, upstream)
        try:
            added = self._reconcile_schema(tmp, _State(ours_db))
            # The clone is "ours" (the upstream, as in Dolt); the branch's
            # changes since base are "theirs". A resolve callable runs on it.
            self._use(tmp)
            info = self._three_way(tmp, _State(ours_db), base, on_conflict, ref,
                                   f"R{uuid.uuid4().hex[:10]}", added["tables"])
            self._copy_log(tmp, ours_db)
            self._use(ours_db)
            info["archive"] = self._swap_in(ours_db, tmp)
        except BaseException:
            self._current_ref = None
            try:
                self._use(ours_db)
            except Exception:
                self._current_db = None
            self._drop_quietly(tmp)
            raise
        self._add_commit(ours_db, "rebase", f"rebase onto {onto}", parent=onto_db,
                         snapshot=upstream)
        info["up_to_date"] = info["ours_changes"] == 0
        info["schema_changes"] = self._schema_report(added)
        if base is None:
            info["two_way"] = True
        elif not exact:
            info["fallback_base"] = True
        return info

    # ------------------------------------------------------------------

    def _delete_impl(self, ref: Ref) -> dict:
        """Drop the branch's database. Clones don't depend on the database
        they were cloned from, so even main can be dropped. As in git, the
        branch's commits stay readable while another branch's log has them
        (merged, or inherited by a child): the database is then renamed to
        an archive instead, and its snapshots repointed there."""
        db = self._db_for(ref.branch)
        if db == self._current_db:
            self._use(self._db_for(self.default_branch))
            self._current_ref = Ref(self.default_branch)
        self._forget(db)
        self._ref_states.clear()
        if not self._referenced_elsewhere(db):
            self._execute(f"DROP DATABASE {_quote(db)}")
            return {"archived": False}
        archive = self._run_db(f"{_ARCHIVE_PREFIX}{time.time_ns():x}")
        self._execute(f"ALTER DATABASE {_quote(db)} RENAME TO {_quote(archive)}")
        self._execute(f"UPDATE {self._snapshots()} SET db = %s WHERE db = %s", (archive, db))
        return {"archived": True, "archive": archive}

    def _referenced_elsewhere(self, db: str) -> bool:
        """Whether another live branch's log has a commit whose snapshot is
        in db."""
        for attempt in range(2):
            with self.conn.cursor() as cur:
                others = [d for d in _run_databases(cur, self.db_name) + [self.db_name]
                          if d != db and self._is_branch_db(d)]
            if not others:
                return False
            parts = [f"SELECT 1 FROM {self._log(o)} WHERE id IN "
                     f"(SELECT id FROM {self._snapshots()} WHERE db = %s)" for o in others]
            try:
                return bool(self._execute(" UNION ALL ".join(parts) + " LIMIT 1",
                                          (db,) * len(parts)))
            except ProgrammingError as e:
                # Another worker dropped a branch between the listing and
                # the query; list again.
                if e.errno != _ER_NO_OBJECT or attempt:
                    raise
        return True

    def _is_branch_db(self, db: str) -> bool:
        if db == self.db_name:
            return True
        name = db[len(self.db_name) + len(_BRANCH_SEP):]
        return not name.startswith((_TEMP_PREFIX, _ARCHIVE_PREFIX)) and name != _META_SUFFIX

    def _qualified_table(self, ref: Ref, table: str) -> str:
        return self._state(ref).src(_ident(table))

    def close_connection(self) -> None:
        if self.conn and self._temp_dbs:
            for db in list(self._temp_dbs.values()):
                self._drop_quietly(db)
            self._temp_dbs.clear()
        super().close_connection()
