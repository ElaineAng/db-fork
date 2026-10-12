"""MotherDuck backend.

MotherDuck is DuckDB in the cloud: full SQL, plus zero-copy database clones
(CREATE DATABASE b FROM a, metadata only), in-place overwrites (COPY FROM
DATABASE a (OVERWRITE) TO b) and queries across databases in one session.
Each of our branches is its own database:

    main branch      -> <db_name>
    branch "x"       -> <db_name>__x
    commit <id>      -> <db_name>__c_<id>   (a frozen clone, never written)

A commit is a row in the branch's _bb_commits table followed by a frozen
zero-copy clone of the branch, so the clone carries the log as of the
commit. Clones keep their contents for as long as they exist, so commits do
not depend on MotherDuck's snapshot retention (which the free plan does not
have); a deleted branch's commits stay readable.

    branch     CREATE DATABASE <branch> FROM <parent or commit>, plus a fork
               commit (its frozen clone is the fork point)
    commit     INSERT INTO _bb_commits, CREATE DATABASE <commit> FROM <branch>
    log        SELECT FROM _bb_commits (a commit's clone has the log up to it)
    diff       primary-key joins between the two databases
    merge      SQL three-way merge on MotherDuck: base = the newest commit both
               logs have, or the fork point; ours = the target database,
               theirs = the source database
    rebase     clone the upstream, three-way merge the branch's changes into
               the clone, COPY FROM DATABASE ... (OVERWRITE) into the branch
    reset      COPY FROM DATABASE <commit> (OVERWRITE) TO <branch>
    revert     the inverse of the commit's row changes, applied with SQL
    delete     DROP DATABASE <branch>

The three-way merge (dblib.duckdb_common) matches rows by primary key;
tables without one only receive the other side's new rows. Conflicts are
rows both sides changed differently, resolved per on_conflict ("ours" |
"theirs" | callable, see dblib.db_api); in a rebase, as in Dolt, "ours" is
the upstream. A table the source added is created from its DDL, added
columns are added, and a differing primary key or column type is a schema
conflict that fails the verb before any data changes. Its working tables
live in a _bb schema of the target database, so it runs on MotherDuck.

DuckDB uses optimistic concurrency: a transaction that changes rows another
committed transaction changed fails with a conflict instead of waiting.
"""

import os
import re
import threading
import time
import uuid

import duckdb
from dotenv import load_dotenv

from dblib import duckdb_common as ddb
from dblib.db_api import DBToolSuite, Ref
import dblib.result_collector as rc

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token", "")
# A database of the account each connection opens on (single attach mode
# needs one) and moves to before dropping a run's databases.
MOTHERDUCK_HOME_DB = os.environ.get("MOTHERDUCK_HOME_DB", "my_db")

MAIN_BRANCH = "main"
_SEP = "__"
_COMMIT_PREFIX = "c_"
_TEMP_PREFIX = "tmp_"
_NAME_RE = re.compile(r"^[a-z0-9_]+$")

COMMITS_TABLE = "_bb_commits"
META_PREFIX = "_bb_"
SCRATCH_SCHEMA = "_bb"
# Optional file that every database-level operation (clone, overwrite,
# drop, attach, USE) is appended to, for debugging concurrent runs.
MOTHERDUCK_TRACE = os.environ.get("MOTHERDUCK_TRACE", "")
_trace_lock = threading.Lock()


def _trace(msg: str) -> None:
    if MOTHERDUCK_TRACE:
        with _trace_lock, open(MOTHERDUCK_TRACE, "a") as f:
            f.write(f"{time.time():.3f} {threading.current_thread().name} {msg}\n")


# Attempts to make a database usable in a connection's catalog (_attach).
_ATTACH_TRIES = 8
# Attempts at a clone that comes out unusable (_clone).
_CLONE_TRIES = 3

_LOG_COLS = ("seq", "id", "kind", "message", "parent", "prev_id", "ts")
_COMMITS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    seq BIGINT NOT NULL,
    id VARCHAR NOT NULL,
    kind VARCHAR NOT NULL,
    message VARCHAR,
    parent VARCHAR,
    prev_id VARCHAR,
    ts TIMESTAMP
)
"""
_CREATE_VIEW_RE = re.compile(r"^\s*CREATE\s+VIEW\s+(\"[^\"]+\"|\S+?)\s+AS", re.I)
_CREATE_TABLE_RE = re.compile(r"^\s*CREATE\s+TABLE\s+(\"[^\"]+\"|\S+?)\s*\(", re.I)


def connect():
    """Open a DuckDB connection to the account (MOTHERDUCK_TOKEN) in single
    attach mode: the connection's catalog holds only what it attaches. In
    the default workspace mode, attaching or detaching a database in one
    connection does the same in every other connection of the account,
    so concurrent workers would detach each other's databases."""
    if not MOTHERDUCK_TOKEN:
        raise RuntimeError("MOTHERDUCK_TOKEN must be set (see the MotherDuck backend "
                           "section of the README)")
    return duckdb.connect(f"md:{MOTHERDUCK_HOME_DB}?attach_mode=single",
                          config={"motherduck_token": MOTHERDUCK_TOKEN})


def _name(name: str) -> str:
    out = name.lower()
    if not _NAME_RE.match(out):
        raise ValueError(f"'{name}' cannot be used as a MotherDuck database name "
                         "(letters, digits and _ only)")
    return out


def _q(ident: str) -> str:
    return ddb.q(ident)


def _qt(db: str, table: str) -> str:
    return f"{_q(db)}.main.{_q(table)}"


def branch_db_name(db_name: str, branch: str) -> str:
    root = _name(db_name)
    return root if branch == MAIN_BRANCH else _name(f"{root}{_SEP}{branch}")


def commit_db_name(db_name: str, commit_id: str) -> str:
    return _name(f"{_name(db_name)}{_SEP}{_COMMIT_PREFIX}{commit_id}")


def _run_databases(con, db_name: str) -> list:
    """The run's databases (root first is not guaranteed). Listed from
    MotherDuck itself: databases another process created may not be
    attached in this session's catalog."""
    root = _name(db_name)
    rows = con.execute(
        "SELECT name FROM md_information_schema.databases "
        "WHERE name = ? OR starts_with(name, ?)", [root, root + _SEP]).fetchall()
    return [r[0] for r in rows]


def setup_database(db_name: str, sql_path: str = None) -> None:
    """(Re)create db_name, with no branches."""
    if sql_path:
        raise NotImplementedError(
            "The MotherDuck backend does not load SQL dumps yet; "
            "use database_setup { generated {} }")
    drop_database(db_name)
    con = connect()
    try:
        con.execute(f"CREATE DATABASE {_q(_name(db_name))}")
        print("Database created successfully.")
    finally:
        con.close()


def drop_database(db_name: str) -> None:
    """Drop db_name and every database of the run (branches, commits,
    temporaries)."""
    con = connect()
    try:
        con.execute(f"USE {_q(MOTHERDUCK_HOME_DB)}")
        for db in _run_databases(con, db_name):
            con.execute(f"ATTACH IF NOT EXISTS 'md:{db}'")
            con.execute(f"DROP DATABASE IF EXISTS {_q(db)}")
        print(f"Database '{db_name}' deleted successfully.")
    finally:
        con.close()


class _Connection:
    """What sessions get as their connection: pyformat cursors over the
    suite's DuckDB connection; cancel() interrupts the running query (the
    operation watchdog calls it)."""

    def __init__(self, raw):
        self.raw = raw

    def cursor(self):
        return ddb.DuckDBCursor(self.raw)

    def cancel(self):
        self.raw.interrupt()

    def close(self):
        self.raw.close()


class MotherDuckToolSuite(DBToolSuite):
    BACKEND_NAME = "motherduck"
    SUPPORTS_COMMIT_REFS = True
    SUPPORTS_MULTI_REF_EXEC = True
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
        "branch": "CREATE DATABASE ... FROM (zero-copy clone)",
        "commit": "_bb_commits row + a frozen zero-copy clone of the branch",
        "diff": "primary-key joins across the two databases",
        "log": "SELECT from the _bb_commits bookkeeping table",
        "merge": "SQL three-way merge on MotherDuck against the newest common commit or fork point",
        "rebase": "clone of the upstream + SQL three-way merge, COPY FROM DATABASE (OVERWRITE)",
        "revert": "inverse of the commit's row changes applied with SQL",
        "reset": "COPY FROM DATABASE <commit clone> (OVERWRITE) TO <branch>",
        "delete": "DROP DATABASE",
        "commit_refs": "reads of the commit's frozen clone",
        "multi_ref_exec": "cross-database queries",
    }

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return "md:"

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return f"md:{_name(db_name)}"

    @classmethod
    def init_for_bench(cls, collector: rc.ResultCollector, db_name: str,
                       default_branch_name: str = MAIN_BRANCH,
                       measure_storage: bool = False):
        return cls(connect(), collector, db_name, default_branch_name, measure_storage)

    def __init__(self, raw_connection, collector: rc.ResultCollector, db_name: str,
                 default_branch_name: str = MAIN_BRANCH, measure_storage: bool = False):
        super().__init__(_Connection(raw_connection), collector, measure_storage)
        self.db_name = _name(db_name)
        self.default_branch = default_branch_name
        self._current_db = None
        # Commit-ref temporaries this suite created: Ref string -> database.
        self._temp_dbs: dict = {}
        # Databases attached in this connection's catalog (see _attach).
        self._attached: set = set()
        self._ensure_meta(self._db_for(default_branch_name))
        self._connect_impl(Ref(default_branch_name))
        self._current_ref = Ref(default_branch_name)

    # ------------------------------------------------------------------
    # Names and the session's database
    # ------------------------------------------------------------------

    def _x(self, sql: str, params=None) -> list:
        """Run one internal statement (DuckDB ? parameters); rows if any."""
        if MOTHERDUCK_TRACE and sql.lstrip().upper().startswith(
                ("CREATE DATABASE", "COPY FROM DATABASE", "DROP DATABASE", "USE", "ATTACH", "DETACH")):
            _trace(sql)
        raw = self.conn.raw
        cur = raw.execute(sql, params) if params is not None else raw.execute(sql)
        return cur.fetchall() if cur.description else []

    def _db_for(self, branch: str) -> str:
        return branch_db_name(self.db_name, branch)

    def _commit_db(self, commit_id: str) -> str:
        return commit_db_name(self.db_name, commit_id)

    def _new_temp(self) -> str:
        return _name(f"{self.db_name}{_SEP}{_TEMP_PREFIX}{uuid.uuid4().hex[:12]}")

    def _ref_db(self, ref: Ref) -> str:
        """The database holding a ref: the branch's, or the commit's clone."""
        return self._commit_db(ref.commit) if ref.commit else self._db_for(ref.branch)

    def _attach(self, db: str, require_log: bool = True) -> str:
        """Make db usable in this connection. Each connection has its own
        catalog: a database another connection (another worker) created is
        not visible until attached, and under concurrent CREATE DATABASE a
        just-created database can be missing even from the creating
        connection's catalog for a moment, or attached with its tables not
        yet visible. So attach, prove the database's log table binds (every
        branch, commit and temporary database has one; ``require_log`` is
        off only while the root's log is created), and retry with a short
        backoff. Returns db."""
        if db in self._attached:
            return db
        last = None
        for attempt in range(_ATTACH_TRIES):
            try:
                self._x(f"ATTACH IF NOT EXISTS 'md:{db}'")
                if require_log:
                    self._x(f"SELECT 1 FROM {_qt(db, COMMITS_TABLE)} LIMIT 0")
                else:
                    self._x(f"SHOW TABLES FROM {_q(db)}")  # binds the database
                self._attached.add(db)
                return db
            except duckdb.Error as e:
                last = e
                if not self._x("SELECT 1 FROM md_information_schema.databases WHERE name = ?", [db]):
                    raise ValueError(f"MotherDuck database '{db}' does not exist") from e
                if self._current_db != db:
                    try:
                        self._x(f"DETACH DATABASE IF EXISTS {_q(db)}")
                    except duckdb.Error:
                        pass
                time.sleep(min(0.1 * 2 ** attempt, 2.0))
        raise last

    def _use(self, db: str) -> None:
        self._attach(db)
        self._x(f"USE {_q(db)}")
        self._current_db = db
        # Callers that move to a ref (_on, _connect) set it again; any other
        # switch (a rebase's temporary, a cleanup) must make the next exec()
        # reconnect rather than run on this database.
        self._current_ref = None

    def _on(self, ref: Ref) -> str:
        db = self._db_for(ref.branch)
        if self._current_db != db:
            self._use(db)
        self._current_ref = Ref(ref.branch)
        return db

    def _clone(self, new: str, source: str) -> None:
        """CREATE DATABASE new FROM source, until new is usable. Under
        concurrent clones MotherDuck occasionally fails a clone with an
        "internal error", creates it but fails to attach it ("already
        exists": our names are unique), or creates one whose tables never
        appear in this connection; each is retried, the last by dropping
        the clone and cloning again."""
        self._attach(source)
        for attempt in range(_CLONE_TRIES):
            last = attempt == _CLONE_TRIES - 1
            try:
                self._x(f"CREATE DATABASE {_q(new)} FROM {_q(source)}")
            except duckdb.Error as e:
                transient = ("internal MotherDuck error" in str(e)
                             or "already exists" in str(e))
                exists = self._x("SELECT 1 FROM md_information_schema.databases WHERE name = ?",
                                 [new])
                if not transient or (last and not exists):
                    raise
                if not exists:
                    time.sleep(0.5 * (attempt + 1))
                    continue
            try:
                self._attach(new)
                return
            except duckdb.Error:
                if last:
                    raise
                _trace(f"clone {new} of {source} not usable; cloning again")
                try:
                    self._x(f"ATTACH IF NOT EXISTS 'md:{new}'")
                    self._x(f"DROP DATABASE IF EXISTS {_q(new)}")
                except duckdb.Error:
                    pass
                self._attached.discard(new)
                time.sleep(0.5 * (attempt + 1))

    def _drop_quietly(self, db: str) -> None:
        try:
            if self._current_db == db:
                self._use(self._db_for(self.default_branch))
            self._attach(db)
            self._x(f"DROP DATABASE IF EXISTS {_q(db)}")
        except Exception:
            pass
        self._attached.discard(db)

    def list_branches(self) -> list:
        prefix = self.db_name + _SEP
        names = [d[len(prefix):] for d in _run_databases(self.conn.raw, self.db_name)
                 if d.startswith(prefix)]
        return [MAIN_BRANCH] + [n for n in names
                                if not n.startswith((_COMMIT_PREFIX, _TEMP_PREFIX))]

    # ------------------------------------------------------------------
    # Commit log
    # ------------------------------------------------------------------

    def _log(self, db: str) -> str:
        return _qt(self._attach(db), COMMITS_TABLE)

    def _ensure_meta(self, db: str) -> None:
        self._attach(db, require_log=False)
        self._x(_COMMITS_DDL.format(table=self._log(db)))
        if not self._x(f"SELECT COUNT(*) FROM {self._log(db)}")[0][0]:
            self._add_commit(db, "root", "initial state")

    def _add_commit(self, db: str, kind: str, message: str, parent: str = None,
                    frozen: bool = False) -> str:
        """Append a row to db's log, then freeze db as the commit's clone (so
        the clone has the row). ``frozen`` means the caller already created
        the clone. Returns the commit id."""
        seq = time.time_ns()
        commit_id = f"{seq:x}"
        log = self._log(db)
        self._x(
            f"INSERT INTO {log} ({', '.join(_LOG_COLS)}) SELECT ?, ?, ?, ?, ?, "
            f"(SELECT id FROM {log} ORDER BY seq DESC LIMIT 1), now()",
            [seq, commit_id, kind, message or "", parent])
        if not frozen:
            self._clone(self._commit_db(commit_id), db)
        return commit_id

    def _log_rows(self, db: str, limit: int = None) -> list:
        sql = f"SELECT {', '.join(_LOG_COLS)} FROM {self._log(db)} ORDER BY seq DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return [dict(zip(_LOG_COLS, r)) for r in self._x(sql)]

    def _find_commit(self, db: str, commit_id: str) -> dict:
        rows = self._x(f"SELECT {', '.join(_LOG_COLS)} FROM {self._log(db)} WHERE id = ? "
                       "ORDER BY seq DESC LIMIT 1", [str(commit_id)])
        if not rows:
            raise ValueError(f"unknown commit '{commit_id}' on {db}")
        return dict(zip(_LOG_COLS, rows[0]))

    def _copy_log(self, into_db: str, from_db: str) -> None:
        """Add from_db's commits that into_db does not have to its log."""
        cols = ", ".join(_LOG_COLS)
        self._x(f"INSERT INTO {self._log(into_db)} ({cols}) SELECT {cols} FROM {self._log(from_db)} s "
                f"WHERE s.kind = 'commit' AND s.id NOT IN (SELECT id FROM {self._log(into_db)})")

    def _merge_base(self, ours_db: str, theirs_db: str, ours_name: str, theirs_name: str):
        """The commit database of the two branches' common ancestor: the
        newest of the log rows both have and the fork/rebase rows by which
        one descends from the other; None if there is none."""
        O, T = self._log(ours_db), self._log(theirs_db)
        rows = self._x(
            f"SELECT seq, id FROM {T} WHERE id IN (SELECT id FROM {O}) "
            f"UNION SELECT seq, id FROM {T} WHERE kind IN ('fork', 'rebase') AND parent = ? "
            f"UNION SELECT seq, id FROM {O} WHERE kind IN ('fork', 'rebase') AND parent = ? "
            f"ORDER BY seq DESC LIMIT 1", [ours_name, theirs_name])
        return self._commit_db(rows[0][1]) if rows else None

    # ------------------------------------------------------------------
    # Table metadata
    # ------------------------------------------------------------------

    def _columns(self, db: str) -> dict:
        """{table: [(column, type, default)]} of db's data tables."""
        self._attach(db)
        out = {}
        for table, col, dtype, default in self._x(
                "SELECT c.table_name, c.column_name, c.data_type, c.column_default "
                "FROM duckdb_columns() c JOIN duckdb_tables() t ON t.database_name = c.database_name "
                "AND t.schema_name = c.schema_name AND t.table_name = c.table_name "
                "WHERE c.database_name = ? AND c.schema_name = 'main' "
                "AND NOT starts_with(c.table_name, ?) "
                "ORDER BY c.table_name, c.column_index", [db, META_PREFIX]):
            out.setdefault(table, []).append((col, dtype, default))
        return out

    def _views(self, db: str) -> dict:
        """{view: CREATE VIEW statement} of db."""
        self._attach(db)
        return dict(self._x("SELECT view_name, sql FROM duckdb_views() WHERE database_name = ? "
                            "AND schema_name = 'main' AND NOT internal", [db]))

    def _pks(self, db: str) -> dict:
        self._attach(db)
        return {t: list(cols) for t, cols in self._x(
            "SELECT table_name, constraint_column_names FROM duckdb_constraints() "
            "WHERE database_name = ? AND schema_name = 'main' AND constraint_type = 'PRIMARY KEY'",
            [db])}

    @staticmethod
    def _rel(db: str, table: str, cols: list, meta: dict, types: dict) -> str:
        """``cols`` of db's table (NULL for missing columns; an empty relation
        if the table is missing)."""
        if table not in meta:
            sel = ", ".join(f"CAST(NULL AS {types.get(c, 'VARCHAR')}) AS {_q(c)}" for c in cols)
            return f"(SELECT {sel} WHERE FALSE)"
        return ddb.project_sql(_qt(db, table), cols, [c for c, _, _ in meta[table]], types)

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def _storage_bytes(self) -> int:
        """Sizes of the run's databases (pragma_database_size, which reports
        human-readable sizes)."""
        units = {"bytes": 1, "kib": 2 ** 10, "mib": 2 ** 20, "gib": 2 ** 30, "tib": 2 ** 40}
        # The runner may ask after the connection closed.
        raw = self.conn.raw if self.conn is not None else connect()
        for db in _run_databases(raw, self.db_name):
            try:
                raw.execute(f"ATTACH IF NOT EXISTS 'md:{db}'")
            except duckdb.Error:
                # Being created or dropped by another worker right now
                # ("already exists", an internal error): measure the rest.
                pass
        total = 0
        for db, size in raw.execute(
                "SELECT database_name, database_size FROM pragma_database_size()").fetchall():
            if db != self.db_name and not db.startswith(self.db_name + _SEP):
                continue
            m = re.match(r"([\d.]+)\s*(\w+)", str(size or ""))
            if m:
                total += int(float(m.group(1)) * units.get(m.group(2).lower(), 1))
        return total

    def _connect_impl(self, ref: Ref) -> None:
        if ref.commit:
            # Writes on a commit ref must not touch the commit's clone.
            key = str(ref)
            if key not in self._temp_dbs:
                tmp = self._new_temp()
                self._clone(tmp, self._commit_db(ref.commit))
                self._temp_dbs[key] = tmp
            self._use(self._temp_dbs[key])
            return
        self._use(self._db_for(ref.branch))

    def _qualified_table(self, ref: Ref, table: str) -> str:
        return _qt(self._attach(self._ref_db(ref)), table.lower())

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        db = self._db_for(name)
        self._clone(db, self._ref_db(from_ref))
        self._add_commit(db, "fork", f"fork from {from_ref}",
                         parent=self._db_for(from_ref.branch))

    def _commit_impl(self, ref: Ref, message: str) -> str:
        return self._add_commit(self._on(ref), "commit", message)

    def _log_impl(self, ref: Ref, limit: int) -> list:
        return [
            {"commit_hash": r["id"], "kind": r["kind"], "committer": "motherduck",
             "email": "", "date": r["ts"], "message": r["message"]}
            for r in self._log_rows(self._ref_db(ref), limit)
        ]

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        a, b = self._ref_db(ref_a), self._ref_db(ref_b)
        meta_a, meta_b = self._columns(a), self._columns(b)
        pks_a, pks_b = self._pks(a), self._pks(b)
        out = []
        for table in sorted(set(meta_a) | set(meta_b)):
            entry = {"table_name": table, "rows_added": 0, "rows_deleted": 0, "rows_modified": 0}
            if table not in meta_b:
                entry["rows_deleted"] = int(self._x(f"SELECT COUNT(*) FROM {_qt(a, table)}")[0][0])
            elif table not in meta_a:
                entry["rows_added"] = int(self._x(f"SELECT COUNT(*) FROM {_qt(b, table)}")[0][0])
            else:
                have_b = {c for c, _, _ in meta_b[table]}
                cols = [c for c, _, _ in meta_a[table] if c in have_b]
                pk = pks_a.get(table) or pks_b.get(table) or []
                added, deleted, modified = ddb.diff_counts(
                    self.conn.raw, _qt(a, table), _qt(b, table), cols, pk)
                entry.update(rows_added=added, rows_deleted=deleted, rows_modified=modified)
            out.append(entry)
        return {
            "tables": out,
            "rows_added": sum(t["rows_added"] for t in out),
            "rows_deleted": sum(t["rows_deleted"] for t in out),
            "rows_modified": sum(t["rows_modified"] for t in out),
        }

    def _overwrite(self, target: str, source: str) -> None:
        """Replace target's contents with source's (zero-copy); target keeps
        its identity, and sessions on it see the new contents."""
        self._attach(source)
        self._attach(target)
        self._x(f"COPY FROM DATABASE {_q(source)} (OVERWRITE) TO {_q(target)}")

    def _reset_impl(self, ref: Ref, to: str) -> None:
        db = self._db_for(ref.branch)
        self._find_commit(db, to)  # must be in the branch's history
        self._overwrite(db, self._commit_db(to))

    def _revert_impl(self, ref: Ref, commit: str) -> None:
        db = self._on(ref)
        row = self._find_commit(db, commit)
        if not row["prev_id"]:
            raise ValueError(f"commit '{commit}' has no parent to revert to")
        before, after = self._commit_db(row["prev_id"]), self._commit_db(commit)
        meta, pks = self._columns(db), self._pks(db)
        meta_b, meta_a = self._columns(before), self._columns(after)
        self._x("BEGIN")
        try:
            for table, columns in meta.items():
                pk = pks.get(table)
                if not pk or (table not in meta_b and table not in meta_a):
                    continue
                cols = [c for c, _, _ in columns]
                types = {c: t for c, t, _ in columns}
                ddb.revert(self.conn.raw, _qt(db, table),
                           self._rel(before, table, cols, meta_b, types),
                           self._rel(after, table, cols, meta_a, types), cols, pk)
            self._x("COMMIT")
        except BaseException:
            self._x("ROLLBACK")
            raise
        self._add_commit(db, "commit", f"revert {commit}")

    # ------------------------------------------------------------------
    # Merge and rebase
    # ------------------------------------------------------------------

    def _reconcile_schema(self, ours_db: str, theirs_db: str) -> dict:
        """Give ours_db the tables and columns theirs_db has and it lacks.
        A differing primary key or column type is a schema conflict (raises
        before anything is changed)."""
        ours, theirs = self._columns(ours_db), self._columns(theirs_db)
        pk_o, pk_t = self._pks(ours_db), self._pks(theirs_db)
        add_tables, add_cols = [], []
        for table, columns in theirs.items():
            if table not in ours:
                add_tables.append(table)
                continue
            if pk_o.get(table, []) != pk_t.get(table, []):
                raise RuntimeError(f"schema conflict on {table}: primary keys differ "
                                   f"between {ours_db} and {theirs_db}")
            have = {c: t for c, t, _ in ours[table]}
            for col, dtype, default in columns:
                if col not in have:
                    add_cols.append((table, col, dtype, default))
                elif have[col] != dtype:
                    raise RuntimeError(f"schema conflict on {table}.{col}: {have[col]} in "
                                       f"{ours_db}, {dtype} in {theirs_db}")
        for table in add_tables:
            ddl = self._x("SELECT sql FROM duckdb_tables() WHERE database_name = ? "
                          "AND schema_name = 'main' AND table_name = ?", [theirs_db, table])[0][0]
            self._x(_CREATE_TABLE_RE.sub(f"CREATE TABLE {_qt(ours_db, table)}(", ddl, count=1))
            self._x(f"INSERT INTO {_qt(ours_db, table)} SELECT * FROM {_qt(theirs_db, table)}")
        for table, col, dtype, default in add_cols:
            self._x(f"ALTER TABLE {_qt(ours_db, table)} ADD COLUMN {_q(col)} {dtype}"
                    + (f" DEFAULT {default}" if default is not None else ""))
        # Views the source added are created from their definitions.
        ours_views = self._views(ours_db)
        add_views = []
        for view, ddl in self._views(theirs_db).items():
            if view not in ours_views and view not in ours:
                self._x(_CREATE_VIEW_RE.sub(f"CREATE VIEW {_qt(ours_db, view)} AS", ddl, count=1))
                add_views.append(view)
        return {"added_tables": add_tables,
                "added_columns": [f"{t}.{c}" for t, c, _, _ in add_cols],
                "added_views": add_views}

    def _three_way(self, ours_db: str, theirs_db: str, base_db, on_conflict, ref: Ref,
                   skip: list) -> dict:
        """Apply theirs' changes since base onto ours_db in one transaction."""
        raw = self.conn.raw
        meta_o, meta_t = self._columns(ours_db), self._columns(theirs_db)
        meta_b = self._columns(base_db) if base_db else {}
        pks = self._pks(ours_db)
        info = {"conflicts": 0, "conflict_tables": [], "ours_changes": 0, "theirs_changes": 0,
                "merged_tables": [], "skipped_tables": {}, "two_way": base_db is None}
        conflicts = []
        self._x(f"CREATE SCHEMA IF NOT EXISTS {_q(ours_db)}.{SCRATCH_SCHEMA}")
        scratch = ddb.Scratch(raw, f"{_q(ours_db)}.{SCRATCH_SCHEMA}")
        self._x("BEGIN")
        try:
            for table in sorted(set(meta_o) & set(meta_t)):
                if table in skip:
                    info["merged_tables"].append(table)  # copied whole
                    continue
                have_t = {c for c, _, _ in meta_t[table]}
                cols = [c for c, _, _ in meta_o[table] if c in have_t]
                types = {c: t for c, t, _ in meta_o[table]}
                O = _qt(ours_db, table)
                T = self._rel(theirs_db, table, cols, meta_t, types)
                B = self._rel(base_db, table, cols, meta_b, types) if base_db else \
                    self._rel(theirs_db, "\0none", cols, {}, types)
                # Skip tables neither side changed (one query instead of the
                # full three-way).
                t_changed, o_changed = self._x(
                    f"SELECT EXISTS (SELECT * FROM {T} EXCEPT SELECT * FROM {B}) "
                    f"OR EXISTS (SELECT * FROM {B} EXCEPT SELECT * FROM {T}), "
                    f"EXISTS (SELECT {ddb.cols_sql(None, cols)} FROM {O} EXCEPT SELECT * FROM {B}) "
                    f"OR EXISTS (SELECT * FROM {B} EXCEPT SELECT {ddb.cols_sql(None, cols)} FROM {O})")[0]
                info["ours_changes"] += int(bool(o_changed))
                if not t_changed:
                    continue
                pk = pks.get(table, [])
                r = ddb.three_way(raw, O, B, T, cols, pk, on_conflict, scratch)
                info["theirs_changes"] += int(r["theirs_changes"] > 0)
                info["merged_tables"].append(table)
                if r.get("no_primary_key"):
                    info["skipped_tables"][table] = "no primary key: new rows only"
                if r["conflicts"]:
                    info["conflicts"] += r["conflicts"]
                    info["conflict_tables"].append(table)
                    if r["rows"]:
                        conflicts.append({"table": table, "rows": r["rows"]})
            if conflicts:
                info["resolution"] = on_conflict(self._conflict_session(ref), conflicts)
                info["resolved"] = "custom"
            elif info["conflicts"]:
                info["resolved"] = on_conflict
            scratch.drop()
            self._x("COMMIT")
        except BaseException:
            self._x("ROLLBACK")
            raise
        return info

    def _merge_impl(self, into: Ref, source: Ref, message: str, on_conflict="ours") -> dict:
        ours_db = self._on(into)
        theirs_db = self._ref_db(source)
        theirs_name = self._db_for(source.branch)
        if theirs_name == ours_db:
            raise ValueError(f"Cannot merge branch '{source.branch}' into itself")
        base = self._merge_base(ours_db, theirs_db, ours_db, theirs_name)
        schema = self._reconcile_schema(ours_db, theirs_db)
        info = self._three_way(ours_db, theirs_db, base, on_conflict, into,
                               schema["added_tables"])
        self._copy_log(ours_db, theirs_db)
        info["hash"] = self._add_commit(
            ours_db, "merge", message or f"Merge {source} into {into}", parent=theirs_name)
        info["fast_forward"] = info["ours_changes"] == 0
        info["schema_changes"] = schema
        return info

    def _rebase_impl(self, ref: Ref, onto: Ref, on_conflict="ours") -> dict:
        branch_db = self._db_for(ref.branch)
        upstream = self._ref_db(onto)
        upstream_name = self._db_for(onto.branch)
        if upstream_name == branch_db:
            raise ValueError(f"Cannot rebase branch '{ref.branch}' onto itself")
        base = self._merge_base(branch_db, upstream, branch_db, upstream_name)
        # Freeze the upstream as it is now: the rebase commit's clone (the
        # branch's new fork point, so a later merge back sees only newer
        # changes), and the starting point of the rebased branch.
        commit_id = f"{time.time_ns():x}"
        frozen = self._commit_db(commit_id)
        self._clone(frozen, upstream)
        tmp = self._new_temp()
        self._clone(tmp, frozen)
        try:
            schema = self._reconcile_schema(tmp, branch_db)
            # The upstream is "ours" (as in Dolt); the branch's changes are
            # "theirs". A resolve callable runs on the clone.
            self._use(tmp)
            info = self._three_way(tmp, branch_db, base, on_conflict, ref, schema["added_tables"])
            self._copy_log(tmp, branch_db)
            self._use(branch_db)
            self._overwrite(branch_db, tmp)
        finally:
            self._drop_quietly(tmp)
        self._current_db = None
        self._on(ref)
        self._x(f"INSERT INTO {self._log(branch_db)} ({', '.join(_LOG_COLS)}) "
                f"SELECT ?, ?, 'rebase', ?, ?, (SELECT id FROM {self._log(branch_db)} "
                "ORDER BY seq DESC LIMIT 1), now()",
                [time.time_ns(), commit_id, f"rebase onto {onto}", upstream_name])
        info["up_to_date"] = info["ours_changes"] == 0
        info["schema_changes"] = schema
        return info

    def _delete_impl(self, ref: Ref) -> None:
        db = self._db_for(ref.branch)
        if db == self._current_db:
            self._use(self._db_for(self.default_branch))
            self._current_ref = Ref(self.default_branch)
        self._attach(db)
        self._x(f"DROP DATABASE {_q(db)}")
        self._attached.discard(db)

    def close_connection(self) -> None:
        if self.conn is not None:
            for db in list(self._temp_dbs.values()):
                self._drop_quietly(db)
            self._temp_dbs.clear()
        super().close_connection()
