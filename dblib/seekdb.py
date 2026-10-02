"""SeekDB backend.

SeekDB (OceanBase) is MySQL-compatible, but it branches by forking whole
databases rather than keeping named branches inside one database. Each of our
branches is therefore its own database:

    main branch      -> <db_name>
    branch "X"       -> <db_name>__X

The mapping is derived from the names alone, so every worker (each with its own
connection) resolves the same branch to the same database with no shared state.
A branch's ID is its database name.

    create_branch    FORK DATABASE <parent db> TO <branch db>, then USE it
    connect_branch   USE <branch db>
    merge_branch     MERGE TABLE <src db>.t INTO <current db>.t STRATEGY OURS,
                     for each table
    delete_branch    DROP DATABASE <branch db>

Merging differs from Dolt: it is per table, in effect insert-only (the
source's updates and deletes aren't merged), merges no schema changes and needs
a primary key. See SeekDBToolSuite._merge_branch_impl.
"""

import os

import pymysql
from pymysql.constants import CLIENT

from dblib.db_api import DBToolSuite
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


def _branch_databases(cur, db_name: str) -> list[str]:
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
    """
    A suite of tools for interacting with SeekDB on a shared connection, where
    each branch is a forked database (see the module docstring).
    """

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return cls.get_initial_connection_uri("")

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return (
            f"mysql://{SEEKDB_USER}:{SEEKDB_PASSWORD}"
            f"@{SEEKDB_HOST}:{SEEKDB_PORT}/{db_name}"
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
        db_name: str,
    ):
        super().__init__(connection, result_collector=collector)
        # Root database; every branch database name is derived from it.
        self.db_name = db_name
        self.autocommit = autocommit
        # Branch and database the connection is on, tracked client-side so
        # get_current_branch() needs no round trip (it runs before most ops).
        self._current_branch = None
        self._current_db = None
        # (table, reason) pairs already warned about, so a table skipped on
        # every merge is reported once.
        self._merge_skip_warned = set()
        self._connect_branch_impl(default_branch_name)

    # ------------------------------------------------------------------
    # Branch name <-> database name
    # ------------------------------------------------------------------

    def _db_for(self, branch: str) -> str:
        """Database for a branch name or branch ID (a database name)."""
        if branch == self.db_name or branch.startswith(self.db_name + _BRANCH_SEP):
            return branch
        return branch_db_name(self.db_name, branch)

    def _branch_for(self, db: str) -> str:
        """Branch name held in database db."""
        if db == self.db_name:
            return MAIN_BRANCH
        return db[len(self.db_name + _BRANCH_SEP):]

    def _use(self, db: str) -> None:
        super().execute_sql(f"USE {_quote(db)};")
        self._current_db = db
        self._current_branch = self._branch_for(db)

    # ------------------------------------------------------------------
    # DBToolSuite hooks
    # ------------------------------------------------------------------

    def _get_table_columns(self, table_name: str) -> list[tuple]:
        rows = super().execute_sql(mysql_common.TABLE_COLUMNS_QUERY, (table_name,))
        return mysql_common.normalize_column_types(rows)

    def list_branches(self) -> list[str]:
        with self.conn.cursor() as cur:
            branch_dbs = _branch_databases(cur, self.db_name)
        return [MAIN_BRANCH] + [self._branch_for(db) for db in branch_dbs]

    def _create_branch_impl(
        self, branch_name: str, parent_id: str = None
    ) -> None:
        """Fork the parent's database (the current one if parent_id is not
        given) and switch to the fork, like Dolt's checkout -b."""
        source_db = self._db_for(parent_id) if parent_id else self._current_db
        new_db = branch_db_name(self.db_name, branch_name)
        super().execute_sql(
            f"FORK DATABASE {_quote(source_db)} TO {_quote(new_db)};"
        )
        self._use(new_db)

    def _connect_branch_impl(self, branch_name: str) -> None:
        self._use(self._db_for(branch_name))

    def _get_current_branch_impl(self) -> tuple[str, str]:
        # The database name is unique, so it serves as the branch ID.
        return (self._current_branch, self._current_db)

    def _merge_branch_impl(self, source_branch: str, message: str = "") -> dict:
        """Merge source_branch into the current branch, one table at a time.

        DRAFT. SeekDB's merge differs from Dolt's, so results aren't directly
        comparable:

        - Per table: SeekDB has no database-level merge, so each table is
          merged with its own MERGE TABLE statement, in name order. The merge
          as a whole isn't atomic. If a later table fails, earlier tables
          stay merged.
        - Insert only, in effect: MERGE TABLE compares the two tables
          directly, with no common ancestor, so it can't tell which side
          changed a row. Every row that differs is a conflict, and STRATEGY
          OURS keeps the current branch's version (like dolt_mysql's --ours
          resolution). The result is that only rows whose primary key is
          missing from the target are added: updates made on the source are
          dropped even when the target never touched the row, and rows
          deleted on the source stay. (STRATEGY THEIRS would apply the
          source's updates but also undo the target's own changes.)
        - SeekDB doesn't report how many conflicts it resolved, so
          "conflicts" is None.
        - No schema merge: tables that exist only on the source aren't
          created on the target, and MERGE TABLE refuses (error 4029) when
          the two tables' columns or primary key differ. Such tables are
          skipped with a warning.
        - Needs a primary key: tables without one (e.g. CH's history) are
          skipped with a warning.
        - Not transactional: each MERGE TABLE takes effect at once, even
          with autocommit off; a later rollback doesn't undo it.
        - SeekDB has no commits, so message is unused and "hash" is "".

        The timed MERGE op also includes the two information_schema queries
        that list each side's tables.

        Returns {"fast_forward": False, "conflicts": None, "hash": "",
                 "merged_tables": [table, ...],
                 "skipped_tables": {table: reason}}.
        """
        source_db = self._db_for(source_branch)
        target_db = self._current_db
        if source_db == target_db:
            raise ValueError(f"Cannot merge branch '{source_branch}' into itself")

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
        """MERGE TABLE source_db.table INTO target_db.table, keeping the
        target's rows on conflict. Returns False if SeekDB refuses because
        the two tables' schemas differ.

        Runs on the cursor directly, rather than through execute_sql, which
        re-raises every error as a plain Exception and loses the error code.
        """
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

    def _tables_with_pk_flag(self, db: str) -> dict[str, bool]:
        """Map each base table in db to whether it has a primary key."""
        rows = super().execute_sql(
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

    def _delete_branch_impl(self, branch_name: str, branch_id: str) -> None:
        """Drop the branch's database; must NOT be on the branch being deleted.

        Like Dolt, main can be deleted too: forks don't depend on the database
        they were forked from, and drop_database() still finds them by name.
        """
        db = self._db_for(branch_id or branch_name)
        if db == self._current_db:
            raise ValueError(f"Cannot delete the current branch '{branch_name}'")
        super().execute_sql(f"DROP DATABASE {_quote(db)};")

    def get_total_storage_bytes(self) -> int:
        """Get total storage by measuring the SeekDB server's data directory.

        SeekDB keeps all databases in one store, with no per-database
        directory, so this covers the whole server.
        """
        return dbutil.get_directory_size_bytes(SEEKDB_DATA_DIR)
