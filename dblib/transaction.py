from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
import dblib.result_collector as rc
from dblib import result_pb2 as rslt
import dblib.util as dbutil
from microbench import task_pb2 as tp

PGSQL_USER = "elaineang"
PGSQL_PASSWORD = "password"
PGSQL_HOST = "localhost"
PGSQL_PORT = 5433

class TxnToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a PGSQL database using
    connection-per-root-branch concurrency.

    Root-level branches get their own PG connection (with an implicit open
    transaction), so multiple workers can operate concurrently without
    interleaving SAVEPOINTs. Sub-branches (children of non-root nodes) still
    use SAVEPOINTs within their root-ancestor's connection.
    """

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return dbutil.format_db_uri(
            PGSQL_USER, PGSQL_PASSWORD, PGSQL_HOST, PGSQL_PORT, "postgres"
        )

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return dbutil.format_db_uri(
            PGSQL_USER, PGSQL_PASSWORD, PGSQL_HOST, PGSQL_PORT, db_name
        )

    @classmethod
    def get_connection(cls, cur_conn: _pgconn, db_name: str) -> _pgconn:
        """ Returns a psycopg2 connection object with an open transaction """
        if cur_conn and not cur_conn.closed:
            return cur_conn
        uri = cls.get_initial_connection_uri(db_name)
        conn = None
        try:
            conn = psycopg2.connect(uri)
        except Exception as e:
            raise Exception(f"Failed to open initial connection: {e}")
        finally:
            return conn

    @classmethod
    def init_for_bench(
        cls,
        collector: rc.ResultCollector,
        db_name: str,
        autocommit: bool,
        default_branch_name: str,
        setup_branches: list,
        conn: _pgconn = None
    ):
        if conn is None:
            conn = cls.get_connection(None, db_name)
        return cls(
            connection=conn,
            collector=collector,
            autocommit=autocommit,
            setup_branches=setup_branches,
            default_branch_name=default_branch_name,
            db_name=db_name,
        )

    def __init__(
        self,
        connection: _pgconn,
        collector: rc.ResultCollector,
        autocommit: bool,
        setup_branches: list,
        default_branch_name: str,
        db_name: str = "",
    ):
        super().__init__(connection, result_collector=collector)
        self.autocommit = False  # Cannot commit during benchmark
        self._db_name = db_name
        self._root_name = default_branch_name
        self._current_branch = default_branch_name
        # Map branch name -> the PG connection it lives on
        self._branch_conn = {default_branch_name: connection}
        # Ordered list of all branches (for microbench connect_specific_branch)
        self._all_branches = list(setup_branches) if setup_branches else []
        if default_branch_name not in self._all_branches:
            self._all_branches.append(default_branch_name)

    def list_branches(self) -> list[str]:
        return list(self._all_branches)

    def _open_new_connection(self) -> _pgconn:
        """Open a new PG connection to the same database.

        The connection starts with an implicit open transaction (psycopg2
        default when autocommit is off), which is what we want — it
        simulates a branch fork from the committed state.
        """
        uri = self.get_initial_connection_uri(self._db_name)
        return psycopg2.connect(uri)

    def _create_branch_impl(self, branch_name: str, parent_id: str = None) -> None:
        """Create a new branch.

        If branching from root (or parent_id is None), open a brand-new PG
        connection so the branch can run concurrently with others.
        If branching from a non-root node, create a SAVEPOINT within the
        parent's connection.
        """
        if parent_id is None or parent_id == self._root_name:
            # Root-level branch: new connection with its own transaction
            conn = self._open_new_connection()
            self._branch_conn[branch_name] = conn
        else:
            # Sub-branch: savepoint in parent's connection
            parent_conn = self._branch_conn.get(parent_id)
            if parent_conn is None:
                raise Exception(f"No connection for parent '{parent_id}'")
            cur = parent_conn.cursor()
            cur.execute(f"SAVEPOINT {branch_name}")
            cur.close()
            self._branch_conn[branch_name] = parent_conn
        self._all_branches.append(branch_name)

    def connect_specific_branch(self, op: rslt.OpType) -> None:
        """
        Connects to an existing branch to allow reading and writing data to that
        branch. Return a bool indicating whether the operation was successful.
        """
        timed = True
        branch = ""
        op_type = 0
        if op == tp.OperationType.CONNECT_FIRST:
            branch = self._all_branches[0]
            op_type = rslt.OpType.CONNECT_FIRST
        elif op == tp.OperationType.CONNECT_MID:
            branch = self._all_branches[int(len(self._all_branches) / 2)]
            op_type = rslt.OpType.CONNECT_MID
        elif op == tp.OperationType.CONNECT_LAST:
            branch = self._all_branches[len(self._all_branches) - 1]
            op_type = rslt.OpType.CONNECT_LAST
        try:
            with self.result_collector.maybe_time_ops(
                op_type=op_type, timed=timed
            ):
                self._connect_branch_impl(branch)
        except Exception as e:
            raise Exception(f"Error connecting to branch: {e}")
        if timed:
            self.result_collector.record_num_keys_touched(0)
            self.result_collector.flush_record()

    def _connect_branch_impl(self, branch_name: str) -> None:
        """Switch to a branch's connection.

        For root-level branches this simply swaps self.conn.
        No ROLLBACK TO SAVEPOINT is needed for the primary macrobench
        use case (flat star topology).
        """
        conn = self._branch_conn.get(branch_name)
        if conn is None:
            return  # Unknown branch, no-op
        self.conn = conn
        self._current_branch = branch_name

    def _get_current_branch_impl(self) -> tuple[str, str]:
        return (self._current_branch, self._current_branch)

    def _delete_branch_impl(self, branch_name: str, branch_id: str) -> None:
        """Delete a branch.

        For root-level branches (those with their own connection), rollback
        and close the connection.  For sub-branches (savepoints), just
        remove tracking — the savepoint is implicitly released when the
        parent connection is closed.
        """
        conn = self._branch_conn.pop(branch_name, None)
        if conn is None:
            return
        if branch_name in self._all_branches:
            self._all_branches.remove(branch_name)
        # Root-level branch: rollback and close the dedicated connection
        root_conn = self._branch_conn.get(self._root_name)
        if conn is not root_conn:
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
        # else: sub-branch savepoint — connection is shared, just remove tracking

    def delete_db(self, db_name: str) -> None:
        self.conn.commit()
        super().delete_db(db_name)

    def commit_changes(self, timed: bool = False, message: str = "") -> None:
        """ Override to a no-op, we have only one persistent transaction """
        pass

    def close_connection(self) -> None:
        """Close ALL connections managed by this tool suite."""
        closed = set()
        for name, conn in self._branch_conn.items():
            if id(conn) not in closed and conn and not conn.closed:
                try:
                    conn.rollback()
                    conn.close()
                except Exception:
                    pass
                closed.add(id(conn))
        self._branch_conn.clear()
        self.conn = None

    def get_total_storage_bytes(self) -> int:
        """Get total storage for the database.

        Since TXN backend uses a single database for all branches,
        we simply return the size of the current database.
        """
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT pg_database_size(current_database());")
            return cur.fetchone()[0]
        finally:
            cur.close()

class SavePoints(object):
    """ A simple container for cheap membership testing of an ordered list """
    def __init__(self, items: list):
        self.s = set()
        if items:
            self.l = items[:]
            self.s.update(self.l)
        else:
            self.l = list()

    def __contains__(self, item):
        return item in self.s

    def append(self, item):
        if item in self.s:
            return
        self.l.append(item)
        self.s.add(item)

    def __getitem__(self, key):
        return self.l[key]

    def truncate(self, key):
        if key not in self.s:
            return
        self.l = self.l[:self.l.index(key)+1]
        self.s = set()
        self.s.update(self.l)

    def __len__(self):
        return len(self.l)


def txn_id(conn):
    cur = conn.cursor()
    cur.execute("select txid_current()")
    return cur.fetchone()
