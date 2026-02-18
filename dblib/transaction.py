from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2.extensions import connection as _pgconn
from dblib.db_api import DBToolSuite
import dblib.result_collector as rc
from dblib import result_pb2 as rslt
import dblib.util as dbutil
from microbench import task_pb2 as tp

PGSQL_USER = "postgres"
PGSQL_PASSWORD = "password"
PGSQL_HOST = "localhost"
PGSQL_PORT = 5432

class TxnToolSuite(DBToolSuite):
    """
    A suite of tools for interacting with a PGSQL database on a shared connection.
    Uses a single persistent transaction and Postgres SAVEPOINTs to simulate
    branching. Uses raw SQL to manage transaction instead of psycopg2 semantics
    to better control behavior.
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
        return cls(
            connection=conn,
            collector=collector,
            autocommit=autocommit,
            setup_branches=setup_branches,
            default_branch_name=default_branch_name,
        )

    def __init__(
        self,
        connection: _pgconn,
        collector: rc.ResultCollector,
        autocommit: bool,
        setup_branches: list,
        default_branch_name: str,
    ):
        super().__init__(connection, result_collector=collector)
        self.autocommit = False # Cannot commit during benchmark

        self._save_points = SavePoints(setup_branches)
        self._create_branch_impl(default_branch_name)
        self._connect_branch_impl(default_branch_name)

    def list_branches(self) -> list[str]:
        return self._save_points.l

    def _create_branch_impl(self, branch_name: str, parent_id: str = None) -> None:
        """
        Creates a new SAVEPOINT in the PGSQL database transaction.
        """
        if parent_id and parent_id != self._save_points[-1]:
            raise Exception("Tried to branch from earlier save point, spine shape only allowed")
        cmd = f"SAVEPOINT {branch_name};"
        # Allow exceptions to percolate up
        cur = self.conn.cursor()
        cur.execute(cmd)
        cur.close()
        self._save_points.append(branch_name)

    def connect_specific_branch(self, op: int) -> None: #TODO type hint
        """
        Connects to an existing branch to allow reading and writing data to that
        branch. Return a bool indicating whether the operation was successful.
        """
        timed = True
        branch = ""
        op_type = 0
        if op == tp.OperationType.CONNECT_FIRST:
            branch = self._save_points[0]
            op_type = rslt.OpType.CONNECT_FIRST
        elif op == tp.OperationType.CONNECT_MID:
            branch = self._save_points[int(len(self._save_points) / 2)]
            op_type = rslt.OpType.CONNECT_MID
        elif op == tp.OperationType.CONNECT_LAST:
            branch = self._save_points[len(self._save_points) - 1] 
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
        """
        Connects to an existing branch in the PGSQL database to allow reads and
        writes on that branch. With this backend, that means rolling back to 
        a previous SAVEPOINT.
        """
        if branch_name not in self._save_points:
            return # No-op when trying to roll forward
        cmd = f"ROLLBACK TO SAVEPOINT {branch_name};"
        # Allow exceptions to percolate up
        cur = self.conn.cursor()
        cur.execute(cmd)
        cur.close()
        # Only truncate if there were no exceptions
        self._save_points.truncate(branch_name)

    def _get_current_branch_impl(self) -> tuple[str, str]:
        return (self._save_points[-1], self._save_points[-1])

    def delete_db(self, db_name: str) -> None:
        conn.commit()
        super().delete_db(db_name)

    def commit_changes(self, timed: bool = False, message: str = "") -> None:
        """ Override to a no-op, we have only one persistent transaction """
        pass

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
