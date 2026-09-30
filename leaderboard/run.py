"""Run the leaderboard suite on one system and publish one result file.

From the repository root:
    python -m leaderboard.run <system> --machine <label>

<system> must be a key of SYSTEMS, so the runner only loads adapters that live
in this repository. The result lands in <out>/<system>/results/<YYYYMMDD>/<machine>.json
only after cleanup succeeded. From the server start on, a failure publishes
{"error": ...} in its place, so a stale success never stays current. A preflight
failure publishes nothing.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import re
import secrets
import shutil
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import dblib.result_collector as rc
from dblib import dolt_mysql
from dblib.dolt import DOLT_DATA_DIR, DoltToolSuite, commit_dolt_schema
from microbench import task2_pb2 as tp
from microbench.operations.crud import (
    InsertOperation,
    RangeReadOperation,
    RangeUpdateOperation,
    ReadOperation,
    UpdateOperation,
)
from microbench.runner2 import (
    BackendInfo,
    BenchmarkConfig,
    SharedBranchManager,
    WorkerContext,
    register_all_operations,
)
from util.import_db import load_sql_file

ROOT = Path(__file__).resolve().parent.parent
BOARD = ROOT / "leaderboard"
RUNS = BOARD / ".runs"
SCHEMA_VERSION = 1
MACHINE_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}")
# What a run imports or reads. A change here, tracked or not, makes the run dirty.
CODE_PATHS = ["dblib", "microbench", "util", "leaderboard", "db_setup", "pyproject.toml", "requirements.lock"]
POINT_OPS = {"read": ReadOperation, "insert": InsertOperation, "update": UpdateOperation}
RANGE_OPS = {"range_read": RangeReadOperation, "range_update": RangeUpdateOperation}


class RunError(Exception):
    """The run cannot produce a valid result."""


class Doltgres:
    """Doltgres over the Postgres protocol: backend dolt."""

    backend = tp.Backend.DOLT
    needs_psql = True
    data_dir = Path(DOLT_DATA_DIR).expanduser()

    def _admin(self) -> psycopg2.extensions.connection:
        conn = psycopg2.connect(DoltToolSuite.get_default_connection_uri())
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def create_db(self, name: str) -> None:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {name}")

    def load(self, name: str, dump: Path) -> None:
        uri = DoltToolSuite.get_initial_connection_uri(name)
        load_sql_file(uri, dump)
        commit_dolt_schema(uri)

    def drop_db(self, name: str) -> None:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {name}")


class Dolt:
    """Dolt over the MySQL protocol: backend dolt_mysql."""

    backend = tp.Backend.DOLT_MYSQL
    needs_psql = False
    data_dir = Path(dolt_mysql.DOLT_MYSQL_DATA_DIR)

    def create_db(self, name: str) -> None:
        with closing(dolt_mysql.connect()) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {name}")

    def load(self, name: str, dump: Path) -> None:
        dolt_mysql.load_sql_dump(name, str(dump))
        with closing(dolt_mysql.connect(name)) as conn, conn.cursor() as cur:
            cur.execute("CALL DOLT_COMMIT('-Am', 'Load SQL schema')")

    def drop_db(self, name: str) -> None:
        dolt_mysql.drop_database(name)


SYSTEMS = {"dolt": Doltgres(), "dolt_mysql": Dolt()}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def lifecycle(sysdir: Path, step: str) -> bool:
    """Run a system's install, start, check, or stop script. Hosted systems have none."""
    script = sysdir / step
    if not script.exists():
        return True
    env = {**os.environ, "PYTHON": sys.executable}
    # check is polled while the server starts, so its failures stay quiet.
    out = subprocess.DEVNULL if step == "check" else None
    return subprocess.run([str(script)], env=env, stdout=out, stderr=out).returncode == 0


def start_server(sysdir: Path) -> None:
    if not (lifecycle(sysdir, "install") and lifecycle(sysdir, "start")):
        raise RunError(f"{sysdir.name}: install or start failed")
    deadline = time.monotonic() + 60
    while not lifecycle(sysdir, "check"):
        if time.monotonic() > deadline:
            raise RunError(f"{sysdir.name}: server failed its check for 60 s")
        time.sleep(0.5)


def journal(system: str) -> Path:
    """Names the database a run creates, from before its creation until its deletion."""
    return RUNS / f"{system}.journal.json"


def clean_up(system: str, server: Doltgres | Dolt) -> Exception | None:
    """Drop the database the journal names, and its dropped-database copy, then delete
    the journal. The journal stays if the drop fails. Reports instead of raising."""
    if not journal(system).exists():
        return None
    db = json.loads(journal(system).read_text())["database"]
    try:
        server.drop_db(db)
    except Exception as e:  # the caller turns this into the run's error record
        return e
    shutil.rmtree(server.data_dir / ".dolt_dropped_databases" / db, ignore_errors=True)
    journal(system).unlink()
    return None


def task_config(backend: int, db: str, suite: dict) -> BenchmarkConfig:
    task = tp.TaskConfig(run_id=db, backend=backend, table_name=suite["table"], autocommit=True, num_threads=1)
    task.database_setup.db_name = db
    # Validation wants one operation; every row brings its own operation and batch.
    task.operation_benchmark.operation = tp.OperationType.READ
    task.operation_benchmark.num_ops = 1
    task.operation_benchmark.range_config.range_size = suite["range_size"]
    return BenchmarkConfig(task)


def count_rows(tools, tables: dict[str, int]) -> dict[str, int]:
    return {t: tools.execute_sql(f"SELECT count(*) FROM {t}")[0][0] for t in tables}


def drop_branch(tools, name: str) -> None:
    """Delete a branch from main, whether or not the step that made it finished."""
    tools.connect_branch("main", timed=False)
    if name in tools.list_branches():
        tools.delete_branch(name, timed=False)


def verify(tools, expected: dict[str, int], main_id: str) -> None:
    """Row counts on main and on a new branch must match the frozen dump."""
    if (on_main := count_rows(tools, expected)) != expected:
        raise RunError(f"row counts on main differ from the dump: {on_main}")
    tools.create_branch("probe", parent_id=main_id, timed=False)
    tools.connect_branch("probe", timed=False)
    on_probe = count_rows(tools, expected)
    drop_branch(tools, "probe")
    if on_probe != expected:
        raise RunError(f"a new branch does not inherit the loaded data: {on_probe}")


def build_chain(tools, length: int, main_id: str) -> list[tuple[str, str]]:
    """A chain of branches, each created untimed from the one before."""
    chain, parent = [], main_id
    for i in range(1, length + 1):
        name = f"chain_{i}"
        tools.create_branch(name, parent_id=parent, timed=False)
        tools.connect_branch(name, timed=False)
        _, parent = tools.get_current_branch()
        chain.append((name, parent))
    tools.connect_branch("main", timed=False)
    return chain


def batch_total(collector: rc.ResultCollector, expected: int) -> float | None:
    """A try's cell: the summed timed calls, or None unless every call succeeded."""
    if collector.failed_operations or len(collector.results) != expected:
        return None
    return round(sum(r.latency for r in collector.results), 6)


# Each cell function below turns any failure into a null cell. That is the
# recovery: the try is recorded as failed, and its branch is removed.


def first_query(tools, name: str, tip_id: str, table: str) -> float | None:
    """Create, connect, and read one row, as one span."""
    try:
        start = time.perf_counter()
        tools.create_branch(name, parent_id=tip_id, timed=False)
        tools.connect_branch(name, timed=False)
        tools.execute_sql(f"SELECT * FROM {table} LIMIT 1", timed=False)
        return round(time.perf_counter() - start, 6)
    except Exception:
        return None
    finally:
        drop_branch(tools, name)


def timed_create(ctx: WorkerContext, name: str, tip_id: str) -> float | None:
    ctx.result_collector.reset()
    try:
        ctx.db_tools.create_branch(name, parent_id=tip_id, timed=True)
        return batch_total(ctx.result_collector, 1)
    except Exception:
        return None
    finally:
        drop_branch(ctx.db_tools, name)


def timed_connects(ctx: WorkerContext, chain: list[tuple[str, str]], batch: int) -> float | None:
    ctx.result_collector.reset()
    try:
        for k in range(batch):
            ctx.db_tools.connect_branch(chain[k % len(chain)][0], timed=True)
        return batch_total(ctx.result_collector, batch)
    except Exception:
        return None
    finally:
        ctx.db_tools.connect_branch("main", timed=False)


def data_row(ctx: WorkerContext, operation, name: str, tip_id: str, batch: int, tries: int) -> list[float | None]:
    """All tries of one data operation, on one fresh branch off the tip."""
    ctx.db_tools.create_branch(name, parent_id=tip_id, timed=False)
    ctx.db_tools.connect_branch(name, timed=False)
    ctx.clear_pk_cache()
    cells = []
    for _ in range(tries):
        ctx.result_collector.reset()
        try:
            for _ in range(batch):
                operation.execute(ctx)
            cells.append(batch_total(ctx.result_collector, batch))
        except Exception:
            cells.append(None)
    drop_branch(ctx.db_tools, name)
    return cells


def run_row(ctx: WorkerContext, row: dict, chain: list, suite: dict, max_batch: int | None) -> list[float | None]:
    op, tries, tip_id = row["op"], suite["tries"], chain[-1][1]
    batch = min(row["batch"], max_batch) if max_batch else row["batch"]
    if op == "time_to_first_query":
        return [first_query(ctx.db_tools, f"ttfq_{t}", tip_id, suite["table"]) for t in range(1, tries + 1)]
    if op == "branch_create":
        return [timed_create(ctx, f"create_{t}", tip_id) for t in range(1, tries + 1)]
    if op == "branch_connect":
        return [timed_connects(ctx, chain, batch) for _ in range(tries)]
    if op in POINT_OPS:
        operation = POINT_OPS[op](suite["table"])
    else:
        operation = RANGE_OPS[op](suite["table"], suite["range_size"])
    return data_row(ctx, operation, f"row_{op}", tip_id, batch, tries)


def measure(server: Doltgres | Dolt, db: str, suite: dict, max_batch: int | None, deadline: float) -> tuple[str, list]:
    """Verify the load, build the chain, and run every row. Returns the server version and the cells."""
    register_all_operations()
    collector = rc.ResultCollector(run_id=db, output_dir=str(RUNS))
    ctx = WorkerContext(task_config(server.backend, db, suite), BackendInfo(default_branch_name="main"),
                        0, suite["seed"], collector, SharedBranchManager(), None, [])
    with ctx:
        tools = ctx.db_tools
        version = tools.execute_sql("SELECT dolt_version()")[0][0]
        _, main_id = tools.get_current_branch()
        expected = suite["dataset"]["row_counts"]
        verify(tools, expected, main_id)
        chain = build_chain(tools, suite["chain_length"], main_id)
        cells = []
        for row in suite["rows"]:
            if time.monotonic() > deadline:
                raise RunError("the run passed its deadline")
            cells.append(run_row(ctx, row, chain, suite, max_batch))
        tools.connect_branch(chain[-1][0], timed=False)
        if (on_tip := count_rows(tools, expected)) != expected:
            raise RunError(f"row writes reached the chain's tip: {on_tip}")
        tools.connect_branch("main", timed=False)
        for name, _ in reversed(chain):  # leaf first
            tools.delete_branch(name, timed=False)
    return version, cells


def to_json(payload: dict) -> str:
    """Pretty JSON with one result row per line, as ClickBench writes it."""
    if "result" not in payload:
        return json.dumps(payload, indent=2) + "\n"
    head = json.dumps({k: v for k, v in payload.items() if k != "result"}, indent=2)[:-2]
    rows = ",\n".join(f"    {json.dumps(r)}" for r in payload["result"])
    return f'{head},\n  "result": [\n{rows}\n  ]\n}}\n'


def publish(out: Path, system: str, machine: str, day: dt.date, payload: dict) -> Path:
    path = out / system / "results" / day.strftime("%Y%m%d") / f"{machine}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    try:
        tmp.write_text(to_json(payload))
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)  # gone after a successful replace; a partial file otherwise
    return path


def run(system: str, machine: str, out: Path, max_batch: int | None, allow_dirty: bool, deadline_min: float) -> Path:
    if not MACHINE_RE.fullmatch(machine or ""):
        raise RunError(f"machine label {machine!r} must match {MACHINE_RE.pattern}")
    server, sysdir = SYSTEMS[system], BOARD / system
    meta = json.loads((sysdir / "system.json").read_text())
    suite_path = BOARD / "suite.json"
    suite = json.loads(suite_path.read_text())
    dump = ROOT / suite["dataset"]["path"]
    if sha256(dump) != suite["dataset"]["sha256"]:
        raise RunError(f"{dump} does not match the digest in suite.json")
    if server.needs_psql and shutil.which("psql") is None:
        raise RunError("psql is not on PATH")
    status = subprocess.run(["git", "-C", str(ROOT), "status", "--porcelain", "--", *CODE_PATHS],
                            capture_output=True, text=True, check=True)
    dirty = bool(status.stdout.strip())
    if dirty and not allow_dirty:
        raise RunError("the working tree has uncommitted changes")
    os.environ.setdefault("PGCONNECT_TIMEOUT", "10")

    RUNS.mkdir(parents=True, exist_ok=True)
    run_id = secrets.token_hex(4)
    db, started = f"bb_{run_id}", dt.datetime.now(dt.UTC)
    failure: Exception | None = None
    try:
        start_server(sysdir)
        if leftover := clean_up(system, server):
            raise RunError(f"cannot delete what an earlier run left behind: {leftover}")
        # Journaled before the create, so a kill at any later point leaves a record.
        journal(system).write_text(json.dumps({"run_id": run_id, "database": db}))
        t0 = time.perf_counter()
        server.create_db(db)
        provision_time = time.perf_counter() - t0
        t0 = time.perf_counter()
        server.load(db, dump)
        load_time = time.perf_counter() - t0
        version, cells = measure(server, db, suite, max_batch, time.monotonic() + deadline_min * 60)
    except Exception as e:  # recorded below as the run's error, after cleanup
        failure = e
    finally:
        cleanup_failure = clean_up(system, server)
        failure = failure or cleanup_failure
        lifecycle(sysdir, "stop")

    if failure:
        detail = (str(failure).splitlines() or [""])[0][:300]
        publish(out, system, machine, started.date(), {"error": f"{type(failure).__name__}: {detail}"})
        raise RunError(f"{system} failed: {detail}") from failure
    commit = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True, check=True)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "system": meta["system"],
        "date": started.date().isoformat(),
        "machine": machine,
        "proprietary": meta["proprietary"],
        "hosted": meta["hosted"],
        "tuned": meta["tuned"],
        "tags": meta["tags"],
        "version": version,
        "suite": suite["suite"],
        "suite_sha256": sha256(suite_path),
        "seed": suite["seed"],
        "commit": commit.stdout.strip(),
        "dirty": dirty,
        "dataset_sha256": suite["dataset"]["sha256"],
        "python": platform.python_version(),
        "lock_sha256": sha256(ROOT / "requirements.lock"),
        "provision_time": round(provision_time, 6),
        "load_time": round(load_time, 6),
    }
    if max_batch:
        payload["max_batch"] = max_batch
    payload["result"] = cells
    return publish(out, system, machine, started.date(), payload)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the leaderboard suite on one system.")
    parser.add_argument("system", choices=sorted(SYSTEMS))
    parser.add_argument("--machine", default=os.environ.get("machine"), help="machine label; defaults to $machine")
    parser.add_argument("--out", type=Path, default=BOARD, help="directory that receives <system>/results/")
    parser.add_argument("--max-batch", type=int, help="cap every batch for a short run; the validator rejects it")
    parser.add_argument("--allow-dirty", action="store_true", help="allow uncommitted changes; the validator rejects it")
    parser.add_argument("--deadline-min", type=float, default=120.0, help="stop the run after this many minutes")
    args = parser.parse_args()
    try:
        path = run(args.system, args.machine, args.out, args.max_batch, args.allow_dirty, args.deadline_min)
    except RunError as e:
        print(f"run failed: {e}", file=sys.stderr)
        return 1
    print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
