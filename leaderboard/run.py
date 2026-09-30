"""Run the leaderboard suite on one system and publish one result file.

From the repository root:
    python -m leaderboard.run <system> --machine <label>

<system> must be a key of SYSTEMS, so the runner only loads adapters that live
in this repository. The result lands in <out>/<system>/results/<YYYYMMDD>/<machine>.json
only after cleanup succeeded. From the server start on, a failure publishes
{"error": ...} in its place, so a stale success never stays current. A preflight
failure publishes nothing.

Hosted systems read their API keys from the environment, set at launch from
Bitwarden. Everything a run creates is named after its database, bb_<run id>,
and cleanup deletes by that name, so even a killed run leaves nothing behind
once the next run of that system starts.
"""

from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import json
import os
import platform
import re
import secrets
import shutil
import statistics
import subprocess
import sys
import time
from contextlib import closing, contextmanager
from pathlib import Path

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import dblib.result_collector as rc
from dblib import dolt_mysql
from dblib.dolt import DOLT_DATA_DIR, DoltToolSuite, commit_dolt_schema
from dblib.neon import NeonToolSuite
from dblib.tiger import TigerToolSuite
from dblib.xata import XataToolSuite
from leaderboard.validate import BOARD, MACHINE_RE, SCHEMA_VERSION, sha256
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

ROOT = BOARD.parent
RUNS = BOARD / ".runs"
# What a run imports or reads. A change here, tracked or not, makes the run dirty.
CODE_PATHS = ["dblib", "microbench", "util", "leaderboard", "db_setup", "pyproject.toml", "requirements.lock"]
POINT_OPS = {"read": ReadOperation, "insert": InsertOperation, "update": UpdateOperation}
RANGE_OPS = {"range_read": RangeReadOperation, "range_update": RangeUpdateOperation}
LOAD_TIMEOUT = 900  # seconds for psql to load the dump
VENDOR_TIMEOUT = 1200  # seconds for a hosted service to become ready, or to disappear after a delete


class RunError(Exception):
    """The run cannot produce a valid result."""


class Doltgres:
    """Doltgres over the Postgres protocol: backend dolt."""

    backend = tp.Backend.DOLT
    needs_psql = True
    version_sql = "SELECT dolt_version()"
    data_dir = Path(DOLT_DATA_DIR).expanduser()

    def _admin(self) -> psycopg2.extensions.connection:
        conn = psycopg2.connect(DoltToolSuite.get_default_connection_uri())
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn

    def provision(self, db: str) -> BackendInfo:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {db}")
        return BackendInfo(default_branch_name="main")

    def load(self, info: BackendInfo, db: str, dump: Path) -> None:
        uri = DoltToolSuite.get_initial_connection_uri(db)
        load_sql_file(uri, dump, timeout=LOAD_TIMEOUT)
        commit_dolt_schema(uri)

    def delete(self, db: str) -> None:
        with closing(self._admin()) as conn, conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
        shutil.rmtree(self.data_dir / ".dolt_dropped_databases" / db, ignore_errors=True)


class Dolt:
    """Dolt over the MySQL protocol: backend dolt_mysql."""

    backend = tp.Backend.DOLT_MYSQL
    needs_psql = False
    version_sql = "SELECT dolt_version()"
    data_dir = Path(dolt_mysql.DOLT_MYSQL_DATA_DIR)

    def provision(self, db: str) -> BackendInfo:
        with closing(dolt_mysql.connect()) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {db}")
        return BackendInfo(default_branch_name="main")

    def load(self, info: BackendInfo, db: str, dump: Path) -> None:
        dolt_mysql.load_sql_dump(db, str(dump))
        with closing(dolt_mysql.connect(db)) as conn, conn.cursor() as cur:
            cur.execute("CALL DOLT_COMMIT('-Am', 'Load SQL schema')")

    def delete(self, db: str) -> None:
        dolt_mysql.drop_database(db)
        shutil.rmtree(self.data_dir / ".dolt_dropped_databases" / db, ignore_errors=True)


def create_database(uri: str, db: str) -> None:
    """Create db on the server that uri points at."""
    with closing(psycopg2.connect(uri)) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {db}")


def wait_until(done, what: str) -> None:
    deadline = time.monotonic() + VENDOR_TIMEOUT
    while not done():
        if time.monotonic() > deadline:
            raise RunError(f"{what} after {VENDOR_TIMEOUT} s")
        time.sleep(1)


class Neon:
    """Neon: one project per run, named after the run's database."""

    backend = tp.Backend.NEON
    needs_psql = True
    version_sql = "SHOW server_version"
    keys = ("NEON_API_KEY_ORG",)
    region = "aws-us-east-1"
    # What the adapter requests: dblib/neon.py create_neon_project and _create_branch_impl.
    service = {"region": region, "pg_version": 17, "endpoint_type": "read_write"}

    def preflight(self) -> None:
        NeonToolSuite._request("GET", "projects", params={"limit": 1})

    def provision(self, db: str) -> BackendInfo:
        created = NeonToolSuite.create_neon_project(db, region_id=self.region)
        create_database(created["connection_uris"][0]["connection_uri"], db)
        return BackendInfo(neon_project_id=created["project"]["id"], default_branch_id=created["branch"]["id"],
                           default_branch_name=created["branch"]["name"])

    def load(self, info: BackendInfo, db: str, dump: Path) -> None:
        uri = NeonToolSuite._get_neon_connection_uri(info.neon_project_id, info.default_branch_id, db)
        load_sql_file(uri, dump, timeout=LOAD_TIMEOUT)

    def _projects(self, db: str) -> list[dict]:
        found = NeonToolSuite._request("GET", "projects", params={"search": db})["projects"]
        return [p for p in found if p["name"] == db]

    def delete(self, db: str) -> None:
        for project in self._projects(db):
            NeonToolSuite._request("DELETE", f"projects/{project['id']}")
        wait_until(lambda: not self._projects(db), f"Neon project {db} is still listed")


class Tiger:
    """Tiger Cloud: a root service per run and one forked service per branch,
    inside the account's project. The forks are named <root>_<branch>."""

    backend = tp.Backend.TIGER
    needs_psql = True
    version_sql = "SHOW server_version"
    keys = ("TIGER_ACCESS_KEY", "TIGER_SECRET_KEY", "TIGER_PROJECT_ID")
    region = "us-east-1"
    # What the adapter requests: dblib/tiger.py create_tiger_service and _create_branch_impl.
    service = {"region": region, "cpu_millis": 1000, "memory_gbs": 4}

    @staticmethod
    def _project() -> str:
        return os.environ["TIGER_PROJECT_ID"]  # checked with the other keys before the run

    def preflight(self) -> None:
        TigerToolSuite.list_tiger_services(self._project())

    def provision(self, db: str) -> BackendInfo:
        created = TigerToolSuite.create_tiger_service(db, project_id=self._project(), region_code=self.region,
                                                      cpu_millis=self.service["cpu_millis"],
                                                      memory_gbs=self.service["memory_gbs"])
        ready = TigerToolSuite.wait_for_service(created["project_id"], created["service_id"], timeout=VENDOR_TIMEOUT)
        password, host, port = created["initial_password"], ready["endpoint"]["host"], ready["endpoint"]["port"]
        info = BackendInfo(default_branch_id=created["service_id"], default_branch_name=created["name"],
                           default_uri=f"postgresql://tsdbadmin:{password}@{host}:{port}/tsdb")
        info.tiger = {"project_id": created["project_id"], "service_id": created["service_id"],
                      "service_name": created["name"], "password": password, "region": created["region_code"],
                      "services": {}}
        return info

    def load(self, info: BackendInfo, db: str, dump: Path) -> None:
        load_sql_file(info.default_uri, dump, timeout=LOAD_TIMEOUT)

    def _services(self, db: str) -> list[dict]:
        listed = TigerToolSuite.list_tiger_services(self._project())
        return [s for s in listed if s["name"] == db or s["name"].startswith(f"{db}_")]

    def delete(self, db: str) -> None:
        for service in sorted(self._services(db), key=lambda s: s["name"] == db):  # the root last
            TigerToolSuite.delete_tiger_service(self._project(), service["service_id"])
        wait_until(lambda: not self._services(db), f"Tiger services named {db} are still listed")


class Xata:
    """Xata: one project per run, named after the run's database."""

    backend = tp.Backend.XATA
    needs_psql = True
    version_sql = "SHOW server_version"
    keys = ("XATA_API_KEY", "XATA_ORGANIZATION_ID")
    region = "us-east-1"
    # What the adapter requests: dblib/xata.py create_xata_project.
    service = {"region": region, "instance_type": "xata.medium", "image": "postgres:18.0", "replicas": 0,
               "scale_to_zero_minutes": 30}

    def preflight(self) -> None:
        XataToolSuite._request("GET", "projects")

    def provision(self, db: str) -> BackendInfo:
        project_id, branch_id, branch_name, uri = XataToolSuite.create_xata_project(db, region=self.region)
        create_database(uri, db)
        return BackendInfo(xata_project_id=project_id, default_branch_id=branch_id, default_branch_name=branch_name)

    def load(self, info: BackendInfo, db: str, dump: Path) -> None:
        uri = XataToolSuite._get_xata_connection_uri(info.xata_project_id, info.default_branch_id, db)
        load_sql_file(uri, dump, timeout=LOAD_TIMEOUT)

    def _projects(self, db: str) -> list[dict]:
        return [p for p in XataToolSuite._request("GET", "projects")["projects"] if p["name"] == db]

    def delete(self, db: str) -> None:
        for project in self._projects(db):
            XataToolSuite.delete_project(project["id"])
        wait_until(lambda: not self._projects(db), f"Xata project {db} is still listed")


SYSTEMS = {"dolt": Doltgres(), "dolt_mysql": Dolt(), "neon": Neon(), "tiger": Tiger(), "xata": Xata()}
System = Doltgres | Dolt | Neon | Tiger | Xata


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


@contextmanager
def exclusive(system: str):
    """Hold a per-system lock, so two runs never share a journal or a server."""
    RUNS.mkdir(parents=True, exist_ok=True)
    with (RUNS / f"{system}.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RunError(f"another run of {system} is in progress") from None
        yield


def journal(system: str) -> Path:
    """Names what a run creates, from before its creation until its deletion."""
    return RUNS / f"{system}.journal.json"


def clean_up(system: str, server: System) -> Exception | None:
    """Delete everything named after the database the journal names, then delete
    the journal. The journal stays if the delete fails. Reports instead of raising."""
    if not journal(system).exists():
        return None
    try:  # an unreadable journal fails the run too, and stays for a person to inspect
        server.delete(json.loads(journal(system).read_text())["name"])
    except Exception as e:  # the caller turns this into the run's error record
        return e
    journal(system).unlink()
    return None


def scrub(text: str) -> str:
    """Mask the password of any connection URI, so no message or record carries one."""
    return re.sub(r"(://[^:/@\s]+):[^@/\s]+@", r"\1:***@", text)


def task_config(backend: int, db: str, suite: dict) -> BenchmarkConfig:
    task = tp.TaskConfig(run_id=db, backend=backend, table_name=suite["table"], autocommit=True, num_threads=1)
    task.database_setup.db_name = db
    # Validation wants one operation; every row brings its own operation and batch.
    task.operation_benchmark.operation = tp.OperationType.READ
    task.operation_benchmark.num_ops = 1
    task.operation_benchmark.range_config.range_size = suite["range_size"]
    return BenchmarkConfig(task)


def root(ctx: WorkerContext) -> str:
    """The branch a run starts on: main, a default branch, or Tiger's root service."""
    return ctx.backend_info.default_branch_name


def count_rows(tools, tables: dict[str, int]) -> dict[str, int]:
    return {t: tools.execute_sql(f"SELECT count(*) FROM {t}")[0][0] for t in tables}


def round_trip_ms(tools) -> float:
    """The median of 20 SELECT 1 round trips: the network cost every hosted operation pays."""
    times = []
    for _ in range(20):
        start = time.perf_counter()
        tools.execute_sql("SELECT 1")
        times.append(time.perf_counter() - start)
    return round(statistics.median(times) * 1000, 3)


def drop_branch(ctx: WorkerContext, name: str) -> None:
    """Delete a branch from the root, whether or not the step that made it finished."""
    ctx.db_tools.connect_branch(root(ctx), timed=False)
    if name in ctx.db_tools.list_branches():
        ctx.db_tools.delete_branch(name, timed=False)


def verify(ctx: WorkerContext, expected: dict[str, int], root_id: str) -> None:
    """Row counts on the root and on a new branch must match the frozen dump."""
    tools = ctx.db_tools
    if (on_root := count_rows(tools, expected)) != expected:
        raise RunError(f"row counts on the root differ from the dump: {on_root}")
    tools.create_branch("probe", parent_id=root_id, timed=False)
    tools.connect_branch("probe", timed=False)
    on_probe = count_rows(tools, expected)
    drop_branch(ctx, "probe")
    if on_probe != expected:
        raise RunError(f"a new branch does not inherit the loaded data: {on_probe}")


def build_chain(ctx: WorkerContext, length: int, root_id: str) -> list[tuple[str, str]]:
    """A chain of branches, each created untimed from the one before."""
    tools, chain, parent = ctx.db_tools, [], root_id
    for i in range(1, length + 1):
        name = f"chain_{i}"
        tools.create_branch(name, parent_id=parent, timed=False)
        tools.connect_branch(name, timed=False)
        _, parent = tools.get_current_branch()
        chain.append((name, parent))
    tools.connect_branch(root(ctx), timed=False)
    return chain


def batch_total(collector: rc.ResultCollector, expected: int) -> float | None:
    """A try's cell: the summed timed calls, or None unless every call succeeded."""
    if collector.failed_operations or len(collector.results) != expected:
        return None
    return round(sum(r.latency for r in collector.results), 6)


# Each cell function below turns any failure into a null cell. That is the
# recovery: the try is recorded as failed, and its branch is removed.


def first_query(ctx: WorkerContext, name: str, tip_id: str, table: str) -> float | None:
    """Create, connect, and read one row, as one span."""
    tools = ctx.db_tools
    try:
        start = time.perf_counter()
        tools.create_branch(name, parent_id=tip_id, timed=False)
        tools.connect_branch(name, timed=False)
        tools.execute_sql(f"SELECT * FROM {table} LIMIT 1", timed=False)
        return round(time.perf_counter() - start, 6)
    except Exception:
        return None
    finally:
        drop_branch(ctx, name)


def timed_create(ctx: WorkerContext, name: str, tip_id: str) -> float | None:
    ctx.result_collector.reset()
    try:
        ctx.db_tools.create_branch(name, parent_id=tip_id, timed=True)
        return batch_total(ctx.result_collector, 1)
    except Exception:
        return None
    finally:
        drop_branch(ctx, name)


def timed_connects(ctx: WorkerContext, chain: list[tuple[str, str]], batch: int) -> float | None:
    ctx.result_collector.reset()
    try:
        for k in range(batch):
            ctx.db_tools.connect_branch(chain[k % len(chain)][0], timed=True)
        return batch_total(ctx.result_collector, batch)
    except Exception:
        return None
    finally:
        ctx.db_tools.connect_branch(root(ctx), timed=False)


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
    drop_branch(ctx, name)
    return cells


def run_row(ctx: WorkerContext, row: dict, chain: list, suite: dict, max_batch: int | None) -> list[float | None]:
    op, tries, tip_id = row["op"], suite["tries"], chain[-1][1]
    batch = min(row["batch"], max_batch) if max_batch else row["batch"]
    if op == "time_to_first_query":
        return [first_query(ctx, f"ttfq_{t}", tip_id, suite["table"]) for t in range(1, tries + 1)]
    if op == "branch_create":
        return [timed_create(ctx, f"create_{t}", tip_id) for t in range(1, tries + 1)]
    if op == "branch_connect":
        return [timed_connects(ctx, chain, batch) for _ in range(tries)]
    if op in POINT_OPS:
        operation = POINT_OPS[op](suite["table"])
    else:
        operation = RANGE_OPS[op](suite["table"], suite["range_size"])
    return data_row(ctx, operation, f"row_{op}", tip_id, batch, tries)


def measure(server: System, info: BackendInfo, db: str, suite: dict, max_batch: int | None,
            deadline: float) -> tuple[str, float, list]:
    """Verify the load, build the chain, and run every row.
    Returns the server version, the round-trip baseline in ms, and the cells."""
    register_all_operations()
    collector = rc.ResultCollector(run_id=db, output_dir=str(RUNS))
    ctx = WorkerContext(task_config(server.backend, db, suite), info, 0, suite["seed"], collector,
                        SharedBranchManager(), None, [])
    with ctx:
        tools = ctx.db_tools
        version = str(tools.execute_sql(server.version_sql)[0][0])
        rtt_ms = round_trip_ms(tools)
        _, root_id = tools.get_current_branch()
        expected = suite["dataset"]["row_counts"]
        verify(ctx, expected, root_id)
        chain = build_chain(ctx, suite["chain_length"], root_id)
        cells = []
        for row in suite["rows"]:
            if time.monotonic() > deadline:
                raise RunError("the run passed its deadline")
            cells.append(run_row(ctx, row, chain, suite, max_batch))
        tools.connect_branch(chain[-1][0], timed=False)
        if (on_tip := count_rows(tools, expected)) != expected:
            raise RunError(f"row writes reached the chain's tip: {on_tip}")
        tools.connect_branch(root(ctx), timed=False)
        for name, _ in reversed(chain):  # leaf first
            tools.delete_branch(name, timed=False)
    return version, rtt_ms, cells


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


def run(system: str, machine: str, out: Path, max_batch: int | None, allow_dirty: bool, deadline_min: float,
        client_location: str | None) -> Path:
    if not MACHINE_RE.fullmatch(machine or ""):
        raise RunError(f"machine label {machine!r} must match {MACHINE_RE.pattern}")
    server, sysdir = SYSTEMS[system], BOARD / system
    meta = json.loads((sysdir / "system.json").read_text())
    hosted = meta["hosted"] == "yes"
    if hosted and not (client_location or "").strip():
        raise RunError("a hosted system needs --client-location, where this machine runs, such as 'US East'")
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
    if hosted and (missing := [k for k in server.keys if not os.environ.get(k)]):
        raise RunError(f"{', '.join(missing)} not set; load them from Bitwarden when launching the run")
    if hosted:
        try:  # the keys work and the account reaches its project, before anything is created
            server.preflight()
        except Exception as e:
            raise RunError(f"{system} preflight failed: {scrub(str(e))}") from e
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
        journal(system).write_text(json.dumps({"run_id": run_id, "name": db}))
        t0 = time.perf_counter()
        info = server.provision(db)
        provision_time = time.perf_counter() - t0
        t0 = time.perf_counter()
        server.load(info, db, dump)
        load_time = time.perf_counter() - t0
        version, rtt_ms, cells = measure(server, info, db, suite, max_batch, time.monotonic() + deadline_min * 60)
    except Exception as e:  # recorded below as the run's error, after cleanup
        failure = e
    finally:
        cleanup_failure = clean_up(system, server)
        if not lifecycle(sysdir, "stop"):
            cleanup_failure = cleanup_failure or RunError(f"{system}: the server did not stop")
        failure = failure or cleanup_failure

    if failure:
        detail = (scrub(str(failure)).splitlines() or [""])[0][:300]
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
    if hosted:
        payload |= {"region": server.region, "rtt_ms": rtt_ms, "client_location": client_location.strip(),
                    "service": server.service}
    if max_batch:
        payload["max_batch"] = max_batch
    payload["result"] = cells
    return publish(out, system, machine, started.date(), payload)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the leaderboard suite on one system.")
    parser.add_argument("system", choices=sorted(SYSTEMS))
    parser.add_argument("--machine", default=os.environ.get("machine"), help="machine label; defaults to $machine")
    parser.add_argument("--client-location", default=os.environ.get("client_location"),
                        help="where this machine runs, recorded for hosted systems; defaults to $client_location")
    parser.add_argument("--out", type=Path, default=BOARD, help="directory that receives <system>/results/")
    parser.add_argument("--max-batch", type=int, help="cap every batch for a short run; the validator rejects it")
    parser.add_argument("--allow-dirty", action="store_true", help="allow uncommitted changes; the validator rejects it")
    parser.add_argument("--deadline-min", type=float, default=120.0, help="stop the run after this many minutes")
    args = parser.parse_args()
    try:
        with exclusive(args.system):
            path = run(args.system, args.machine, args.out, args.max_batch, args.allow_dirty, args.deadline_min,
                       args.client_location)
    except RunError as e:
        print(f"run failed: {e}", file=sys.stderr)
        return 1
    print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
