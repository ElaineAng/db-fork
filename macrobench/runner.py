"""Macrobenchmark runner: one scenario (S1-S6) over one backend.

Usage:
    python -m macrobench.runner --config macrobench/configs/rl_env_mini.textproto

The runner sets the backend up, creates the schema and seed data with the
generators (unless the config reuses an existing database or loads a
dump), starts the optional TPC-C spine load, runs the scenario, and writes
the operation rows to parquet plus an e2e stats JSON with the support
summary, the invariant results and the scenario's own metrics.
"""

import argparse
import json
import os
import signal
import sys
import threading
import time
import traceback

from google.protobuf import text_format
from google.protobuf.json_format import MessageToDict

from dblib import result_collector as rc
from dblib.neon import NeonToolSuite
from macrobench import schema as mschema
from macrobench import task_pb2 as tp
from macrobench.datagen.ch import CHScale, seed_ch
from macrobench.intensity import apply_intensity
from macrobench.scenarios import (  # noqa: F401 (registers the scenarios)
    InvariantFailed,
    InvariantRecorder,
    ScenarioContext,
    ScenarioStopped,
    create_scenario,
)
from macrobench.tpcc import SpineLoad
from microbench.runner2 import (
    BackendManager,
    BackendSetup,
    SharedProgress,
    create_db_tools,
)


def load_config(path: str) -> tp.MacroBenchConfig:
    config = tp.MacroBenchConfig()
    with open(path, "r") as f:
        text_format.Parse(f.read(), config)
    if not config.run_id:
        raise ValueError("run_id is required")
    if not config.database_setup.db_name:
        raise ValueError("database_setup.db_name is required")
    if config.workload.WhichOneof("scenario") is None:
        raise ValueError("workload must set one scenario (rl_env, context_mgmt, ...)")
    if (config.workload.HasField("multi_agent")
            and config.workload.multi_agent.resolution_policy == tp.ResolutionPolicy.LLM_ASSISTED):
        raise ValueError("multi_agent.resolution_policy LLM_ASSISTED is not implemented yet")
    scaled = apply_intensity(config.workload)
    if scaled:
        w = config.workload
        print(f"Intensity: branch x{w.branch_intensity or 1.0}, data x{w.data_intensity or 1.0}")
        for name, (before, after) in scaled.items():
            print(f"  {name}: {before} -> {after}")
    return config


def _flush_to_disk(suite):
    """CHECKPOINT (Postgres; FAILED and ignored elsewhere) then sync."""
    suite.exec(["CHECKPOINT"], timed=False)
    os.sync()


DEFAULT_STORAGE_SAMPLE_SEC = 5.0


class StorageSampler(threading.Thread):
    """Records the backend's storage size every ``interval`` seconds while
    the scenario runs: (wall-clock seconds, bytes) pairs, cheap enough to
    stay on by default (one directory walk per sample) and attributable to
    workflow phases through the operation rows' timestamps."""

    def __init__(self, suite, interval: float):
        super().__init__(daemon=True, name="storage-sampler")
        self.suite = suite
        self.interval = interval
        self.samples: list = []
        self._stop = threading.Event()

    def run(self):
        while not self._stop.is_set():
            t = time.time()
            self.samples.append((round(t, 3), self.suite._safe_storage()))
            self._stop.wait(self.interval)

    def stop(self):
        self._stop.set()
        self.join(timeout=60)


def storage_sample_interval(config, cli_value) -> float:
    """Seconds between samples: the CLI value if given, else the config's
    (0 = the default); 0 or a negative value turns the sampler off."""
    if cli_value is not None:
        return max(0.0, float(cli_value))
    v = float(config.storage_sample_interval_sec or 0)
    if v == 0:
        return DEFAULT_STORAGE_SAMPLE_SEC
    return max(0.0, v)


def _capabilities_for(backend) -> dict:
    from dblib.dolt import DoltToolSuite
    from dblib.dolt_mysql import DoltMySQLToolSuite
    from dblib.seekdb import SeekDBToolSuite
    from dblib.matrixone import MatrixOneToolSuite
    from dblib.xata import XataToolSuite
    from dblib.file_copy import FileCopyToolSuite

    table = {
        tp.Backend.DOLT: DoltToolSuite,
        tp.Backend.DOLT_MYSQL: DoltMySQLToolSuite,
        tp.Backend.SEEKDB: SeekDBToolSuite,
        tp.Backend.MATRIXONE: MatrixOneToolSuite,
        tp.Backend.NEON: NeonToolSuite,
        tp.Backend.XATA: XataToolSuite,
        tp.Backend.FILE_COPY: FileCopyToolSuite,
    }
    cls = table.get(backend)
    return cls.capabilities() if cls else {}


def _implementation_for(backend) -> dict:
    """How the backend realises each verb (native/composed/simulated)."""
    from dblib.dolt import DoltToolSuite
    from dblib.dolt_mysql import DoltMySQLToolSuite
    from dblib.seekdb import SeekDBToolSuite
    from dblib.matrixone import MatrixOneToolSuite
    from dblib.xata import XataToolSuite
    from dblib.file_copy import FileCopyToolSuite

    table = {
        tp.Backend.DOLT: DoltToolSuite,
        tp.Backend.DOLT_MYSQL: DoltMySQLToolSuite,
        tp.Backend.SEEKDB: SeekDBToolSuite,
        tp.Backend.MATRIXONE: MatrixOneToolSuite,
        tp.Backend.NEON: NeonToolSuite,
        tp.Backend.XATA: XataToolSuite,
        tp.Backend.FILE_COPY: FileCopyToolSuite,
    }
    cls = table.get(backend)
    if not cls:
        return {}
    return {"verbs": cls.implementation(), "notes": dict(cls.IMPLEMENTATION_NOTES)}


def _fetch_neon_consumption(project_id, label="", wait_min=15, max_retries=10):
    """Wait for Neon consumption metrics, retrying once per minute."""
    print(f"Waiting {wait_min} min for Neon consumption metrics ({label})...", flush=True)
    for elapsed_min in range(wait_min):
        time.sleep(60)
        if (elapsed_min + 1) % 3 == 0 or elapsed_min + 1 == wait_min:
            print(f"  {elapsed_min + 1}/{wait_min} min elapsed...", flush=True)
    for attempt in range(max_retries):
        result = NeonToolSuite.get_consumption_metrics(project_id)
        if result and result.get("all_metrics"):
            return result
        if attempt < max_retries - 1:
            time.sleep(60)
    print(f"  WARNING: No Neon consumption metrics for {label}", flush=True)
    return None


def setup_data(config, scenario, suite, log=print) -> dict:
    """Create the schema and seed data on the spine; returns stats."""
    spine = scenario.ctx.spine
    stats = {}
    t0 = time.time()
    schema_stats = mschema.apply_schema(suite, spine, config.schema,
                                        scenario.key, log=log)
    stats["schema_statements"] = schema_stats["statements"]
    stats["schema_failed"] = schema_stats["failed"]
    log(f"Schema: {schema_stats['statements']} statements "
        f"({len(schema_stats['failed'])} failed) in {time.time() - t0:.1f}s")

    if config.schema.base == tp.BaseSchema.CH_BENCH:
        t1 = time.time()
        scale = CHScale.from_config(config.schema)
        log(f"Seeding CH-benCHmark at W={scale.warehouses}, items={scale.items}, "
            f"customers/district={scale.customers_per_district}, "
            f"orders/district={scale.orders_per_district}...")
        counts = seed_ch(suite, spine, scale, seed=config.seed or 42,
                         batch_rows=config.schema.seed_batch_rows or 500, log=log)
        stats["ch_rows"] = counts
        stats["ch_seed_sec"] = round(time.time() - t1, 2)
        log(f"CH seed: {sum(counts.values()):,} rows in {stats['ch_seed_sec']}s")

    t2 = time.time()
    ext = scenario.seed(suite)
    stats["extension_rows"] = ext
    stats["extension_seed_sec"] = round(time.time() - t2, 2)

    res = suite.commit(spine, "seed", timed=False)
    stats["seed_commit"] = res.status_name
    stats["setup_sec"] = round(time.time() - t0, 2)
    return stats


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run a macrobenchmark scenario.")
    parser.add_argument("--config", required=True, help="MacroBenchConfig textproto")
    parser.add_argument("--outdir", default="/tmp/run_stats", help="parquet/json output dir")
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument("--measure-storage", action="store_true",
                        help="Measure disk_size_before/after around each timed operation.")
    parser.add_argument("--max-runtime-sec", type=int, default=0,
                        help="Cap the scenario runtime in seconds (0 = no limit).")
    parser.add_argument("--storage-sample-interval", type=float, default=None,
                        help="Seconds between background storage samples during the "
                             f"scenario (default {DEFAULT_STORAGE_SAMPLE_SEC:g}; 0 = off).")
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
    except FileNotFoundError:
        print(f"Error: config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing config: {e}")
        sys.exit(1)
    if args.measure_storage:
        config.measure_storage = True

    scenario_key = config.workload.WhichOneof("scenario")
    backend_name = tp.Backend.Name(config.backend)
    print(f"Run ID: {config.run_id}")
    print(f"Backend: {backend_name}")
    print(f"Scenario: {scenario_key}")
    print(f"Schema: base={tp.BaseSchema.Name(config.schema.base)} "
          f"W={config.schema.scale_factor or 1}")
    print(f"Params: {MessageToDict(getattr(config.workload, scenario_key))}")
    print(f"Branch ops: {MessageToDict(config.workload.branch_ops)}")
    print(f"Data ops: {MessageToDict(config.workload.data_ops)}")
    if config.measure_storage:
        print("Storage measurement: enabled")
    if args.max_runtime_sec:
        print(f"Runtime cap: {args.max_runtime_sec}s")
    sample_sec = storage_sample_interval(config, args.storage_sample_interval)
    print(f"Storage sampling: every {sample_sec:g}s" if sample_sec > 0 else "Storage sampling: off")

    backend_mgr = BackendManager(
        BackendSetup(backend=config.backend, database_setup=config.database_setup)
    )
    backend_info = backend_mgr.setup()

    # A long run with a spine load emits millions of rows; spill them to
    # the parquet file as they come so the run's memory stays bounded.
    collector = rc.ResultCollector(run_id=config.run_id, output_dir=args.outdir,
                                   spill_rows=int(os.environ.get("MACRO_SPILL_ROWS", "200000")))
    # Per-op storage is too expensive on Neon (pg_database_size per branch).
    measure_storage = config.measure_storage and config.backend != tp.Backend.NEON
    db_name = config.database_setup.db_name

    def suite_factory():
        return create_db_tools(config.backend, backend_info, db_name, collector,
                               measure_storage=measure_storage)

    invariants = InvariantRecorder(enabled=not config.invariants.disabled,
                                   fail_fast=config.invariants.fail_fast)
    ctx = ScenarioContext(config=config, suite_factory=suite_factory,
                          collector=collector, spine=backend_info.default_branch_name,
                          invariants=invariants, log=print)
    scenario = create_scenario(ctx)
    scenario.validate()
    collector.set_context(table_name="macrobench", table_schema=scenario.key,
                          initial_db_size=0, seed=ctx.seed)

    setup_suite = ctx.new_suite()
    seed_stats = {}
    if config.database_setup.WhichOneof("source") != "existing_db":
        seed_stats = setup_data(config, scenario, setup_suite)

    # Workflow-level storage points (always on; a directory walk each):
    # after setup, after the scenario (its branches deleted), after the
    # backend's GC/flush, and after the database itself is dropped.
    storage_points = {}
    try:
        _flush_to_disk(setup_suite)
    except Exception as e:
        print(f"Warning: flush before the workflow failed: {e}")
    storage_points["after_setup"] = setup_suite._safe_storage()
    storage_before = storage_points["after_setup"]
    print(f"Storage after setup: {storage_before} bytes ({setup_suite.STORAGE_SCOPE} scope)")
    sampler = None
    if sample_sec > 0:
        sampler = StorageSampler(setup_suite, sample_sec)
        sampler.start()

    progress = SharedProgress(total=scenario.total_units(),
                              desc=f"{scenario.name} ({backend_name})",
                              disable=args.no_progress)
    ctx.progress = progress

    spine_load = None
    data_ops = config.workload.data_ops
    if data_ops.spine_clients > 0 and scenario.uses_spine_load:
        spine_load = SpineLoad(
            suite_factory, ctx.spine, ctx.scale, clients=data_ops.spine_clients,
            analytical_fraction=data_ops.analytical_fraction,
            txn_limit=data_ops.spine_txn_limit, seed=ctx.seed,
            touch_c_data=scenario.spine_touches_c_data, on_error=ctx.note,
        ).start()
        scenario.spine_load = spine_load
        print(f"Spine load: {data_ops.spine_clients} client(s), "
              f"analytical fraction {data_ops.analytical_fraction:.2f}")

    deadline_timer = None
    if args.max_runtime_sec:
        deadline_timer = threading.Timer(args.max_runtime_sec, ctx.cancel_all)
        deadline_timer.daemon = True
        deadline_timer.start()

    # An interrupt (Ctrl-C, SIGTERM) stops the scenario the way the runtime
    # cap does: the workers see the stop event, the run is recorded as
    # interrupted and its rows and stats are still written.
    def _on_signal(signum, frame):
        print(f"\nSignal {signal.Signals(signum).name}: stopping the scenario...", flush=True)
        ctx.cancel_all()
    previous_handlers = {s: signal.signal(s, _on_signal) for s in (signal.SIGINT, signal.SIGTERM)}

    print(f"\nStarting scenario {scenario.name}...")
    start_time = time.time()
    status = "completed"
    try:
        scenario.run()
    except ScenarioStopped:
        status = "interrupted"
    except InvariantFailed as e:
        status = f"invariant_failed: {e}"
    except Exception as e:
        status = f"crashed: {type(e).__name__}: {e}"
        traceback.print_exc()
    finally:
        for s, h in previous_handlers.items():
            signal.signal(s, h)
        if deadline_timer is not None:
            deadline_timer.cancel()
        if spine_load is not None:
            spine_load.stop()
        progress.close()
        elapsed = time.time() - start_time
        stopped = ctx.stop_event.is_set()
        timed_out = bool(args.max_runtime_sec) and stopped and elapsed >= args.max_runtime_sec
        if status == "completed" and stopped:
            status = "interrupted"
        print(f"\nScenario {status} in {elapsed:.1f}s")
        if timed_out:
            print("Run terminated early due to runtime cap.")

        if sampler is not None:
            sampler.stop()
        try:
            _flush_to_disk(setup_suite)
        except Exception as e:
            print(f"Warning: flush after the workflow failed: {e}")
        storage_after = storage_points["after_workflow"] = setup_suite._safe_storage()
        print(f"Storage after workflow: {storage_after} bytes "
              f"(delta {storage_after - storage_before})")
        gc_result = setup_suite.gc()
        if gc_result.ok:
            os.sync()
        storage_points["after_gc"] = setup_suite._safe_storage()
        print(f"Storage after gc ({gc_result.status_name}, {gc_result.latency:.1f}s): "
              f"{storage_points['after_gc']} bytes")
        ctx.close_suite(setup_suite)

        neon_consumption = None
        if config.measure_storage and config.backend == tp.Backend.NEON:
            neon_consumption = _fetch_neon_consumption(
                backend_info.neon_project_id, label="after")

        inv = invariants.summary()

        # Drop the database before the record is written so the last
        # storage point sees the server without it.
        for attempt in range(2):
            try:
                backend_mgr.cleanup()
                break
            except Exception as e:
                if attempt == 0:
                    print(f"Cleanup failed ({type(e).__name__}), retrying...")
                    time.sleep(2)
                else:
                    print(f"Cleanup failed after retry: {e}")
        os.sync()
        storage_points["after_cleanup"] = setup_suite._safe_storage()
        print(f"Storage after cleanup: {storage_points['after_cleanup']} bytes")

        e2e = {
            "run_id": config.run_id,
            "backend": backend_name,
            "scenario": scenario.key,
            "status": status,
            "elapsed_sec": round(elapsed, 2),
            "max_runtime_sec": args.max_runtime_sec,
            "timed_out": timed_out,
            "seed": ctx.seed,
            "schema": MessageToDict(config.schema),
            "workload": MessageToDict(config.workload),
            "setup": seed_stats,
            "capabilities": _capabilities_for(config.backend),
            "implementation": _implementation_for(config.backend),
            "invariants": inv,
            "metrics": ctx.metrics,
        }
        e2e.update(collector.support_summary())
        observed = getattr(type(setup_suite), "OBSERVED", None)
        if observed:
            e2e["backend_observations"] = dict(observed)
        if spine_load is not None:
            e2e["spine_load"] = {"transactions": spine_load.counts,
                                 "failures": spine_load.failures}
        e2e["storage_before_bytes"] = storage_before
        e2e["storage_after_bytes"] = storage_after
        e2e["storage_delta_bytes"] = storage_after - storage_before
        e2e["storage"] = {
            "unit": "bytes",
            "scope": setup_suite.STORAGE_SCOPE,
            "points": storage_points,
            "gc": {"status": gc_result.status_name, "latency_sec": round(gc_result.latency, 3),
                   "error": gc_result.error},
            "per_operation": measure_storage,
            "sample_interval_sec": sample_sec,
            "run_start_time": round(start_time, 3),
            "samples": sampler.samples if sampler is not None else [],
        }
        if neon_consumption:
            e2e["neon_metrics_count"] = neon_consumption.get("count", 0)
            e2e["neon_all_metrics"] = neon_consumption.get("all_metrics", [])
            for k, v in (neon_consumption.get("summary") or {}).items():
                e2e[f"neon_{k}"] = v

        os.makedirs(args.outdir, exist_ok=True)
        path = os.path.join(args.outdir, f"{config.run_id}_e2e_stats.json")
        with open(path, "w") as f:
            json.dump(e2e, f, indent=2, default=str)
        print(f"E2E stats written to {path}")
        print(f"Workflow supported: {e2e['workflow_supported']}; "
              f"unsupported ops: {e2e['unsupported_ops']}")
        print(f"Invariants: {inv['passed']} passed, {inv['failed']} failed, "
              f"{inv['not_applicable']} not applicable")
        for r in inv["results"]:
            if r["passed"] is False:
                print(f"  FAILED {r['name']}: {r['detail']}")
        if ctx.metrics:
            print(f"Metrics: {json.dumps(ctx.metrics, default=str)}")

        collector.write_to_parquet(append=False)


if __name__ == "__main__":
    main()
