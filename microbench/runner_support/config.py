from __future__ import annotations

from typing import Optional

from microbench import task_pb2 as tp


def validate_config(config: tp.TaskConfig):
    if config.measure_storage and config.num_threads > 1:
        raise ValueError(
            "measure_storage is incompatible with num_threads > 1. "
            "Concurrent writes from other threads pollute each thread's "
            "before/after storage deltas. Use the single-threaded script "
            "with --measure-storage instead."
        )

    if config.WhichOneof("benchmark_mode") == "throughput_benchmark":
        tc = config.throughput_benchmark
        if tc.duration_seconds <= 0:
            raise ValueError("throughput_benchmark.duration_seconds must be > 0")
        if not tc.operations:
            raise ValueError("throughput_benchmark.operations must not be empty")
        if config.measure_storage:
            raise ValueError("measure_storage is incompatible with throughput mode")

    if config.backend == tp.Backend.NEON:
        db_setup = config.database_setup
        source_type = db_setup.WhichOneof("source")
        if source_type == "existing_db":
            assert db_setup.existing_db.neon_project_id, (
                "When reusing existing Neon database, neon_project_id "
                "must be provided."
            )


def get_benchmark_setup(config: tp.TaskConfig) -> Optional[tp.BenchmarkSetup]:
    """Return the optional setup block for the active benchmark mode."""
    mode = config.WhichOneof("benchmark_mode")
    setup_getters = {
        "nth_op_benchmark": (
            lambda c: c.nth_op_benchmark.setup
            if c.nth_op_benchmark.HasField("setup")
            else None
        ),
        "throughput_benchmark": (
            lambda c: c.throughput_benchmark.setup
            if c.throughput_benchmark.HasField("setup")
            else None
        ),
    }
    getter = setup_getters.get(mode)
    return getter(config) if getter else None


def get_num_iterations(config: tp.TaskConfig, nth_op_num_runs: int) -> int:
    """Return number of benchmark iterations for the selected mode."""
    if (
        config.WhichOneof("benchmark_mode") == "nth_op_benchmark"
        and config.nth_op_benchmark.num_ops == 1
    ):
        return nth_op_num_runs
    return 1


def get_ops_per_thread(config: tp.TaskConfig) -> int:
    """Return per-thread op count for non-throughput benchmark modes."""
    mode = config.WhichOneof("benchmark_mode")
    ops_getters = {
        "nth_op_benchmark": lambda c: c.nth_op_benchmark.num_ops or 1,
        "randomized_benchmark": lambda c: c.randomized_benchmark.num_ops,
    }
    getter = ops_getters.get(mode)
    if not getter:
        raise ValueError(
            f"Mode '{mode}' does not use fixed operation counts per thread."
        )
    return getter(config)
