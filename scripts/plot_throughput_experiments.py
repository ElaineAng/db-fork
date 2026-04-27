#!/usr/bin/env python3
"""
Plot throughput vs threads/branches/concurrency for throughput experiments.

Supports two data sources:
  - JSON mode: Read pre-calculated throughput from summary.json files
  - Parquet mode: Calculate throughput from raw parquet files (excluding warm-up)

Usage:
    # JSON mode - sweep threads (includes all overhead)
    python plot_throughput_experiments.py \
      --data-dir run_stats/micro/tp_fix_branches \
      --mode threads \
      --source json \
      --output figures/throughput_vs_threads.png

    # Parquet mode - sweep branches (default: uses all measured ops)
    python plot_throughput_experiments.py \
      --data-dir run_stats/micro/tp_fix_thread \
      --mode branches \
      --source parquet \
      --output figures/throughput_vs_branches.png

    # Parquet mode - sweep concurrency (concurrent requests per thread)
    python plot_throughput_experiments.py \
      --data-dir run_stats/micro/tp_cc \
      --mode concurrency \
      --source parquet \
      --output figures/throughput_vs_concurrency.png

    # Parquet mode with warm-up exclusion (uses last 80% of measured ops)
    python plot_throughput_experiments.py \
      --data-dir run_stats/micro/tp_fix_thread \
      --mode branches \
      --source parquet \
      --exclude-warmup-time \
      --output figures/throughput_vs_branches_steady.png

Timing methodology:
    JSON:
      - elapsed_time = wall-clock from benchmark start to finish
      - Includes thread creation, warm-up execution, measured ops, cleanup
      - Most accurate for real-world performance

    Parquet (default):
      - elapsed_time = max(end_time) - min(start_time) of measured ops
      - Includes SQL execution time for measured operations
      - Excludes thread overhead but may include implicit warm-up

    Parquet (--exclude-warmup-time):
      - elapsed_time = max(end_time) - min(start_time) of last 80% of measured ops
      - Steady-state throughput, excludes both explicit and implicit warm-up
      - Best for comparing "warmed up" performance
"""

import argparse
import json
import re
from pathlib import Path
from collections import defaultdict
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd


def parse_summary_filename(filename):
    """
    Parse summary filename to extract metadata.

    Format: {backend}_ch-w_tp_t{threads}_b{branches}_cr{concurrency}_{operation}_threads{threads}_summary.json
    Example: neon_ch-w_tp_t1_b1_cr16_READ_threads1_summary.json
    Legacy: neon_ch-w_tp_t128_b16_READ_threads128_summary.json (no cr field)

    Returns:
        dict with keys: backend, operation, num_threads, num_branches, concurrent_requests
    """
    # Extract backend
    backend = None
    if filename.startswith('dolt_'):
        backend = 'DOLT'
    elif filename.startswith('neon_'):
        backend = 'NEON'
    elif filename.startswith('kpg_'):
        backend = 'KPG'
    elif filename.startswith('xata_'):
        backend = 'XATA'

    # Extract operation (READ, RANGE_READ, UPDATE, etc.)
    op_match = re.search(r'_(READ|RANGE_READ|UPDATE|RANGE_UPDATE|INSERT|CONNECT)_threads', filename)
    operation = op_match.group(1) if op_match else None

    # Extract thread count
    threads_match = re.search(r'_t(\d+)_b', filename)
    num_threads = int(threads_match.group(1)) if threads_match else None

    # Extract branch count
    branches_match = re.search(r'_b(\d+)_', filename)
    num_branches = int(branches_match.group(1)) if branches_match else None

    # Extract concurrent requests (optional, defaults to 1 for legacy files)
    cr_match = re.search(r'_cr(\d+)_', filename)
    concurrent_requests = int(cr_match.group(1)) if cr_match else 1

    return {
        'backend': backend,
        'operation': operation,
        'num_threads': num_threads,
        'num_branches': num_branches,
        'concurrent_requests': concurrent_requests
    }


def load_throughput_from_json(data_dir, mode):
    """
    Load throughput data from summary JSON files.

    Args:
        data_dir: Directory containing summary JSON files
        mode: 'threads', 'branches', or 'concurrency' (determines x-axis)

    Returns:
        dict: {(backend, operation): {x_value: throughput}}
    """
    data = defaultdict(dict)
    data_path = Path(data_dir)

    if not data_path.exists():
        raise ValueError(f"Data directory not found: {data_dir}")

    # Find all summary JSON files
    for json_file in data_path.glob("*_summary.json"):
        metadata = parse_summary_filename(json_file.name)

        if not all([metadata['backend'], metadata['operation'],
                   metadata['num_threads'], metadata['num_branches']]):
            continue

        # Read throughput from JSON
        with open(json_file, 'r') as f:
            summary = json.load(f)

        throughput = summary.get('throughput', 0)
        backend = metadata['backend']
        operation = metadata['operation']

        # Determine x-axis value based on mode
        if mode == 'branches':
            x_value = metadata['num_branches']
        elif mode == 'concurrency':
            x_value = metadata['concurrent_requests']
        else:  # mode == 'threads'
            x_value = metadata['num_threads']

        key = (backend, operation)
        data[key][x_value] = throughput

    return data


def load_throughput_from_parquet(data_dir, mode, exclude_warmup_time=False):
    """
    Load throughput data from parquet files.

    Reads companion summary.json to get num_ops, then filters parquet
    to exclude warm-up operations and approximates elapsed time.

    Args:
        data_dir: Directory containing parquet files and summary JSONs
        mode: 'threads', 'branches', or 'concurrency' (determines x-axis)
        exclude_warmup_time: If True, skip first 20% of measured ops to exclude
                           implicit warm-up time (only use steady-state ops)

    Returns:
        dict: {(backend, operation): {x_value: throughput}}
    """
    data = defaultdict(dict)
    data_path = Path(data_dir)

    if not data_path.exists():
        raise ValueError(f"Data directory not found: {data_dir}")

    # Find all parquet files (excluding _setup.parquet)
    for parquet_file in data_path.glob("*.parquet"):
        if '_setup.parquet' in parquet_file.name:
            continue

        # Find corresponding summary JSON to get num_ops
        # Pattern: neon_ch-w_tp_t1_b1_cr16.parquet -> neon_ch-w_tp_t1_b1_cr16_READ_threads1_summary.json
        base_name = parquet_file.stem  # e.g., "neon_ch-w_tp_t1_b1_cr16"

        # Find all matching summary files for this run
        summary_files = list(data_path.glob(f"{base_name}_*_summary.json"))

        if not summary_files:
            print(f"Warning: No summary.json found for {parquet_file.name}, skipping")
            continue

        # Read parquet data once
        df = pd.read_parquet(parquet_file)

        # Process each operation from summary files
        for summary_file in summary_files:
            metadata = parse_summary_filename(summary_file.name)

            if not all([metadata['backend'], metadata['operation'],
                       metadata['num_threads'], metadata['num_branches']]):
                continue

            # Read summary to get num_ops
            with open(summary_file, 'r') as f:
                summary = json.load(f)

            num_ops = summary.get('num_ops', 0)
            num_threads = summary.get('num_threads', 1)

            if num_ops == 0:
                continue

            # Filter by operation type using num_keys_touched
            # READ: num_keys_touched == 1, RANGE_READ: num_keys_touched > 1
            operation = metadata['operation']
            if operation == 'READ':
                op_df = df[df['num_keys_touched'] == 1]
            elif operation == 'RANGE_READ':
                op_df = df[df['num_keys_touched'] > 1]
            else:
                # For other operations, use all data
                op_df = df

            # Filter to last num_ops per thread (excludes explicit warm-up)
            measured_ops = op_df.groupby('thread_id').tail(num_ops)

            # If exclude_warmup_time is enabled, further filter to skip first 20%
            # of measured operations to exclude implicit warm-up effects
            if exclude_warmup_time:
                # Skip first 20% of operations per thread to get steady-state only
                steady_state_ops_per_thread = int(num_ops * 0.8)
                measured_ops = measured_ops.groupby('thread_id').tail(steady_state_ops_per_thread)

            # Calculate elapsed time (use start/end times if available, else fall back to latency sum)
            if 'start_time' in df.columns and 'end_time' in df.columns:
                # Use actual timestamps (excluding warm-up)
                min_start = measured_ops['start_time'].min()
                max_end = measured_ops['end_time'].max()
                elapsed_time = max_end - min_start
            else:
                # Fall back to approximate elapsed time as max(sum of latencies per thread)
                thread_times = measured_ops.groupby('thread_id')['latency'].sum()
                elapsed_time = thread_times.max()

            if elapsed_time == 0:
                continue

            # Calculate throughput
            throughput = len(measured_ops) / elapsed_time

            backend = metadata['backend']
            operation = metadata['operation']

            # Determine x-axis value based on mode
            if mode == 'branches':
                x_value = metadata['num_branches']
            elif mode == 'concurrency':
                x_value = metadata['concurrent_requests']
            else:  # mode == 'threads'
                x_value = metadata['num_threads']

            key = (backend, operation)
            data[key][x_value] = throughput

    return data


def plot_throughput(data, mode, output_file, max_value=None):
    """
    Plot throughput vs threads/branches/concurrency.

    Args:
        data: dict from load_throughput_*() functions
        mode: 'threads', 'branches', or 'concurrency'
        output_file: path to save figure
        max_value: maximum x-axis value to include in plot (default: None, show all)
    """
    if not data:
        print("No data to plot")
        return

    fig, ax = plt.subplots(figsize=(12, 7))

    # Combined style map for each backend+operation combination
    # Each combination gets a UNIQUE marker shape for black-and-white printing clarity
    style_map = {
        # DOLT - using circles and squares
        ('DOLT', 'READ'): {'color': '#2176AE', 'marker': 'o', 'linestyle': '-', 'label': 'Dolt - Read'},
        ('DOLT', 'RANGE_READ'): {'color': '#2176AE', 'marker': 's', 'linestyle': '--', 'label': 'Dolt - Range Read'},
        ('DOLT', 'UPDATE'): {'color': '#2176AE', 'marker': 'p', 'linestyle': '-', 'label': 'Dolt - Update'},
        ('DOLT', 'RANGE_UPDATE'): {'color': '#2176AE', 'marker': 'P', 'linestyle': '--', 'label': 'Dolt - Range Update'},
        ('DOLT', 'INSERT'): {'color': '#2176AE', 'marker': 'h', 'linestyle': '-', 'label': 'Dolt - Insert'},
        ('DOLT', 'CONNECT'): {'color': '#2176AE', 'marker': 'H', 'linestyle': '-', 'label': 'Dolt - Connect'},
        # NEON - using triangles
        ('NEON', 'READ'): {'color': '#E8554E', 'marker': '^', 'linestyle': '-', 'label': 'Neon - Read'},
        ('NEON', 'RANGE_READ'): {'color': '#E8554E', 'marker': 'v', 'linestyle': '--', 'label': 'Neon - Range Read'},
        ('NEON', 'UPDATE'): {'color': '#E8554E', 'marker': '<', 'linestyle': '-', 'label': 'Neon - Update'},
        ('NEON', 'RANGE_UPDATE'): {'color': '#E8554E', 'marker': '>', 'linestyle': '--', 'label': 'Neon - Range Update'},
        ('NEON', 'INSERT'): {'color': '#E8554E', 'marker': '1', 'linestyle': '-', 'label': 'Neon - Insert'},
        ('NEON', 'CONNECT'): {'color': '#E8554E', 'marker': '2', 'linestyle': '-', 'label': 'Neon - Connect'},
    }

    # Plot each backend+operation combination
    for (backend, operation), x_data in sorted(data.items()):
        key = (backend, operation)
        if key not in style_map:
            continue

        # Sort by x-axis value and filter by max_value
        x_values = sorted(x_data.keys())
        if max_value is not None:
            x_values = [x for x in x_values if x <= max_value]

        if not x_values:
            continue

        throughputs = [x_data[x] for x in x_values]

        style = style_map[key]

        ax.plot(x_values, throughputs,
                marker=style['marker'],
                linestyle=style['linestyle'],
                color=style['color'],
                linewidth=3.5,
                markersize=12,
                label=style['label'],
                alpha=0.85)

    # Formatting based on mode
    if mode == 'threads':
        x_label = 'Number of Threads'
    elif mode == 'branches':
        x_label = 'Number of Branches'
    else:  # concurrency
        x_label = 'Concurrent Requests per Thread'

    ax.set_xlabel(f'{x_label} (log scale)', fontsize=20, fontweight='bold')
    ax.set_ylabel('Throughput (ops/sec) (log scale)', fontsize=20, fontweight='bold')

    # Use log2 scale for both axes
    ax.set_xscale('log', base=2)
    ax.set_yscale('log', base=2)

    # Force more granular y-axis ticks to ensure at least 2 measures are visible
    from matplotlib.ticker import LogLocator, FixedLocator
    import numpy as np

    # Get the y-axis data range
    all_y_values = []
    for x_data in data.values():
        all_y_values.extend(x_data.values())

    if all_y_values:
        y_min, y_max = min(all_y_values), max(all_y_values)
        # Calculate appropriate tick locations in powers of 2
        # Start from the power of 2 below y_min
        tick_start = int(np.floor(np.log2(y_min)))
        tick_end = int(np.ceil(np.log2(y_max))) + 1
        y_ticks = [2**i for i in range(tick_start, tick_end)]

        # Ensure we have at least 3 ticks for better readability
        if len(y_ticks) < 3:
            # Add one more tick on each end
            y_ticks = [2**(tick_start-1)] + y_ticks + [2**tick_end]

        # Set explicit tick locations
        ax.set_yticks(y_ticks)
        print(f"Y-axis range: {y_min:.1f} to {y_max:.1f}")
        print(f"Y-axis ticks set to: {y_ticks}")

    # Grid
    ax.grid(True, alpha=0.3, linestyle='--', which='both')

    # Legend - positioned on top of the plot in two columns
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1.02), ncol=2, fontsize=16, framealpha=0.95)

    # Format both axes to show decimal numbers (not powers of 2)
    from matplotlib.ticker import FuncFormatter
    def format_decimal(x, p):
        if x <= 0:
            return '0'
        if x >= 1000:
            return f'{x/1000:.0f}k'
        else:
            return f'{x:.0f}'

    ax.xaxis.set_major_formatter(FuncFormatter(format_decimal))
    ax.yaxis.set_major_formatter(FuncFormatter(format_decimal))

    # Increase tick label font size
    ax.tick_params(axis='both', which='major', labelsize=20)
    ax.tick_params(axis='both', which='minor', labelsize=16)

    plt.tight_layout(pad=1.5)

    # Save figure
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_file, dpi=300)
    print(f"Saved figure to {output_file}")

    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(
        description='Plot throughput vs threads/branches for throughput experiments',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--data-dir',
        required=True,
        help='Directory containing experiment data'
    )

    parser.add_argument(
        '--mode',
        required=True,
        choices=['threads', 'branches', 'concurrency'],
        help='Experiment type: threads (sweep threads), branches (sweep branches), or concurrency (sweep concurrent requests)'
    )

    parser.add_argument(
        '--source',
        required=True,
        choices=['json', 'parquet'],
        help='Data source: json (summary files) or parquet (raw data)'
    )

    parser.add_argument(
        '--output',
        required=True,
        help='Output file path (e.g., figures/throughput_vs_threads.png)'
    )

    parser.add_argument(
        '--max-threads',
        type=int,
        default=None,
        help='Maximum number of threads to include in plot (only for threads mode)'
    )

    parser.add_argument(
        '--max-branches',
        type=int,
        default=None,
        help='Maximum number of branches to include in plot (only for branches mode)'
    )

    parser.add_argument(
        '--max-concurrency',
        type=int,
        default=None,
        help='Maximum concurrent requests to include in plot (only for concurrency mode)'
    )

    parser.add_argument(
        '--exclude-warmup-time',
        action='store_true',
        help='Exclude warm-up time by skipping first 20%% of measured operations (parquet mode only). '
             'This gives steady-state throughput by excluding implicit warm-up effects.'
    )

    args = parser.parse_args()

    # Load data based on source
    print(f"Loading data from {args.data_dir}...")
    if args.source == 'json':
        if args.exclude_warmup_time:
            print("Warning: --exclude-warmup-time only applies to parquet mode, ignoring for JSON")
        data = load_throughput_from_json(args.data_dir, args.mode)
    else:  # parquet
        if args.exclude_warmup_time:
            print("Excluding warm-up time (using last 80% of measured operations per thread)")
        data = load_throughput_from_parquet(args.data_dir, args.mode,
                                           exclude_warmup_time=args.exclude_warmup_time)

    if not data:
        print("Error: No data loaded")
        return 1

    print(f"Found data for {len(data)} backend+operation combinations")
    for key in sorted(data.keys()):
        backend, operation = key
        x_values = sorted(data[key].keys())
        print(f"  {backend} {operation}: {len(x_values)} data points (x={x_values})")

    # Determine max_value based on mode
    if args.mode == 'threads':
        max_value = args.max_threads
    elif args.mode == 'branches':
        max_value = args.max_branches
    else:  # concurrency
        max_value = args.max_concurrency

    # Plot
    print(f"\nGenerating plot...")
    if max_value:
        print(f"Filtering to max {args.mode}: {max_value}")
    plot_throughput(data, args.mode, args.output, max_value=max_value)

    return 0


if __name__ == '__main__':
    exit(main())
