#!/usr/bin/env python3
"""
Plot throughput vs number of threads/branches for different backends.

Usage:
    python plot_throughput.py --data-dir final/micro/tp/ops_tp_single_branch --output-dir micro-analysis/figures_throughput
    python plot_throughput.py --data-dir final/micro/tp/ops_tp_single_branch --operation READ --output-dir micro-analysis/figures_throughput
"""

import json
import os
import re
from pathlib import Path
from collections import defaultdict
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import numpy as np
import argparse


def parse_summary_filename(filename):
    """
    Parse summary filename to extract metadata.

    Supported formats:
    - Old: {run_id}_{operation}_threads{num_threads}_summary.json
      Example: dolt_ch-w_multitrd_1_spine_READ_threads8_summary.json
    - New: {backend}_{prefix}_tp_t{threads}_b{branches}_{operation}_threads{threads}_summary.json
      Example: dolt_ch-w_tp_t128_b16_READ_threads128_summary.json

    Returns:
        dict with keys: backend, operation, num_threads, num_branches
    """
    # Extract backend from filename
    backend = None
    if filename.startswith('dolt_'):
        backend = 'DOLT'
    elif filename.startswith('neon_'):
        backend = 'NEON'
    elif filename.startswith('kpg_'):
        backend = 'KPG'

    # Extract operation (READ, RANGE_READ, UPDATE, RANGE_UPDATE, etc.)
    op_match = re.search(r'_(READ|RANGE_READ|UPDATE|RANGE_UPDATE|CONNECT|BRANCH)_threads', filename)
    operation = op_match.group(1) if op_match else None

    # Extract thread count (from the _threads{N} suffix)
    threads_match = re.search(r'threads(\d+)', filename)
    num_threads = int(threads_match.group(1)) if threads_match else None

    # Extract branch count (from _b{N} in run_id if present)
    branches_match = re.search(r'_b(\d+)', filename)
    num_branches = int(branches_match.group(1)) if branches_match else None

    return {
        'backend': backend,
        'operation': operation,
        'num_threads': num_threads,
        'num_branches': num_branches
    }


def load_throughput_data(data_dir, mode='auto', use_aggregated=False):
    """
    Load all summary JSON files and extract throughput data.

    Args:
        data_dir: Directory containing summary JSON files (searches recursively)
        mode: 'threads' (x-axis=threads), 'branches' (x-axis=branches), or 'auto' (detect)
        use_aggregated: If True, use average_wall_clock_throughput (aggregated across threads).
                       If False, use average_throughput (per-thread average). Default: False.

    Returns:
        tuple: (data, detected_mode)
        - data: dict {operation: {backend: {x_value: throughput}}}
        - detected_mode: 'threads' or 'branches'
    """
    data = defaultdict(lambda: defaultdict(dict))

    data_path = Path(data_dir)
    if not data_path.exists():
        raise ValueError(f"Data directory not found: {data_dir}")

    # Collect all metadata to detect mode (search recursively)
    all_metadata = []
    for json_file in data_path.rglob("*_summary.json"):
        metadata = parse_summary_filename(json_file.name)
        if all([metadata['backend'], metadata['operation'], metadata['num_threads']]):
            # Read JSON file
            with open(json_file, 'r') as f:
                summary = json.load(f)
            # Choose throughput metric based on use_aggregated flag
            if use_aggregated:
                metadata['throughput'] = summary.get('average_wall_clock_throughput', 0)
            else:
                metadata['throughput'] = summary.get('average_throughput', 0)
            all_metadata.append(metadata)

    if not all_metadata:
        return data, mode

    # Auto-detect mode if requested
    if mode == 'auto':
        # Check if num_branches varies (sweep-branches mode)
        branch_counts = set(m['num_branches'] for m in all_metadata if m['num_branches'] is not None)
        thread_counts = set(m['num_threads'] for m in all_metadata)

        if len(branch_counts) > 1:
            mode = 'branches'  # Multiple branch counts -> x-axis is branches
        else:
            mode = 'threads'   # x-axis is threads (default)

    # Store data based on mode
    for metadata in all_metadata:
        operation = metadata['operation']
        backend = metadata['backend']
        throughput = metadata['throughput']

        if mode == 'branches':
            # X-axis is number of branches
            x_value = metadata['num_branches']
            if x_value is None:
                # For old format without branch info, skip
                continue
        else:
            # X-axis is number of threads (default)
            x_value = metadata['num_threads']

        data[operation][backend][x_value] = throughput

    return data, mode


def plot_throughput_comparison(data, operation, mode='threads', output_dir=None):
    """
    Plot throughput vs threads/branches for a single operation across multiple backends.

    Args:
        data: dict from load_throughput_data()
        operation: operation type to plot (e.g., 'READ')
        mode: 'threads' or 'branches' (determines x-axis label)
        output_dir: directory to save figure (optional)
    """
    if operation not in data:
        print(f"Warning: No data for operation '{operation}'")
        return

    fig, ax = plt.subplots(figsize=(10, 6))

    # Backend colors and markers
    backend_styles = {
        'DOLT': {'color': '#2E86AB', 'marker': 'o', 'label': 'Dolt'},
        'NEON': {'color': '#A23B72', 'marker': 's', 'label': 'Neon'},
        'KPG': {'color': '#F18F01', 'marker': '^', 'label': 'KPG'},
    }

    for backend, x_data in sorted(data[operation].items()):
        if backend not in backend_styles:
            continue

        # Sort by x-axis value
        x_values = sorted(x_data.keys())
        throughputs = [x_data[x] for x in x_values]

        style = backend_styles[backend]
        ax.plot(x_values, throughputs,
                marker=style['marker'],
                color=style['color'],
                linewidth=2.5,
                markersize=8,
                label=style['label'])

    # Formatting based on mode
    x_label = 'Number of Threads' if mode == 'threads' else 'Number of Branches'
    title = f'{operation} Throughput vs {x_label.split()[-1].capitalize()}'

    ax.set_xlabel(f'{x_label} (log scale)', fontsize=14, fontweight='bold')
    ax.set_ylabel('Throughput (ops/sec) (log scale)', fontsize=14, fontweight='bold')
    ax.set_title(title, fontsize=16, fontweight='bold')

    # Use log scale for x-axis if appropriate
    ax.set_xscale('log', base=2)
    ax.set_yscale('log')

    # Grid
    ax.grid(True, alpha=0.3, linestyle='--')

    # Legend - positioned on top of the plot in two columns
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1.02), ncol=2, fontsize=12, framealpha=0.9)

    # Format y-axis to show numbers in scientific notation
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.0f}'))

    plt.tight_layout()

    # Save figure
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f'throughput_{operation.lower()}_{mode}.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved figure to {output_file}")

    plt.close(fig)


def plot_all_operations(data, mode='threads', output_dir=None):
    """
    Create a single figure with subplots for all operations.

    Args:
        data: dict from load_throughput_data()
        mode: 'threads' or 'branches' (determines x-axis label)
        output_dir: directory to save figure (optional)
    """
    operations = sorted(data.keys())

    if not operations:
        print("No operations found in data")
        return

    # Create subplots
    n_ops = len(operations)
    n_cols = min(2, n_ops)
    n_rows = (n_ops + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(12, 5*n_rows))
    if n_ops == 1:
        axes = [axes]
    else:
        axes = axes.flatten()

    # Backend styles
    backend_styles = {
        'DOLT': {'color': '#2E86AB', 'marker': 'o', 'label': 'Dolt'},
        'NEON': {'color': '#A23B72', 'marker': 's', 'label': 'Neon'},
        'KPG': {'color': '#F18F01', 'marker': '^', 'label': 'KPG'},
    }

    x_label = 'Number of Threads' if mode == 'threads' else 'Number of Branches'

    for idx, operation in enumerate(operations):
        ax = axes[idx]

        for backend, x_data in sorted(data[operation].items()):
            if backend not in backend_styles:
                continue

            x_values = sorted(x_data.keys())
            throughputs = [x_data[x] for x in x_values]

            style = backend_styles[backend]
            ax.plot(x_values, throughputs,
                    marker=style['marker'],
                    color=style['color'],
                    linewidth=2.5,
                    markersize=8,
                    label=style['label'])

        # Formatting
        ax.set_xlabel(f'{x_label} (log scale)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Throughput (ops/sec) (log scale)', fontsize=12, fontweight='bold')
        ax.set_title(f'{operation}', fontsize=14, fontweight='bold')
        ax.set_xscale('log', base=2)
        ax.set_yscale('log')
        ax.grid(True, alpha=0.3, linestyle='--')
        # Legend - positioned on top of each subplot in two columns
        ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1.02), ncol=2, fontsize=10)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.0f}'))

    # Hide unused subplots
    for idx in range(n_ops, len(axes)):
        axes[idx].set_visible(False)

    plt.tight_layout()

    # Save figure
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f'throughput_all_operations_{mode}.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved figure to {output_file}")

    plt.close(fig)


def plot_read_operations_combined(data, mode='threads', output_dir=None, range_size=100, max_threads=None, legend_loc='outside top'):
    """
    Plot READ and RANGE_READ operations in a single figure.
    Different colors for each backend+operation combination.

    Args:
        data: dict from load_throughput_data()
        mode: 'threads' or 'branches' (determines x-axis label)
        output_dir: directory to save figure (optional)
        range_size: size of range for RANGE_READ operations (default: 100)
        max_threads: maximum number of threads to include in plot (default: None, show all)
        legend_loc: legend location (default: 'outside top'). Options: 'outside top' (two lines above plot), 'outside right', 'best', 'upper left', etc.
    """
    # Filter for READ and RANGE_READ operations
    operations_to_plot = ['READ', 'RANGE_READ']
    available_ops = [op for op in operations_to_plot if op in data]

    if not available_ops:
        print(f"Warning: No READ or RANGE_READ data found")
        return

    fig, ax = plt.subplots(figsize=(12, 7))

    # Define colors for backend+operation combinations
    # Using color scheme from macro_comparison.py
    DOLT_COLOR = "#2176AE"  # Blue
    NEON_COLOR = "#E8554E"  # Red

    style_map = {
        ('DOLT', 'READ'): {'color': DOLT_COLOR, 'marker': 'o', 'linestyle': '-', 'label': 'Dolt - Read'},
        ('DOLT', 'RANGE_READ'): {'color': DOLT_COLOR, 'marker': 's', 'linestyle': '--', 'label': f'Dolt - Range Read (row count = {range_size})'},
        ('NEON', 'READ'): {'color': NEON_COLOR, 'marker': '^', 'linestyle': '-', 'label': 'Neon - Read'},
        ('NEON', 'RANGE_READ'): {'color': NEON_COLOR, 'marker': 'v', 'linestyle': '--', 'label': f'Neon - Range Read (row count = {range_size})'},
        ('KPG', 'READ'): {'color': '#F18F01', 'marker': 'D', 'linestyle': '-', 'label': 'KPG - Read'},
        ('KPG', 'RANGE_READ'): {'color': '#C77100', 'marker': 'p', 'linestyle': '--', 'label': f'KPG - Range Read (row count = {range_size})'},
    }

    # Plot each backend+operation combination
    for operation in available_ops:
        for backend, x_data in sorted(data[operation].items()):
            key = (backend, operation)
            if key not in style_map:
                continue

            # Sort by x-axis value (thread count) and filter by max_threads
            x_values = sorted(x_data.keys())
            if max_threads is not None:
                x_values = [x for x in x_values if x <= max_threads]

            if not x_values:
                continue

            throughputs = [x_data[x] for x in x_values]

            style = style_map[key]
            ax.plot(x_values, throughputs,
                    marker=style['marker'],
                    color=style['color'],
                    linestyle=style['linestyle'],
                    linewidth=3.5,
                    markersize=12,
                    label=style['label'],
                    alpha=0.85)

    # Formatting
    x_label = 'Number of Threads' if mode == 'threads' else 'Number of Branches'

    ax.set_xlabel(f'{x_label} (log scale)', fontsize=20, fontweight='bold')
    ax.set_ylabel('Throughput (ops/sec) (log scale)', fontsize=20, fontweight='bold')

    # Use log2 scale for both axes
    ax.set_xscale('log', base=2)
    ax.set_yscale('log', base=2)

    # Grid
    ax.grid(True, alpha=0.3, linestyle='--', which='both')

    # Legend positioning
    if legend_loc == 'outside top':
        # Position legend above the plot in two lines (2 columns)
        # This creates: Dolt Read, Dolt Range Read on line 1
        #               Neon Read, Neon Range Read on line 2
        ax.legend(fontsize=16, framealpha=0.95, ncol=2,
                 loc='lower center', bbox_to_anchor=(0.5, 1.02))
    elif legend_loc == 'outside right':
        # Position legend to the right of the plot, vertically stacked
        ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.5), ncol=1,
                 fontsize=16, framealpha=0.95)
    else:
        # Use specified location inside plot area
        ax.legend(fontsize=16, framealpha=0.95, ncol=2, loc=legend_loc)

    # Format y-axis
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.0f}'))

    # Increase tick label font size
    ax.tick_params(axis='both', which='major', labelsize=20)
    ax.tick_params(axis='both', which='minor', labelsize=16)

    plt.tight_layout()

    # Save figure
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        suffix = f'_max{max_threads}t' if max_threads else ''
        output_file = os.path.join(output_dir, f'throughput_read_combined_{mode}{suffix}.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved combined read throughput figure to {output_file}")

    plt.close(fig)


def plot_update_operations_combined(data, mode='threads', output_dir=None, range_size=100, max_threads=None, legend_loc='outside top'):
    """
    Plot UPDATE and RANGE_UPDATE operations in a single figure.
    Different colors for each backend+operation combination.

    Args:
        data: dict from load_throughput_data()
        mode: 'threads' or 'branches' (determines x-axis label)
        output_dir: directory to save figure (optional)
        range_size: size of range for RANGE_UPDATE operations (default: 100)
        max_threads: maximum number of threads to include in plot (default: None, show all)
        legend_loc: legend location (default: 'outside top'). Options: 'outside top' (two lines above plot), 'outside right', 'best', 'upper left', etc.
    """
    # Filter for UPDATE and RANGE_UPDATE operations
    operations_to_plot = ['UPDATE', 'RANGE_UPDATE']
    available_ops = [op for op in operations_to_plot if op in data]

    if not available_ops:
        print(f"Warning: No UPDATE or RANGE_UPDATE data found")
        return

    fig, ax = plt.subplots(figsize=(12, 7))

    # Define colors for backend+operation combinations
    # Using color scheme from macro_comparison.py
    DOLT_COLOR = "#2176AE"  # Blue
    NEON_COLOR = "#E8554E"  # Red

    style_map = {
        ('DOLT', 'UPDATE'): {'color': DOLT_COLOR, 'marker': 'o', 'linestyle': '-', 'label': 'Dolt - Update'},
        ('DOLT', 'RANGE_UPDATE'): {'color': DOLT_COLOR, 'marker': 's', 'linestyle': '--', 'label': f'Dolt - Range Update (row count = {range_size})'},
        ('NEON', 'UPDATE'): {'color': NEON_COLOR, 'marker': '^', 'linestyle': '-', 'label': 'Neon - Update'},
        ('NEON', 'RANGE_UPDATE'): {'color': NEON_COLOR, 'marker': 'v', 'linestyle': '--', 'label': f'Neon - Range Update (row count = {range_size})'},
        ('KPG', 'UPDATE'): {'color': '#F18F01', 'marker': 'D', 'linestyle': '-', 'label': 'KPG - Update'},
        ('KPG', 'RANGE_UPDATE'): {'color': '#C77100', 'marker': 'p', 'linestyle': '--', 'label': f'KPG - Range Update (row count = {range_size})'},
    }

    # Plot each backend+operation combination
    for operation in available_ops:
        for backend, x_data in sorted(data[operation].items()):
            key = (backend, operation)
            if key not in style_map:
                continue

            # Sort by x-axis value (thread count) and filter by max_threads
            x_values = sorted(x_data.keys())
            if max_threads is not None:
                x_values = [x for x in x_values if x <= max_threads]

            if not x_values:
                continue

            throughputs = [x_data[x] for x in x_values]

            style = style_map[key]
            ax.plot(x_values, throughputs,
                    marker=style['marker'],
                    color=style['color'],
                    linestyle=style['linestyle'],
                    linewidth=3.5,
                    markersize=12,
                    label=style['label'],
                    alpha=0.85)

    # Formatting
    x_label = 'Number of Threads' if mode == 'threads' else 'Number of Branches'

    ax.set_xlabel(f'{x_label} (log scale)', fontsize=20, fontweight='bold')
    ax.set_ylabel('Throughput (ops/sec) (log scale)', fontsize=20, fontweight='bold')

    # Use log2 scale for both axes
    ax.set_xscale('log', base=2)
    ax.set_yscale('log', base=2)

    # Grid
    ax.grid(True, alpha=0.3, linestyle='--', which='both')

    # Legend positioning
    if legend_loc == 'outside top':
        # Position legend above the plot in two lines (2 columns)
        # This creates: Dolt Update, Dolt Range Update on line 1
        #               Neon Update, Neon Range Update on line 2
        ax.legend(fontsize=16, framealpha=0.95, ncol=2,
                 loc='lower center', bbox_to_anchor=(0.5, 1.02))
    elif legend_loc == 'outside right':
        # Position legend to the right of the plot, vertically stacked
        ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.5), ncol=1,
                 fontsize=16, framealpha=0.95)
    else:
        # Use specified location inside plot area
        ax.legend(fontsize=16, framealpha=0.95, ncol=2, loc=legend_loc)

    # Format y-axis
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.0f}'))

    # Increase tick label font size
    ax.tick_params(axis='both', which='major', labelsize=20)
    ax.tick_params(axis='both', which='minor', labelsize=16)

    plt.tight_layout()

    # Save figure
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        suffix = f'_max{max_threads}t' if max_threads else ''
        output_file = os.path.join(output_dir, f'throughput_update_combined_{mode}{suffix}.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved combined update throughput figure to {output_file}")

    plt.close(fig)


def plot_proportional_all_operations(data, output_dir=None, range_size=100, max_value=None, legend_loc='outside top'):
    """
    Plot read operations (READ, RANGE_READ) in a single figure
    for proportional throughput experiments where branches scale with threads.

    Args:
        data: dict {operation: {backend: {x_value: throughput}}}
        output_dir: directory to save figure (optional)
        range_size: size of range for RANGE operations (default: 100)
        max_value: maximum x-axis value (threads or branches) to include in plot (default: None, show all)
        legend_loc: legend location (default: 'outside top')
    """
    # Filter for read operations only
    operations_to_plot = ['READ', 'RANGE_READ']
    available_ops = [op for op in operations_to_plot if op in data]

    if not available_ops:
        print(f"Warning: No READ or RANGE_READ data found")
        return

    fig, ax = plt.subplots(figsize=(12, 7))

    # Define colors for backend+operation combinations
    # Using color scheme from macro_comparison.py
    DOLT_COLOR = "#2176AE"  # Blue
    NEON_COLOR = "#E8554E"  # Red

    style_map = {
        ('DOLT', 'READ'): {'color': DOLT_COLOR, 'marker': 'o', 'linestyle': '-', 'label': 'Dolt - Read'},
        ('DOLT', 'RANGE_READ'): {'color': DOLT_COLOR, 'marker': 's', 'linestyle': '--', 'label': f'Dolt - Range Read (row count = {range_size})'},
        ('NEON', 'READ'): {'color': NEON_COLOR, 'marker': '^', 'linestyle': '-', 'label': 'Neon - Read'},
        ('NEON', 'RANGE_READ'): {'color': NEON_COLOR, 'marker': 'v', 'linestyle': '--', 'label': f'Neon - Range Read (row count = {range_size})'},
        ('KPG', 'READ'): {'color': '#F18F01', 'marker': 'D', 'linestyle': '-', 'label': 'KPG - Read'},
        ('KPG', 'RANGE_READ'): {'color': '#C77100', 'marker': 'p', 'linestyle': '--', 'label': f'KPG - Range Read (row count = {range_size})'},
    }

    # Plot each backend+operation combination
    for operation in available_ops:
        for backend, x_data in sorted(data[operation].items()):
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
                    color=style['color'],
                    linestyle=style['linestyle'],
                    linewidth=3.5,
                    markersize=12,
                    label=style['label'],
                    alpha=0.85)

    # Formatting
    ax.set_xlabel('Number of Threads/Branches (log scale)', fontsize=20, fontweight='bold')
    ax.set_ylabel('Throughput (ops/sec) (log scale)', fontsize=20, fontweight='bold')

    # Use log2 scale for both axes
    ax.set_xscale('log', base=2)
    ax.set_yscale('log', base=2)

    # Grid
    ax.grid(True, alpha=0.3, linestyle='--', which='both')

    # Legend positioning
    if legend_loc == 'outside top':
        # Position legend above the plot in multiple lines
        ax.legend(fontsize=16, framealpha=0.95, ncol=2,
                 loc='lower center', bbox_to_anchor=(0.5, 1.02))
    elif legend_loc == 'outside right':
        # Position legend to the right of the plot, vertically stacked
        ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.5), ncol=1,
                 fontsize=16, framealpha=0.95)
    else:
        # Use specified location inside plot area
        ax.legend(fontsize=16, framealpha=0.95, ncol=2, loc=legend_loc)

    # Format y-axis
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.0f}'))

    # Increase tick label font size
    ax.tick_params(axis='both', which='major', labelsize=20)
    ax.tick_params(axis='both', which='minor', labelsize=16)

    plt.tight_layout()

    # Save figure
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        suffix = f'_max{max_value}' if max_value else ''
        output_file = os.path.join(output_dir, f'throughput_proportional_all{suffix}.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved proportional throughput figure to {output_file}")

    plt.close(fig)


def print_summary_table(data):
    """Print a summary table of throughput data."""
    for operation in sorted(data.keys()):
        print(f"\n{'='*80}")
        print(f"Operation: {operation}")
        print(f"{'='*80}")

        # Get all thread counts across all backends
        all_threads = set()
        for backend_data in data[operation].values():
            all_threads.update(backend_data.keys())
        all_threads = sorted(all_threads)

        # Print header
        backends = sorted(data[operation].keys())
        header = f"{'Threads':<10}" + "".join([f"{b:<20}" for b in backends])
        print(header)
        print("-" * len(header))

        # Print data rows
        for threads in all_threads:
            row = f"{threads:<10}"
            for backend in backends:
                throughput = data[operation][backend].get(threads, 0)
                row += f"{throughput:<20.0f}"
            print(row)


def main():
    parser = argparse.ArgumentParser(
        description='Plot throughput vs threads for database benchmarks'
    )
    parser.add_argument(
        '--data-dir',
        type=str,
        default='final/micro/tp/ops_tp_single_branch',
        help='Directory containing summary JSON files'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='micro-analysis/figures_throughput',
        help='Directory to save output figures'
    )
    parser.add_argument(
        '--operation',
        type=str,
        default=None,
        help='Specific operation to plot (e.g., READ, RANGE_READ). If not specified, plots all operations.'
    )
    parser.add_argument(
        '--table',
        action='store_true',
        help='Print summary table to console'
    )
    parser.add_argument(
        '--combined-read',
        action='store_true',
        help='Create a single combined plot for READ and RANGE_READ operations'
    )
    parser.add_argument(
        '--combined-update',
        action='store_true',
        help='Create a single combined plot for UPDATE and RANGE_UPDATE operations'
    )
    parser.add_argument(
        '--proportional-all',
        action='store_true',
        help='Create a single combined plot for read operations (READ, RANGE_READ) for proportional throughput (branches scale with threads)'
    )
    parser.add_argument(
        '--range-size',
        type=int,
        default=100,
        help='Range size for RANGE_READ/RANGE_UPDATE operations (default: 100)'
    )
    parser.add_argument(
        '--max-threads',
        type=int,
        default=None,
        help='Maximum number of threads to include in the plot (default: None, show all)'
    )
    parser.add_argument(
        '--legend-loc',
        type=str,
        default='outside top',
        help='Legend location (default: outside top). Options: "outside top" (two lines above plot), "outside right" (vertical to right of plot), best, upper left, upper right, lower left, lower right, upper center, lower center, center left, center right, center'
    )
    parser.add_argument(
        '--aggregated',
        action='store_true',
        help='Use aggregated throughput (total ops/sec across all threads) instead of average throughput (per-thread average)'
    )

    args = parser.parse_args()

    # Load data
    print(f"Loading throughput data from {args.data_dir}...")
    throughput_type = "aggregated" if args.aggregated else "average"
    print(f"Using {throughput_type} throughput metric")
    data, mode = load_throughput_data(args.data_dir, use_aggregated=args.aggregated)

    if not data:
        print("No data loaded. Check the data directory path.")
        return

    print(f"Found {len(data)} operation(s): {', '.join(sorted(data.keys()))}")
    print(f"Detected mode: {'Threads' if mode == 'threads' else 'Branches'} on x-axis")

    # Print table if requested
    if args.table:
        print_summary_table(data)

    # Plot
    if args.combined_read:
        # Create combined READ and RANGE_READ plot
        plot_read_operations_combined(data, mode, args.output_dir,
                                       range_size=args.range_size,
                                       max_threads=args.max_threads,
                                       legend_loc=args.legend_loc)
    elif args.combined_update:
        # Create combined UPDATE and RANGE_UPDATE plot
        plot_update_operations_combined(data, mode, args.output_dir,
                                        range_size=args.range_size,
                                        max_threads=args.max_threads,
                                        legend_loc=args.legend_loc)
    elif args.proportional_all:
        # Create proportional plot for all operations
        plot_proportional_all_operations(data, args.output_dir,
                                          range_size=args.range_size,
                                          max_value=args.max_threads,
                                          legend_loc=args.legend_loc)
    elif args.operation:
        # Plot single operation
        if args.operation in data:
            plot_throughput_comparison(data, args.operation, mode, args.output_dir)
        else:
            print(f"Error: Operation '{args.operation}' not found in data.")
            print(f"Available operations: {', '.join(sorted(data.keys()))}")
    else:
        # Plot all operations
        plot_all_operations(data, mode, args.output_dir)


if __name__ == '__main__':
    main()
