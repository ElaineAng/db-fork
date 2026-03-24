"""Shared constants and helpers for visualization scripts."""

import matplotlib.pyplot as plt
import pandas as pd

# Operation type enum values to names (from task.proto)
OP_TYPE_NAMES = {
    0: "UNSPECIFIED",
    1: "BRANCH",
    2: "CONNECT",
    3: "READ",
    4: "INSERT",
    5: "UPDATE",
    6: "RANGE_UPDATE (per-key)",
}

# Colors for each operation type
OP_COLORS = {
    0: "#888888",
    1: "#1f77b4",
    2: "#ff7f0e",
    3: "#2ca02c",
    4: "#d62728",
    5: "#9467bd",
    6: "#8c564b",
}


def process_range_updates(df: pd.DataFrame) -> pd.DataFrame:
    """Mark RANGE_UPDATE ops (op_type 5 with num_keys_touched > 1) as
    synthetic op_type 6 and compute per-key latency."""
    if df.empty or "num_keys_touched" not in df.columns:
        return df

    range_update_mask = (df["op_type"] == 5) & (df["num_keys_touched"] > 1)
    if range_update_mask.any():
        df = df.copy()
        df.loc[range_update_mask, "op_type"] = 6
        df.loc[range_update_mask, "latency"] = (
            df.loc[range_update_mask, "latency"]
            / df.loc[range_update_mask, "num_keys_touched"]
        )
    return df


def auto_scale_storage(values_bytes: pd.Series) -> tuple[pd.Series, str]:
    """Pick a human-readable unit for a series of byte values.

    Returns (scaled_series, unit_label) where unit_label is one of
    'B', 'KB', 'MB', 'GB'.
    """
    max_val = values_bytes.max()
    if max_val >= 1 << 30:
        return values_bytes / (1 << 30), "GB"
    elif max_val >= 1 << 20:
        return values_bytes / (1 << 20), "MB"
    elif max_val >= 1 << 10:
        return values_bytes / (1 << 10), "KB"
    else:
        return values_bytes, "B"


def save_or_show(fig, output_path: str | None, dpi: int = 150):
    """Save *fig* to *output_path* (if given) or show interactively, then close."""
    if output_path:
        fig.savefig(output_path, dpi=dpi, bbox_inches="tight")
        print(f"Saved figure to {output_path}")
    else:
        plt.show()
    plt.close(fig)
