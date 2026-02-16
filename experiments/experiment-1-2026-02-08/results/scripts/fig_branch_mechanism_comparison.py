#!/usr/bin/env python3
"""Diagram: Dolt O(1) branch creation vs PostgreSQL file_copy O(F) per-file clone loop."""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

fig, (ax_dolt, ax_pg) = plt.subplots(1, 2, figsize=(14, 8))

# Colors
C_BOX = "#e8e8e8"
C_REF = "#4a90d9"
C_HASH = "#d9534f"
C_CHUNK = "#5cb85c"
C_ARROW = "#333333"
C_CLONE = "#e67e22"
C_DIR = "#f5f5dc"
C_FILE = "#dce6f0"
C_NEWFILE = "#fde8d0"
C_INSIGHT = "#fff3cd"

def draw_box(ax, xy, w, h, label, color, fontsize=9, bold=False, text_color="black"):
    box = FancyBboxPatch(xy, w, h, boxstyle="round,pad=0.02",
                         facecolor=color, edgecolor="#555", linewidth=1.2)
    ax.add_patch(box)
    weight = "bold" if bold else "normal"
    ax.text(xy[0] + w/2, xy[1] + h/2, label, ha="center", va="center",
            fontsize=fontsize, fontweight=weight, color=text_color)
    return box

def draw_arrow(ax, start, end, color=C_ARROW, style="-|>", lw=1.5, label="", label_offset=(0, 0)):
    arrow = FancyArrowPatch(start, end, arrowstyle=style, color=color,
                            lw=lw, mutation_scale=15)
    ax.add_patch(arrow)
    if label:
        mid = ((start[0]+end[0])/2 + label_offset[0],
               (start[1]+end[1])/2 + label_offset[1])
        ax.text(mid[0], mid[1], label, ha="center", va="center",
                fontsize=7.5, color=color, fontstyle="italic")

# =========================================================================
# LEFT PANEL: Dolt O(1)
# =========================================================================
ax_dolt.set_xlim(-0.5, 10)
ax_dolt.set_ylim(-1, 10.5)
ax_dolt.set_aspect("equal")
ax_dolt.axis("off")
ax_dolt.set_title("Dolt:  $O(1)$ — Single Pointer", fontsize=13, fontweight="bold", pad=12)

# Branch refs
draw_box(ax_dolt, (0.5, 9), 2.5, 0.7, "main", C_REF, fontsize=10, bold=True, text_color="white")
draw_box(ax_dolt, (0.5, 8), 2.5, 0.7, "branch_1", C_REF, fontsize=10, bold=True, text_color="white")
draw_box(ax_dolt, (0.5, 7), 2.5, 0.7, "branch_N", C_REF, fontsize=10, bold=True, text_color="white")
ax_dolt.text(1.75, 6.5, "...", ha="center", va="center", fontsize=14, color="#888")

# Root hash
draw_box(ax_dolt, (5.5, 8), 3.2, 0.7, "Root Hash (commit)", C_HASH, fontsize=9, bold=True, text_color="white")

# Arrows from refs to root hash
draw_arrow(ax_dolt, (3.0, 9.35), (5.5, 8.7), color=C_REF)
draw_arrow(ax_dolt, (3.0, 8.35), (5.5, 8.35), color=C_REF)
draw_arrow(ax_dolt, (3.0, 7.35), (5.5, 8.0), color=C_REF)

# NEW branch arrow (highlighted)
ax_dolt.annotate("", xy=(0.5, 7.35), xytext=(-0.3, 7.35),
                 arrowprops=dict(arrowstyle="-|>", color=C_CLONE, lw=2.5))
ax_dolt.text(-0.4, 7.8, "NEW", ha="center", va="center", fontsize=8,
             color=C_CLONE, fontweight="bold")
ax_dolt.text(-0.4, 7.55, "SetHead(hash)", ha="center", va="center", fontsize=7, color=C_CLONE)

# Prolly tree
# Level 1 (root splits into chunks)
for i, x in enumerate([5.0, 6.8, 8.6]):
    draw_box(ax_dolt, (x, 6.2), 1.2, 0.6, f"chunk", C_CHUNK, fontsize=7.5, text_color="white")

draw_arrow(ax_dolt, (7.1, 8.0), (5.6, 6.8), color="#888", lw=1)
draw_arrow(ax_dolt, (7.1, 8.0), (7.4, 6.8), color="#888", lw=1)
draw_arrow(ax_dolt, (7.1, 8.0), (9.2, 6.8), color="#888", lw=1)

# Level 2 (leaf chunks)
for i, x in enumerate([4.2, 5.5, 6.1, 7.4, 8.0, 9.2]):
    draw_box(ax_dolt, (x, 4.8), 0.9, 0.5, "", C_CHUNK, fontsize=6, text_color="white")

for parent_x, children in [(5.6, [4.65, 5.95]), (7.4, [6.55, 7.85]), (9.2, [8.45, 9.65])]:
    for cx in children:
        draw_arrow(ax_dolt, (parent_x, 6.2), (cx, 5.3), color="#888", lw=0.8)

ax_dolt.text(7.1, 4.2, "Shared Prolly Tree Chunks\n(content-addressed, deduplicated)",
             ha="center", va="center", fontsize=8.5, color="#555", fontstyle="italic")

# Cost annotation
cost_box = FancyBboxPatch((0.2, 1.8), 9.3, 1.8, boxstyle="round,pad=0.15",
                          facecolor="#e8f5e9", edgecolor="#5cb85c", linewidth=1.5)
ax_dolt.add_patch(cost_box)
ax_dolt.text(4.85, 3.0, "Cost = 1 pointer write", ha="center", va="center",
             fontsize=11, fontweight="bold", color="#2e7d32")
ax_dolt.text(4.85, 2.3, "Independent of database size, file count, or topology.\n"
             "Source: doltdb.go → SetHead(ctx, ds, commit_hash)",
             ha="center", va="center", fontsize=8, color="#555")

# =========================================================================
# RIGHT PANEL: PostgreSQL file_copy O(F)
# =========================================================================
ax_pg.set_xlim(-0.5, 10.5)
ax_pg.set_ylim(-1, 10.5)
ax_pg.set_aspect("equal")
ax_pg.axis("off")
ax_pg.set_title("PostgreSQL file_copy:  $O(F)$ — Per-File Clone Loop",
                fontsize=13, fontweight="bold", pad=12)

# Parent directory
parent_box = FancyBboxPatch((0.2, 3.5), 3.6, 6.5, boxstyle="round,pad=0.1",
                            facecolor=C_DIR, edgecolor="#888", linewidth=1.5)
ax_pg.add_patch(parent_box)
ax_pg.text(2.0, 9.65, "Parent: base/<oid>/", ha="center", va="center",
           fontsize=9.5, fontweight="bold", color="#555")

# Parent files
parent_files = [
    ("heap_seg_0", 9.0),
    ("heap_seg_1", 8.3),
    ("heap_seg_2", 7.6),
    ("idx_orders_pk", 6.9),
    ("fsm", 6.2),
    ("vm", 5.5),
    ("toast", 4.8),
]
file_boxes_parent = []
for fname, y in parent_files:
    b = draw_box(ax_pg, (0.5, y), 3.0, 0.5, fname, C_FILE, fontsize=8)
    file_boxes_parent.append((0.5 + 3.0, y + 0.25))  # right edge center

# New branch directory
new_box = FancyBboxPatch((6.5, 3.5), 3.6, 6.5, boxstyle="round,pad=0.1",
                         facecolor=C_DIR, edgecolor=C_CLONE, linewidth=2)
ax_pg.add_patch(new_box)
ax_pg.text(8.3, 9.65, "New: base/<new_oid>/", ha="center", va="center",
           fontsize=9.5, fontweight="bold", color=C_CLONE)

# New files
file_boxes_new = []
for fname, y in parent_files:
    b = draw_box(ax_pg, (6.8, y), 3.0, 0.5, fname, C_NEWFILE, fontsize=8)
    file_boxes_new.append((6.8, y + 0.25))  # left edge center

# Clone arrows
for (px, py), (nx, ny) in zip(file_boxes_parent, file_boxes_new):
    draw_arrow(ax_pg, (px + 0.1, py), (nx - 0.1, ny), color=C_CLONE, lw=1.3,
               label="clonefile()", label_offset=(0, 0.3))

# F label
ax_pg.annotate("", xy=(4.1, 4.5), xytext=(4.1, 9.3),
               arrowprops=dict(arrowstyle="<->", color="#d9534f", lw=2))
ax_pg.text(4.45, 6.9, "F files", ha="left", va="center",
           fontsize=10, fontweight="bold", color="#d9534f", rotation=90)

# Cost annotation
cost_box = FancyBboxPatch((0.2, 1.8), 9.8, 1.8, boxstyle="round,pad=0.15",
                          facecolor="#fce4ec", edgecolor="#d9534f", linewidth=1.5)
ax_pg.add_patch(cost_box)
ax_pg.text(5.1, 3.05, "Cost = F × clonefile() + WAL + catalog updates", ha="center", va="center",
           fontsize=10.5, fontweight="bold", color="#c62828")
ax_pg.text(5.1, 2.3, "F grows with parent size. Spine: parent accumulates N−1\n"
           "workloads → more heap segments → F increases → higher cost.",
           ha="center", va="center", fontsize=8, color="#555")

# Source annotation
ax_pg.text(5.1, 0.6, "Source: PostgreSQL copydir.c — while(ReadDir()) { clone_file(fromfile, tofile); }",
           ha="center", va="center", fontsize=7.5, color="#888", fontstyle="italic")

fig.tight_layout(w_pad=3)
out = "/Users/garfield/PycharmProjects/db-fork/experiments/experiment-1-2026-02-08/results/figures/fig_branch_mechanism_comparison.jpg"
fig.savefig(out, dpi=200, bbox_inches="tight", facecolor="white")
plt.close(fig)
print(f"Saved: {out}")
