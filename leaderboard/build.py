"""Collect the newest result per system and machine into data.js for the page.

From the repository root:
    python -m leaderboard.build
The page loads data.js with a script tag, so it also works when opened from disk.
"""

from __future__ import annotations

import json
from pathlib import Path

from leaderboard.validate import BOARD, validate


def newest(board: Path) -> list[Path]:
    """For each system and machine, the file in the newest date directory."""
    found: dict[tuple[str, str], Path] = {}
    for path in sorted(board.glob("*/results/*/*.json")):  # YYYYMMDD directories sort by date
        found[path.parts[-4], path.stem] = path
    return list(found.values())


def build(board: Path = BOARD) -> tuple[list[dict], list[dict]]:
    """Entries to rank, and excluded entries with their reasons. A newest file that
    is an error or invalid excludes its system and machine; no older file stands in."""
    entries, excluded = [], []
    for path in newest(board):
        source = path.relative_to(board).as_posix()
        if problems := validate(path, board):
            reason = "invalid: " + "; ".join(problems)
        elif "error" in (data := json.loads(path.read_text())):
            reason = data["error"]
        else:
            entries.append({**data, "source": source})
            continue
        excluded.append({"system": path.parts[-4], "machine": path.stem, "source": source, "reason": reason})
    return entries, excluded


def main() -> None:
    entries, excluded = build()
    suite = json.loads((BOARD / "suite.json").read_text())
    (BOARD / "data.js").write_text("".join(
        f"const {name} = {json.dumps(value, indent=1)};\n"
        for name, value in (("suite", suite), ("data", entries), ("excluded", excluded))))
    print(f"data.js: {len(entries)} entries, {len(excluded)} excluded")


if __name__ == "__main__":
    main()
