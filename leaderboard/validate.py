"""Check leaderboard result files. Uses only the standard library.

From the repository root:
    python -m leaderboard.validate [file ...]
Without arguments it checks every <system>/results/<YYYYMMDD>/<machine>.json.
Exits non-zero if any file has a problem.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import re
import sys
from pathlib import Path

BOARD = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
MACHINE_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}")
IDENTITY = ("system", "proprietary", "hosted", "tuned", "tags")
# Every key of a result file and its type. float also admits int; no type admits bool.
FIELDS = {
    "schema_version": int, "run_id": str, "system": str, "date": str, "machine": str,
    "proprietary": str, "hosted": str, "tuned": str, "tags": list, "version": str,
    "suite": str, "suite_sha256": str, "seed": int, "commit": str, "dirty": bool,
    "dataset_sha256": str, "python": str, "lock_sha256": str,
    "provision_time": float, "load_time": float, "result": list,
}
SECRET_KEY = re.compile(r"pass(word)?|secret|token|api[-_]?key|credential", re.I)
SECRET_VALUE = re.compile(r"://[^/\s@]*:[^/\s@]*@|(pass(word)?|secret|token|api[-_]?key)\s*[=:]", re.I)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def has_type(value, kind: type) -> bool:
    return type(value) in (int, float) if kind is float else type(value) is kind


def parse_day(name: str) -> dt.date | None:
    """A date directory's date, or None unless the name is a real YYYYMMDD date."""
    try:
        return dt.datetime.strptime(name, "%Y%m%d").date() if re.fullmatch(r"\d{8}", name) else None
    except ValueError:
        return None


def credentials(value, where: str = "$") -> list[str]:
    """Where keys or strings look like credentials. Never includes the value itself."""
    if isinstance(value, dict):
        return [p for k, v in value.items()
                for p in ([f"{where}.{k}"] if SECRET_KEY.search(k) else []) + credentials(v, f"{where}.{k}")]
    if isinstance(value, list):
        return [p for i, v in enumerate(value) for p in credentials(v, f"{where}[{i}]")]
    return [where] if isinstance(value, str) and SECRET_VALUE.search(value) else []


def validate(path: Path, board: Path = BOARD) -> list[str]:
    """Every problem with one result file. An empty list means the file is valid."""
    try:
        sysdir, results, day_dir, name = path.resolve().relative_to(board.resolve()).parts
    except ValueError:
        return [f"not at <system>/results/<YYYYMMDD>/<machine>.json under {board}"]
    problems = []
    if results != "results" or not name.endswith(".json"):
        problems.append("not at <system>/results/<YYYYMMDD>/<machine>.json")
    if (day := parse_day(day_dir)) is None:
        problems.append(f"date directory {day_dir!r} is not a YYYYMMDD date")
    if not MACHINE_RE.fullmatch(path.stem):
        problems.append(f"machine {path.stem!r} must match {MACHINE_RE.pattern}")
    try:
        meta = json.loads((board / sysdir / "system.json").read_text())
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as e:
        return problems + [f"cannot read {sysdir}/system.json or the file as JSON: {e}"]
    if not isinstance(data, dict):
        return problems + ["not a JSON object"]
    problems += [f"{where} looks like a credential" for where in credentials(data)]

    if "error" in data:
        if set(data) != {"error"} or not isinstance(data["error"], str) or not data["error"].strip():
            problems.append("an error file holds only a non-empty error string")
        return problems

    if missing := sorted(FIELDS.keys() - data.keys()):
        problems.append(f"missing keys: {missing}")
    if unexpected := sorted(data.keys() - FIELDS.keys()):
        problems.append(f"unexpected keys: {unexpected}")
    problems += [f"{k} must be {t.__name__}" for k, t in FIELDS.items() if k in data and not has_type(data[k], t)]
    if problems:
        return problems  # the checks below rely on every key being present and typed

    suite = json.loads((board / "suite.json").read_text())
    current = {"schema_version": SCHEMA_VERSION, "suite": suite["suite"], "seed": suite["seed"],
               "suite_sha256": sha256(board / "suite.json"), "dataset_sha256": suite["dataset"]["sha256"]}
    problems += [f"{k} differs from the current suite.json" for k, v in current.items() if data[k] != v]
    problems += [f"{k} differs from {sysdir}/system.json" for k in IDENTITY if data[k] != meta.get(k)]
    if day and data["date"] != day.isoformat():
        problems.append(f"date {data['date']} differs from its directory")
    if data["machine"] != path.stem:
        problems.append("machine differs from the file name")
    problems += [f"{k} is empty" for k in ("version", "python") if not data[k].strip()]
    if not re.fullmatch(r"[0-9a-f]{40}", data["commit"]):
        problems.append("commit is not a full git SHA")
    if data["dirty"]:
        problems.append("the run had uncommitted changes")
    problems += [f"{k} must be finite and positive" for k in ("provision_time", "load_time")
                 if not (math.isfinite(data[k]) and data[k] > 0)]
    rows, tries = data["result"], suite["tries"]
    if len(rows) != len(suite["rows"]) or any(type(r) is not list or len(r) != tries for r in rows):
        problems.append(f"result must have {len(suite['rows'])} rows of {tries} tries")
    elif any(c is not None and not (has_type(c, float) and math.isfinite(c) and c >= 0) for r in rows for c in r):
        problems.append("every try must be null or a finite, non-negative number")
    return problems


def main() -> int:
    paths = [Path(p) for p in sys.argv[1:]] or sorted(BOARD.glob("*/results/*/*.json"))
    failed = {path: problems for path in paths if (problems := validate(path))}
    for path, problems in failed.items():
        for problem in problems:
            print(f"{path}: {problem}", file=sys.stderr)
    print(f"{len(paths) - len(failed)} of {len(paths)} files valid")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
