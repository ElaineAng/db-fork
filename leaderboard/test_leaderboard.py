"""Checks for the leaderboard and the harness fixes it relies on.

Run from the repository root: python -m unittest leaderboard.test_leaderboard
These tests need no database server.
"""

from __future__ import annotations

import json
import random
import shutil
import tempfile
import unittest
from pathlib import Path

import psycopg2.errors
import pymysql

from dblib.db_api import DBToolSuite
from leaderboard.build import build
from leaderboard.validate import BOARD, SCHEMA_VERSION, sha256, validate
from microbench.datagen import DynamicDataGenerator
from microbench.operations.crud import is_duplicate_key

ITEM_DDL = """CREATE TABLE item (
    i_id INT NOT NULL,
    i_im_id INT,
    i_name VARCHAR(24),
    i_price DECIMAL(5, 2),
    i_data VARCHAR(50),
    PRIMARY KEY (i_id)
);"""


class HarnessFixes(unittest.TestCase):
    def test_base_delete_raises(self) -> None:
        with self.assertRaises(NotImplementedError):
            DBToolSuite._delete_branch_impl(object(), "branch", "id")

    def test_duplicate_key_on_each_driver(self) -> None:
        doltgres = psycopg2.errors.InternalError_("duplicate primary key given: [1] (errno 1062) (sqlstate HY000)")
        self.assertTrue(is_duplicate_key(psycopg2.errors.UniqueViolation("duplicate key")))
        self.assertTrue(is_duplicate_key(pymysql.err.IntegrityError(1062, "duplicate primary key given: [1]")))
        self.assertTrue(is_duplicate_key(doltgres))

    def test_other_errors_are_not_duplicates(self) -> None:
        self.assertFalse(is_duplicate_key(pymysql.err.IntegrityError(1452, "foreign key constraint fails")))
        self.assertFalse(is_duplicate_key(psycopg2.errors.InternalError_("nothing to commit (errno 1105)")))
        self.assertFalse(is_duplicate_key(None))

    def test_seeded_generator_repeats_rows(self) -> None:
        def rows(seed: int) -> list[dict]:
            gen = DynamicDataGenerator(ITEM_DDL, random.Random(seed))
            return [gen.generate_row() for _ in range(5)]

        self.assertEqual(rows(7), rows(7))
        self.assertNotEqual(rows(7), rows(8))

    def test_generated_item_rows_fit_the_columns(self) -> None:
        gen = DynamicDataGenerator(ITEM_DDL, random.Random(1))
        for _ in range(200):
            row = gen.generate_row()
            self.assertLessEqual(row["i_price"], 999.99)
            self.assertLessEqual(len(row["i_name"]), 24)
            self.assertLessEqual(len(row["i_data"]), 50)


class ResultFiles(unittest.TestCase):
    """The validator and the build, on a scratch copy of the board."""

    def setUp(self) -> None:
        self.board = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.board)
        shutil.copy(BOARD / "suite.json", self.board)
        shutil.copytree(BOARD / "dolt", self.board / "dolt", ignore=shutil.ignore_patterns("results"))
        self.suite = json.loads((BOARD / "suite.json").read_text())

    def payload(self, date: str = "2026-09-30", **changes) -> dict:
        meta = json.loads((BOARD / "dolt" / "system.json").read_text())
        return {"schema_version": SCHEMA_VERSION, "run_id": "0123abcd", **meta, "date": date, "machine": "box",
                "version": "0.54.10", "suite": self.suite["suite"], "suite_sha256": sha256(BOARD / "suite.json"),
                "seed": self.suite["seed"], "commit": "0" * 40, "dirty": False,
                "dataset_sha256": self.suite["dataset"]["sha256"], "python": "3.13.14", "lock_sha256": "0" * 64,
                "provision_time": 0.07, "load_time": 2.3,
                "result": [[0.01] * self.suite["tries"] for _ in self.suite["rows"]], **changes}

    def write(self, payload: dict, day_dir: str = "20260930") -> Path:
        path = self.board / "dolt" / "results" / day_dir / "box.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload))
        return path

    def test_valid_file_passes(self) -> None:
        self.assertEqual(validate(self.write(self.payload()), self.board), [])

    def test_bad_files_are_rejected(self) -> None:
        good = self.payload()["result"]
        cases = {  # label: file, date directory, the problem it must report
            "a short row": (self.payload(result=[good[0][:2], *good[1:]]), "20260930", "rows of 3 tries"),
            "a wrong suite digest": (self.payload(suite_sha256="0" * 64), "20260930", "suite_sha256 differs"),
            "a bad date directory": (self.payload(), "2026-09-30", "not a YYYYMMDD date"),
            "a boolean timing": (self.payload(result=[[True, 0.01, 0.01], *good[1:]]), "20260930", "every try"),
            "a secret-looking field": (self.payload(version="postgresql://u:pw@host/db"), "20260930", "credential"),
            "a capped run": (self.payload(max_batch=5), "20260930", "max_batch"),
            "a dirty run": (self.payload(dirty=True), "20260930", "uncommitted changes"),
        }
        for label, (payload, day_dir, problem) in cases.items():
            with self.subTest(label):
                problems = validate(self.write(payload, day_dir), self.board)
                self.assertTrue(any(problem in p for p in problems), problems)

    def test_newest_date_wins(self) -> None:
        self.write(self.payload("2026-09-29"), "20260929")
        self.write(self.payload(load_time=9.0))
        entries, excluded = build(self.board)
        self.assertEqual([(e["load_time"], e["source"]) for e in entries], [(9.0, "dolt/results/20260930/box.json")])
        self.assertEqual(excluded, [])

    def test_newer_error_hides_older_result(self) -> None:
        self.write(self.payload("2026-09-29"), "20260929")
        self.write({"error": "RunError: server failed its check for 60 s"})
        entries, excluded = build(self.board)
        self.assertEqual(entries, [])
        self.assertEqual([e["reason"] for e in excluded], ["RunError: server failed its check for 60 s"])


if __name__ == "__main__":
    unittest.main()
