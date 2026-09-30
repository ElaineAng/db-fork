"""Checks for the leaderboard and the harness fixes it relies on.

Run from the repository root: python -m unittest leaderboard.test_leaderboard
These tests need no database server.
"""

from __future__ import annotations

import json
import os
import random
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import psycopg2.errors
import pymysql
import requests

from dblib.db_api import DBToolSuite
from dblib.neon import NeonToolSuite
from dblib.tiger import TigerToolSuite
from leaderboard.build import build
from leaderboard.run import SYSTEMS, RunError, exclusive, scrub
from leaderboard.validate import BOARD, SCHEMA_VERSION, sha256, validate
from microbench.datagen import DynamicDataGenerator
from microbench.operations.crud import is_duplicate_key
from util.import_db import _split_password

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

    def test_psql_password_leaves_the_command_line(self) -> None:
        uri, password = _split_password("postgresql://owner:p%40ss:w@host.example/db?sslmode=require")
        self.assertEqual((uri, password), ("postgresql://owner@host.example/db?sslmode=require", "p@ss:w"))
        self.assertEqual(_split_password("postgresql://postgres@localhost/db"), ("postgresql://postgres@localhost/db", None))

    def test_neon_delete_waits_out_locks_and_its_operations(self) -> None:
        locked = requests.Response()
        locked.status_code = 423
        replies = iter([requests.exceptions.HTTPError(response=locked), {"operations": [{"id": "op1"}]},
                        {"operation": {"status": "running"}}, {"operation": {"status": "finished"}}])

        def fake_request(method: str, endpoint: str, **kwargs) -> dict:
            reply = next(replies)
            if isinstance(reply, Exception):
                raise reply
            return reply

        suite = NeonToolSuite.__new__(NeonToolSuite)
        suite.project_id, suite._all_branches = "project", {"row_read": ("br-1", "")}
        with mock.patch.object(NeonToolSuite, "_request", side_effect=fake_request), \
                mock.patch("dblib.neon.POLL_INTERVAL", 0):
            suite._delete_branch_impl("row_read", "")
        self.assertEqual(list(replies), [])
        self.assertNotIn("row_read", suite._all_branches)

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
        for system in ("dolt", "neon"):
            shutil.copytree(BOARD / system, self.board / system, ignore=shutil.ignore_patterns("results"))
        self.suite = json.loads((BOARD / "suite.json").read_text())

    def payload(self, date: str = "2026-09-30", **changes) -> dict:
        meta = json.loads((BOARD / "dolt" / "system.json").read_text())
        return {"schema_version": SCHEMA_VERSION, "run_id": "0123abcd", **meta, "date": date, "machine": "box",
                "version": "0.54.10", "suite": self.suite["suite"], "suite_sha256": sha256(BOARD / "suite.json"),
                "seed": self.suite["seed"], "commit": "0" * 40, "dirty": False,
                "dataset_sha256": self.suite["dataset"]["sha256"], "python": "3.13.14", "lock_sha256": "0" * 64,
                "provision_time": 0.07, "load_time": 2.3,
                "result": [[0.01] * self.suite["tries"] for _ in self.suite["rows"]], **changes}

    def write(self, payload: dict, day_dir: str = "20260930", system: str = "dolt") -> Path:
        path = self.board / system / "results" / day_dir / "box.json"
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

    def test_hosted_results_need_their_fields(self) -> None:
        def hosted(**changes) -> dict:
            meta = json.loads((BOARD / "neon" / "system.json").read_text())
            return self.payload(**{**meta, "region": "aws-us-east-1", "rtt_ms": 12.5, "client_location": "US East",
                                   "service": {"region": "aws-us-east-1", "pg_version": 17}, **changes})

        self.assertEqual(validate(self.write(hosted(), system="neon"), self.board), [])
        cases = {  # label: file, system, the problem it must report
            "no round trip": ({k: v for k, v in hosted().items() if k != "rtt_ms"}, "neon", "missing keys"),
            "a service key outside the allowlist": (hosted(service={"api_hint": "x"}), "neon", "allowlist"),
            "hosted fields on a local system": (self.payload(region="us-east-1"), "dolt", "unexpected keys"),
        }
        for label, (payload, system, problem) in cases.items():
            with self.subTest(label):
                problems = validate(self.write(payload, system=system), self.board)
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


class Runner(unittest.TestCase):
    def test_a_second_run_of_a_system_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as runs, mock.patch("leaderboard.run.RUNS", Path(runs)):
            with exclusive("neon"), self.assertRaises(RunError):
                with exclusive("neon"):
                    pass

    def test_errors_never_carry_a_uri_password(self) -> None:
        self.assertEqual(scrub("connect failed: postgresql://owner:hunter2@ep-1.neon.tech/db?sslmode=require"),
                         "connect failed: postgresql://owner:***@ep-1.neon.tech/db?sslmode=require")

    def test_tiger_cleanup_deletes_only_the_runs_services_root_last(self) -> None:
        listed = [{"service_id": "s1", "name": "bb_1a2b"}, {"service_id": "s2", "name": "bb_1a2b_chain_1"},
                  {"service_id": "s3", "name": "bb_1a2bc"}, {"service_id": "s4", "name": "analytics"}]
        deleted = []
        with mock.patch.dict(os.environ, {"TIGER_PROJECT_ID": "project"}), \
                mock.patch.object(TigerToolSuite, "list_tiger_services", side_effect=[listed, []]), \
                mock.patch.object(TigerToolSuite, "delete_tiger_service", side_effect=lambda p, s: deleted.append(s)):
            SYSTEMS["tiger"].delete("bb_1a2b")
        self.assertEqual(deleted, ["s2", "s1"])


if __name__ == "__main__":
    unittest.main()
