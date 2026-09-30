# BranchBench leaderboard

BranchBench ranks databases that support branches on branch operations and on
point and range reads and writes. It follows ClickBench's structure: one directory per
system, one shared runner, dated result files, a validator, a build step that
writes `data.js`, and a static page that ranks the results.

Everything runs on one machine. The target databases are either local servers
(Doltgres, Dolt) or hosted services (Neon, Tiger Cloud, Xata).

## Quick start

Prerequisites: Python 3.13, `dolt` and `doltgres` on `PATH`, and `psql` from libpq.

```bash
# Once, from the repository root
python3.13 -m venv .venv && source .venv/bin/activate
pip install -r requirements.lock && pip install -e . --no-deps
python build_protos.py
export PATH="$(brew --prefix libpq)/bin:$PATH"   # libpq is keg-only

# Each run: commit first, because the runner refuses uncommitted changes
python -m leaderboard.run dolt --machine apple-m5-32gb
python -m leaderboard.run dolt_mysql --machine apple-m5-32gb
python -m leaderboard.validate
python -m leaderboard.build
open leaderboard/index.html
```

A local run starts the server, creates a database named `bb_<run id>`, loads
`db_setup/ch_benchmark_seed.sql`, measures, deletes the database, and stops the
server. A full run takes about 1 to 2 minutes.

For a short trial, add `--max-batch 5 --allow-dirty --out /tmp/lb`. The validator
rejects such results, so they never reach the page.

## Hosted systems

Store each key in Bitwarden as an item named after its variable, and load it
when launching the run. No key is ever written to a file.

| system | variables |
|---|---|
| `neon` | `NEON_API_KEY_ORG` |
| `tiger` | `TIGER_ACCESS_KEY`, `TIGER_SECRET_KEY`, `TIGER_PROJECT_ID` |
| `xata` | `XATA_API_KEY`, `XATA_ORGANIZATION_ID` |

```bash
eval "$(bw-load NEON_API_KEY_ORG='NEON_API_KEY_ORG')"
python -m leaderboard.run neon --machine apple-m5-32gb --client-location "US East"
```

Before creating anything, the runner checks that every variable is set and that
the keys can reach the account. A failed check publishes nothing. Every hosted
statement times out after 120 s.

What a run creates, all named after `bb_<run id>` and all deleted at the end:

- Neon and Xata: one project per run.
- Tiger: one root service, plus one forked service per branch, named
  `bb_<run id>_<branch>`, inside the project in `TIGER_PROJECT_ID`.

At most 5 branches or forks are alive at once, besides the root. A run creates
16 branches. Tiger and Xata averaged about 55 s per branch in earlier measurements,
so their runs should take 15 to 25 minutes.

If a run is killed, `leaderboard/.runs/<system>.journal.json` keeps the name, and
the next run of that system deletes everything with that name before it starts.

## Mock data

Until Neon, Tiger Cloud, and Xata have real runs, the page shows mock entries for
them, from `leaderboard/mock/<system>.json`. These are illustrative numbers, not
measurements. Branch create and connect follow earlier hosted measurements in
`run_stats_final`, and data operations assume about 20 ms per statement from a
client in US East. Each file's `basis` says so.

- The build adds a mock only for a system with no result file. So a system's first
  real result, or even its first error record, replaces its mock.
- The page labels mock entries "(mock)", fades their bars, and shows a notice. The
  Data filter hides them.
- Mock files sit outside `results/`, and the validator rejects a result file that
  carries a `mock` key, so a mock cannot be published as a measurement.
- To remove them all, delete `leaderboard/mock/` and run the build again.

## What is measured

The suite in `suite.json` runs 8 rows, 3 tries each, on the `item` table:

| row | one operation | batch |
|---|---|---|
| Time to first query | create a child of the chain's tip, connect, read one row, timed as one span | 1 |
| BRANCH_CREATE | create a child of the tip | 1 |
| BRANCH_CONNECT | connect to the next branch of a chain of 4 | 100 |
| READ | read one row by primary key | 1000 |
| RANGE_READ | read 100 consecutive keys | 200 |
| INSERT | insert one generated row | 1000 |
| UPDATE | update one row by key | 1000 |
| RANGE_UPDATE | update 100 consecutive keys | 200 |

- **Cell:** the sum of the timed database calls in one batch, in seconds. It is
  null unless every call in the batch succeeded.
- **First try:** try 1. **Best repeat:** the lower of tries 2 and 3, and it needs both.
- **Branches:** every row starts from the tip of a chain of 4 branches. Each data
  row runs on its own new branch. After every run, the runner checks that the tip
  still has the dump's row counts.
- **Determinism:** the seed in `suite.json` fixes the keys and the generated rows.
- **Load:** the time to load the dump. Provisioning is recorded but not ranked.
- **Hosted results** include the network round trip from the client. Each one
  records its region, where the client ran, and a round-trip baseline: the median
  of 20 `SELECT 1` calls.

## Ranking

- The page ranks local and hosted systems in separate tables.
- For each ranked row: (value + 10 ms) / (best value in the same table + 10 ms).
  The score is the geometric mean of these ratios, as on ClickBench.
- The branch view ranks time to first query only. Create and connect are its
  parts, so they are shown but not ranked. The data view ranks READ through
  RANGE_UPDATE, and viewers can untick rows.
- A row that no entry in the table has a value for is left out of the score, and
  the page says so. An entry missing a ranked row is listed below the ranking,
  unranked, with the rows it lacks.
- Only the newest file per system and machine counts. If it is an error file,
  that system and machine are excluded, and no older result stands in.

## Result files

A result lives at `<system>/results/<YYYYMMDD>/<machine>.json`, dated in UTC. It
holds the system's identity from `system.json`, the server version, the suite
name and digest, the seed, the git commit, the dataset and lock digests, the
provisioning and load times, and `result`: 8 rows of 3 cells. Hosted results add
`region`, `rtt_ms`, `client_location`, and `service`, an allowlist of the settings
the adapter requested.

A failed run writes `{"error": "..."}` in the same place. The runner masks the
password of any connection URI in an error record or message.

`python -m leaderboard.validate` rejects a file when:

- its path, date directory, or machine name is malformed;
- a key is missing, unexpected, or of the wrong type, including booleans used as numbers;
- it came from a dirty tree or a capped (`--max-batch`) run;
- its suite, suite digest, seed, or dataset digest differs from `suite.json`;
- its identity differs from `system.json`;
- any key or string looks like a credential.

`results/` and `data.js` are git-ignored until the suite is frozen, because
db-fork is public and a pushed result is a published one.

## Rules

- Systems run with their default configuration. A tuned system says `"tuned": "yes"`
  in `system.json`, and its tuning is described in this file.
- No result caching between tries or runs.
- The runner queries the server version and records it.
- Results come only from a committed tree. Changing `suite.json` changes its
  digest, so results from different suites are never compared.

## Adding a system

1. **Adapter.** Subclass `DBToolSuite` in `dblib/` with branch create, connect,
   current branch, list, and delete. `dblib/dolt.py` is the smallest example. Add
   the backend to the `Backend` enum in `microbench/task2.proto`, run
   `python build_protos.py`, and construct the adapter in `WorkerContext.__enter__`
   in `microbench/runner2.py`.
2. **Identity.** Create `leaderboard/<system>/system.json` with `system`,
   `proprietary`, `hosted`, `tuned`, and `tags`.
3. **Local server scripts.** Add executable `start`, `check`, and `stop` scripts, and
   optionally `install`, as in `leaderboard/dolt/`. `start` must send the server's
   output to a log file, and `stop` must stop only the process `start` launched.
4. **Runner.** Add a class to `leaderboard/run.py` with `backend`, `needs_psql`,
   `version_sql`, `provision`, `load`, and `delete`, and register it in `SYSTEMS`.
   Everything it creates must be named after the run's database, so `delete` can
   find it by name. A hosted class also sets `keys`, `region`, `service`, and
   `preflight`, and every `service` key must be in the validator's allowlist.
5. **Trial.** Run it with `--max-batch 5 --allow-dirty --out /tmp/lb`, and check that
   every cell is filled and nothing is left behind.
6. **Result.** Commit, run it in full, then validate and build.

Tests: `python -m unittest leaderboard.test_leaderboard`.
