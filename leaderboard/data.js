const suite = {
 "suite": "local-1",
 "seed": 20260929,
 "tries": 3,
 "table": "item",
 "range_size": 100,
 "chain_length": 4,
 "dataset": {
  "path": "db_setup/ch_benchmark_seed.sql",
  "sha256": "3a1b5d4f01a98a27cc2241b4267e51a17f31a4469e0e3ed38c7d8df4abe1826e",
  "row_counts": {
   "region": 5,
   "nation": 5,
   "supplier": 1,
   "warehouse": 10,
   "district": 100,
   "customer": 10000,
   "history": 0,
   "item": 1000,
   "stock": 10000,
   "orders": 0,
   "new_order": 0,
   "order_line": 0
  }
 },
 "rows": [
  {
   "op": "time_to_first_query",
   "batch": 1
  },
  {
   "op": "branch_create",
   "batch": 1
  },
  {
   "op": "branch_connect",
   "batch": 100
  },
  {
   "op": "read",
   "batch": 1000
  },
  {
   "op": "range_read",
   "batch": 200
  },
  {
   "op": "insert",
   "batch": 1000
  },
  {
   "op": "update",
   "batch": 1000
  },
  {
   "op": "range_update",
   "batch": 200
  }
 ]
};
const data = [
 {
  "schema_version": 1,
  "run_id": "6864e0a4",
  "system": "Doltgres",
  "date": "2026-09-30",
  "machine": "apple-m5-32gb",
  "proprietary": "no",
  "hosted": "no",
  "tuned": "no",
  "tags": [
   "Go",
   "Postgres protocol",
   "Git-style branches"
  ],
  "version": "0.54.10",
  "suite": "local-1",
  "suite_sha256": "1f81da042c2e656c78f213f721a73c7d41ff49fcc3ac3846bd7baac12c369f93",
  "seed": 20260929,
  "commit": "a58c60cd6bd47ff1c77509706f4cf5e4af438faf",
  "dirty": false,
  "dataset_sha256": "3a1b5d4f01a98a27cc2241b4267e51a17f31a4469e0e3ed38c7d8df4abe1826e",
  "python": "3.13.14",
  "lock_sha256": "7ae09064a562613f4cd3ab164917169c08f062b0c26d419de7ea8b4bc5b0cfec",
  "provision_time": 0.076239,
  "load_time": 2.28157,
  "result": [
   [
    0.017085,
    0.016477,
    0.016774
   ],
   [
    0.018267,
    0.017482,
    0.017207
   ],
   [
    0.030325,
    0.021183,
    0.014354
   ],
   [
    0.101441,
    0.099527,
    0.093816
   ],
   [
    0.096473,
    0.096822,
    0.094895
   ],
   [
    4.421344,
    4.352964,
    4.374929
   ],
   [
    4.499201,
    4.525519,
    4.621396
   ],
   [
    1.013233,
    0.968612,
    1.006176
   ]
  ],
  "source": "dolt/results/20260930/apple-m5-32gb.json"
 },
 {
  "schema_version": 1,
  "run_id": "b0805f32",
  "system": "Dolt",
  "date": "2026-09-30",
  "machine": "apple-m5-32gb",
  "proprietary": "no",
  "hosted": "no",
  "tuned": "no",
  "tags": [
   "Go",
   "MySQL protocol",
   "Git-style branches"
  ],
  "version": "1.81.2",
  "suite": "local-1",
  "suite_sha256": "1f81da042c2e656c78f213f721a73c7d41ff49fcc3ac3846bd7baac12c369f93",
  "seed": 20260929,
  "commit": "a58c60cd6bd47ff1c77509706f4cf5e4af438faf",
  "dirty": false,
  "dataset_sha256": "3a1b5d4f01a98a27cc2241b4267e51a17f31a4469e0e3ed38c7d8df4abe1826e",
  "python": "3.13.14",
  "lock_sha256": "7ae09064a562613f4cd3ab164917169c08f062b0c26d419de7ea8b4bc5b0cfec",
  "provision_time": 0.10169,
  "load_time": 1.881417,
  "result": [
   [
    0.017025,
    0.018352,
    0.017342
   ],
   [
    0.016384,
    0.018392,
    0.018254
   ],
   [
    0.025941,
    0.017882,
    0.013131
   ],
   [
    0.079922,
    0.078657,
    0.077875
   ],
   [
    0.062561,
    0.061475,
    0.062164
   ],
   [
    4.178052,
    4.1807,
    4.242337
   ],
   [
    4.580433,
    4.594474,
    4.550795
   ],
   [
    0.864872,
    0.873143,
    0.925931
   ]
  ],
  "source": "dolt_mysql/results/20260930/apple-m5-32gb.json"
 }
];
const excluded = [];
