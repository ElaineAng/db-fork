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
 },
 {
  "system": "Neon",
  "proprietary": "yes",
  "hosted": "yes",
  "tuned": "no",
  "tags": [
   "Postgres protocol",
   "Copy-on-write branches"
  ],
  "basis": "Mock, not a measurement. Branch create and connect follow earlier Neon measurements in run_stats_final (0.171 s per create, 0.78 s first connect, 117 ms repeated connect), adjusted for a client in US East. Data operations assume about 20 ms per statement.",
  "date": "2026-09-30",
  "machine": "apple-m5-32gb",
  "region": "aws-us-east-1",
  "rtt_ms": 18.4,
  "client_location": "US East",
  "provision_time": 4.8,
  "load_time": 9.6,
  "result": [
   [
    1.079,
    1.024,
    0.995
   ],
   [
    0.199,
    0.189,
    0.195
   ],
   [
    16.029,
    15.263,
    14.452
   ],
   [
    21.985,
    21.115,
    20.35
   ],
   [
    4.934,
    4.564,
    4.386
   ],
   [
    22.093,
    21.842,
    20.81
   ],
   [
    22.939,
    20.852,
    21.304
   ],
   [
    5.268,
    5.115,
    5.094
   ]
  ],
  "mock": true,
  "source": "mock/neon.json"
 },
 {
  "system": "Tiger Cloud",
  "proprietary": "yes",
  "hosted": "yes",
  "tuned": "no",
  "tags": [
   "Postgres protocol",
   "Service forks"
  ],
  "basis": "Mock, not a measurement. Branch create and connect follow earlier Tiger measurements in run_stats_final (55.8 s mean create, 0.52 s connect). Data operations assume about 22 ms per statement from a client in US East.",
  "date": "2026-09-30",
  "machine": "apple-m5-32gb",
  "region": "us-east-1",
  "rtt_ms": 21.7,
  "client_location": "US East",
  "provision_time": 96.5,
  "load_time": 11.2,
  "result": [
   [
    58.433,
    55.028,
    52.563
   ],
   [
    54.509,
    51.815,
    54.406
   ],
   [
    55.157,
    51.903,
    50.658
   ],
   [
    23.788,
    22.039,
    21.47
   ],
   [
    5.001,
    4.706,
    4.775
   ],
   [
    23.994,
    21.927,
    21.981
   ],
   [
    23.634,
    22.616,
    22.775
   ],
   [
    5.621,
    5.246,
    5.147
   ]
  ],
  "mock": true,
  "source": "mock/tiger.json"
 },
 {
  "system": "Xata",
  "proprietary": "yes",
  "hosted": "yes",
  "tuned": "no",
  "tags": [
   "Postgres protocol",
   "Copy-on-write branches"
  ],
  "basis": "Mock, not a measurement. Branch create and connect follow earlier Xata measurements in run_stats_final (54.6 s mean and 43.8 s median create, 0.12 s connect). Data operations assume about 20 ms per statement from a client in US East.",
  "date": "2026-09-30",
  "machine": "apple-m5-32gb",
  "region": "us-east-1",
  "rtt_ms": 19.2,
  "client_location": "US East",
  "provision_time": 63.4,
  "load_time": 10.3,
  "result": [
   [
    46.487,
    44.329,
    45.614
   ],
   [
    46.436,
    43.25,
    45.371
   ],
   [
    14.476,
    13.796,
    14.045
   ],
   [
    20.798,
    20.272,
    20.243
   ],
   [
    4.7,
    4.5,
    4.597
   ],
   [
    22.325,
    20.597,
    21.174
   ],
   [
    23.063,
    20.583,
    20.507
   ],
   [
    5.138,
    4.837,
    4.878
   ]
  ],
  "mock": true,
  "source": "mock/xata.json"
 }
];
const excluded = [];
