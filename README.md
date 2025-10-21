## Environment Setup
1. Get virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2. Install Dependencies

    ```bash
    pip3 install .
    ```
3. Setup psql and a postgres-compatible backend (e.g. dolt, neon, etc.)

    Details omitted here. Plenty documents online.

4. Setup .env (only needed if you will be running an agent)

    Create a `.env` file in the root dir, and put your API key there.

## Run microbenchmark

* **branching-only**, e.g.:
    ```bash
    python3 microbench/runner.py --branch_only --branch_depth=6 --branch_degree=2 
    ```
    This measures average branch creation time (a tree with 6 levels and 2 degrees) and doesn't do any insert on each branch. To do some insertion (e.g. 2 per branch) on branches and measure branch creation time, do
    ```bash
    python3 microbench/runner.py --branch_only --branch_depth=6 --branch_degree=2 --num_inserts=2
    ```

* **insertion-only**, e.g.
    ```bash
    python3 microbench/runner.py --insert_only --num_inserts=x
    ```
    This measures average insert time doing x inserts in a single transaction without branching.

* **branch-insert**, e.g.
    ```bash
    python3 microbench/runner.py --branch_insert --num_inserts=x --branch_depth=6 --branch_degree=2
    ```
    This measures average insert time while having branches. The branch tree is defined by `--branch_depth` and `--branch_degree`, and for each branch we do `--num_inserts`

* **branch-insert-read**, e.g.:
    ```bash
    python3 microbench/runner.py --branch_insert_read
    ```
    This measures average read time while having branches and inserts on each branch. `--branch_depth`, `--branch_degree` and `--num_inserts` are still tunable as before. This bench also allows tunning the following parameters:
    
    * `--sampling_rate`: The ratio of rows read. The total number of rows in the table depend on which branch we are at, which by default is a leaf to root in the branching tree, so effectivly `(branch_depth + 1) * num_inserts`.

    * `--max_sample_size`: Max number of rows read.

    * `--alpha`: the alpha in beta-distribution, defines row selection skewness

    * `--beta`: the beta in beta-distribution, defines row selection skewness

Note that the benchmark automatically creates a database (by default named `microbench` and using `TPC-C` schema), and do insertion on that database. By default the database is cleaned up after each benchmark run. If you don't want to clean it up, specify `--no_cleanup`

## Run the agent (WIP)

```bash
python3 agent/basic_ops.py
```
