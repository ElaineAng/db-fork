import random
from typing import Callable, Any
from pathlib import Path

from dblib.db_api import DBToolSuite
from microbench.datagen import DynamicDataGenerator
from microbench import sampling


class DatabaseTask:
    def __init__(self, db_tools: DBToolSuite):
        self.db_tools = db_tools

        self.all_pks = {}  # Cache of all primary keys per table, lazy init.

        self.all_datagen = {}  # Cache of data generators per table, lazy init.

        self.all_tables = []  # Cache of all tables, lazy init.

    def setup_db(self, db_name: str, db_schema: str) -> None:
        self.db_tools.initialize_schema(db_schema)
        self.db_tools.commit_changes("Initialized database schema.")

    def delete_db(self, db_name: str) -> None:
        self.db_tools.delete_db(db_name)

    def get_all_tables(self) -> list[str]:
        if not self.all_tables:
            self.all_tables = self.db_tools.get_all_tables()
        return self.all_tables

    def preload_db_data(self, dir_path: str) -> list[str]:
        all_tables = self.get_all_tables()
        loaded_tables = []
        for table in all_tables:
            file_path = Path(dir_path) / f"{table}.csv"
            if not file_path.exists():
                print(f" No data file found for table '{table}', skipping.")
                continue
            print(
                f" Bulk copying data into table '{table}' from '{file_path}'..."
            )
            self.db_tools.bulk_copy_from_file(table, str(file_path))
            loaded_tables.append(table)

        self.db_tools.commit_changes("Preloaded data into database.")
        return loaded_tables

    def create_branch(self, branch_name: str, timed: bool = True):
        self.db_tools.create_db_branch(branch_name, timed=timed)

    def connect_branch(self, branch_name: str, timed: bool = True):
        self.db_tools.connect_db_branch(branch_name, timed=timed)

    def get_pk_columns_name(self, table_name: str) -> list[str]:
        """
        Retrieves the primary key columns by ordinal position for the specified
        table.
        """
        pk_columns = self.db_tools.get_primary_key_columns(table_name)
        if not pk_columns:
            raise ValueError(f"Table {table_name} has no primary key.")

        # sort pk_columns by ordinal position
        pk_columns.sort(key=lambda x: x[1])

        return [col[0] for col in pk_columns]

    def load_pk_values_for_table(
        self, table_name: str, pk_columns: list[str] = None
    ) -> None:
        """
        Fetch all primary keys to create a base population. This should be
        relatively fast since it's an index-only scan.
        """
        if table_name in self.all_pks:
            return  # Already loaded

        if not pk_columns:
            pk_columns = self.get_pk_columns_name(table_name)

        sql = f"SELECT {', '.join(pk_columns)} FROM {table_name};"
        self.all_pks[table_name] = set(self.db_tools.run_sql_query(sql))

    def get_table_row_count(self, table_name: str) -> int:
        """
        Returns the total number of rows in the specified table.
        """
        self.load_pk_values_for_table(table_name)
        return len(self.all_pks.get(table_name, []))

    def load_datagen_for_table(self, table_name: str) -> None:
        """
        Loads the DDL schema for the specified table and initializes the data
        generator.
        """
        if table_name in self.all_datagen:
            return  # Already loaded

        ddl = self.db_tools.get_table_schema(table_name)
        if not ddl:
            raise ValueError(f"Could not fetch DDL for table {table_name}.")

        data_generator = DynamicDataGenerator(ddl)
        self.all_datagen[table_name] = data_generator

    def point_read(
        self,
        table_name: str,
        sampling_rate: float,
        max_sampling_size: int,
        dist_lambda: Callable[..., list[float]],  # The distribution to call,
        sort_idx: int,
    ) -> None:
        """
        Performs reads on a skewed sample of keys using the Beta distribution.

        Args:
            table_name: The name of the table to read from.
            sampling_rate: The fraction (0.0 to 1.0) of total keys to form the reading pool.
            dist_lambda: The distribution function to use for sampling.
            sort_idx: The index of the PK column to sort by before sampling.
        """

        pk_columns_name = self.get_pk_columns_name(table_name)
        self.load_pk_values_for_table(table_name, pk_columns_name)
        if not self.all_pks.get(table_name):
            print(" Table is empty. No rows to read.")
            return

        # Convert the set of primary keys to a list for indexing
        # CAREFUL: This could be expensive for very large tables since we are
        # storing everything in memory.
        pk_set = self.all_pks[table_name]
        pk_list = list(pk_set)

        # Sort by first PK column, this could be a argument if needed.
        sampling.sort_population(pk_list, sort_idx)
        skewed_indices = sampling.get_sampled_indices(
            population_size=len(pk_set),
            sampling_rate=sampling_rate,
            max_sampling_size=max_sampling_size,
            dist_lambda=dist_lambda,
        )

        # Now build the read queries for the timed connection to execute.
        where_clause = " AND ".join(
            [f"{pk_name} = %s" for pk_name in pk_columns_name]
        )
        select_sql = f"SELECT * FROM {table_name} WHERE {where_clause};"

        for idx in skewed_indices:
            pk_to_read = pk_list[idx]
            self.db_tools.run_sql_query(select_sql, pk_to_read, timed=True)

        print(f" Read operations completed, {len(skewed_indices)} rows read.")

    def insert(
        self, table_name: str, num_rows: int, timed: bool = True
    ) -> None:
        """
        Inserts unique rows into the database using a provided connection.
        """
        col_names = self.db_tools.get_all_columns(table_name)
        placeholders = ", ".join([f"%({name})s" for name in col_names])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders});"

        pk_columns = self.get_pk_columns_name(table_name)
        self.load_pk_values_for_table(table_name, pk_columns)
        self.load_datagen_for_table(table_name)

        print(
            f"Generating and inserting {num_rows} unique rows into '{table_name}'..."
        )

        inserted_count = 0
        # Safety break to prevent infinite loops on high PK collision rates.
        for _ in range(num_rows * 5):
            if inserted_count >= num_rows:
                break

            row_data = self.all_datagen[table_name].generate_row()
            pk_tuple = tuple(row_data[pk] for pk in pk_columns)

            # Generated a new unique row, insert it.
            if pk_tuple not in self.all_pks[table_name]:
                # Add the new pk to set.
                self.all_pks[table_name].add(pk_tuple)
                self.db_tools.run_sql_query(insert_sql, row_data, timed=timed)
                inserted_count += 1

        # Commit all insertions at once.
        self.db_tools.commit_changes("Inserted new rows.", timed=timed)
        if inserted_count < num_rows:
            print(
                f"Warning: Only generated {inserted_count} unique rows due "
                f"to PK collisions."
            )
        print(f"Insertion commands executed for {inserted_count} rows.")

    def update(
        self,
        table_name: str,
        update_ratio: float,
        max_updates: int,
        sort_idx: int,
        dist_lambda: Callable[..., Any],
        timed: bool = True,
    ) -> None:
        """
        Updates a specified number of random rows in the table.
        """
        # Get updatable columns (exclude primary key columns).
        pk_columns = self.get_pk_columns_name(table_name)
        col_names = self.db_tools.get_all_columns(table_name)
        updatable_columns = [col for col in col_names if col not in pk_columns]
        if not updatable_columns:
            print("   -> No updatable (non-primary key) columns found.")
            return

        # Load existing primary keys and data generators.
        self.load_pk_values_for_table(table_name, pk_columns)
        self.load_datagen_for_table(table_name)

        pk_set = self.all_pks.get(table_name)
        if not pk_set:
            print("   -> Table is empty. No rows to update.")
            return
        pk_list = list(pk_set)
        sampling.sort_population(pk_list, sort_idx)

        # Select a sample of keys to update.
        skewed_indices = sampling.get_sampled_indices(
            population_size=len(pk_set),
            sampling_rate=update_ratio,
            max_sampling_size=max_updates,
            dist_lambda=dist_lambda,
        )

        # Loop through selected keys and perform updates
        for idx in skewed_indices:
            pk_tuple = pk_list[idx]
            # Choose 1 to 3 random columns to update for this row
            cols_to_update = random.sample(
                updatable_columns,
                k=random.randint(1, min(3, len(updatable_columns))),
            )

            # Build the SET clause and data dictionary for the query
            set_clauses = [f"{col} = %({col})s" for col in cols_to_update]

            update_data = {
                col: self.all_datagen[table_name].generate_value(col)
                for col in cols_to_update
            }

            # Build the WHERE clause and add PKs to the data dictionary
            where_clauses = [
                f"{pk_name} = %({pk_name})s" for pk_name in pk_columns
            ]

            for i, pk_name in enumerate(pk_columns):
                update_data[pk_name] = pk_tuple[i]

            update_sql = (
                f"UPDATE {table_name} SET {', '.join(set_clauses)} "
                f"WHERE {' AND '.join(where_clauses)};"
            )

            self.db_tools.run_sql_query(update_sql, update_data, timed=timed)

        # Commit all updates at once.
        self.db_tools.commit_changes("Updated existing rows.", timed=timed)
        print(f"Update commands executed for {len(skewed_indices)} rows.")
