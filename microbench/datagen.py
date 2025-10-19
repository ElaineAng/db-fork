import random
import re
from typing import Dict, Any, Optional, Tuple
from faker import Faker

ColumnDef = Dict[str, Any]


class DynamicDataGenerator:
    """
    Parses a DDL schema to generate and insert fake data, respecting primary keys.
    """

    def __init__(self, ddl_schema: str) -> None:
        self.ddl: str = ddl_schema
        self.fake: Faker = Faker()
        self.table_name: str = ""
        self.columns: Dict[str, ColumnDef] = {}
        self._parse_ddl()

    def _parse_ddl(self) -> None:
        """Parses DDL to extract table name, columns, and primary keys."""
        print(f"Parsing DDL schema...\n{self.ddl}\n")
        # Extract table name
        table_match = re.search(
            r"CREATE TABLE\s+([\w\.]+)\s*\(", self.ddl, re.IGNORECASE
        )
        if not table_match:
            raise ValueError("Could not parse table name from DDL.")
        self.table_name = table_match.group(1)

        # Extract content within parentheses
        content_match = re.search(r"\((.*)\)", self.ddl, re.DOTALL)
        if not content_match:
            raise ValueError("Could not parse column definitions from DDL.")
        content = content_match.group(1).strip()

        # Extract individual column definitions
        for line in content.split(",\n"):
            line = line.strip()
            if not line or line.lower().startswith(
                ("primary key", "foreign key", "constraint")
            ):
                continue

            parts = re.match(r"(\w+)\s+([\w\(\), ]+)", line)
            if parts:
                name, type_full = parts.group(1), parts.group(2).strip()
                type_base = type_full.split("(")[0].lower()
                length: Optional[int] = None
                precision: Optional[Tuple[int, int]] = None

                params_match = re.search(r"\((\d+)(?:,\s*(\d+))?\)", type_full)
                if params_match:
                    if params_match.group(2):
                        precision = (
                            int(params_match.group(1)),
                            int(params_match.group(2)),
                        )
                    else:
                        length = int(params_match.group(1))

                self.columns[name] = {
                    "name": name,
                    "type": type_base,
                    "length": length,
                    "precision": precision,
                }

        print(
            f"✅ Schema parsed for table '{self.table_name}' with {len(self.columns)} columns."
        )

        # for col_name, col_def in self.columns.items():
        #     print(f" - Column: {col_name}, Definition: {col_def}")

    def generate_value(self, column_name: str) -> Any:
        """Generates a single fake value based on column type and name."""
        column = self.columns[column_name]
        name, col_type = column["name"].lower(), column["type"].split()[0]
        length, precision = column["length"], column["precision"]

        # Heuristics for realistic data
        if "city" in name:
            return self.fake.city()[: length or 20]
        if "state" in name:
            return self.fake.state_abbr()
        if "zip" in name or "postal" in name:
            return self.fake.zipcode()[: length or 9]
        if "phone" in name:
            return self.fake.phone_number()[: length or 16]
        if "first" in name:
            return self.fake.first_name()[: length or 40]
        if "last" in name:
            return self.fake.last_name()[: length or 40]
        if "email" in name:
            return self.fake.email()[: length or 40]
        # Type-based generation
        if col_type in ["varchar", "char", "text", "bpchar"]:
            if length <= 2:
                return self.fake.lexify(text="x" * (length or 1)).upper()
            return self.fake.text(max_nb_chars=length or 30)
        if col_type in ["smallint", "int2"]:
            return random.randint(-32768, 32767)
        if col_type in ["int", "integer", "bigint", "int4", "int8"]:
            return random.randint(1, 1000000)
        if col_type in ["decimal", "numeric"]:
            if precision:
                max_val = (10 ** (precision[0] - precision[1])) - 1
                return round(random.uniform(0, max_val), precision[1])
            return round(random.uniform(0, 1000), 2)
        if col_type in ["timestamp", "timestamptz", "date"]:
            return self.fake.date_time_between(start_date="-5y", end_date="now")
        if col_type == "boolean":
            return self.fake.boolean()
        return None

    def generate_row(self) -> Dict[str, Any]:
        """Generates a dictionary representing one row."""
        return {col: self.generate_value(col) for col in self.columns.keys()}
