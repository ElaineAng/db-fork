import psycopg2
from abc import ABC, abstractmethod
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection as pgconn


class DBToolSuite(ABC):
    """
    An API for interacting with Postgres via a shared connection.
    """

    def __init__(self, connection: pgconn):
        self.conn = connection

    @abstractmethod
    def create_db_branch(self, branch_name: str) -> str:
        """
        Creates a new branch in the underlying database and allow reading and
        writing data to that branch.
        """
        pass

    @abstractmethod
    def connect_db_branch(self, branch_name: str) -> str:
        """
        Connects to an existing branch in the underlying database to allow
        reading and writing data to that branch.
        """
        pass

    @abstractmethod
    def list_db_branches(self) -> set[str]:
        """
        Lists all branches in the underlying database.
        """
        pass

    def get_table_schema(self, table_name: str) -> str:
        """
        Returns the schema of a specific table in a simplified CREATE TABLE
        format. This is used to understand the structure of a table before
        trying to insert data.
        """
        try:
            with self.conn.cursor() as cursor:
                # Query to get column name, data type, and if it's nullable
                query = """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s;
                """
                cursor.execute(query, (table_name,))
                columns = cursor.fetchall()

                if not columns:
                    return f"Error: Table '{table_name}' not found."

                # Format the schema into a readable string for the LLM
                schema_parts = [f"CREATE TABLE {table_name} ("]
                for col_name, data_type, is_nullable in columns:
                    part = f"  {col_name} {data_type}"
                    if is_nullable == "NO":
                        part += " NOT NULL"
                    schema_parts.append(part + ",")

                schema_parts.append(");")
                return "\n".join(schema_parts)

        except Exception as e:
            return f"An error occurred: {e}"

    def run_sql_query(self, query: str) -> str:
        """
        Runs an SQL query in the postgres database on the current branch. The
        query could be any one of a SELECT, INSERT, UPDATE, or DELETE statement.
        """
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute(query)
            self.conn.commit()
            try:
                records = cur.fetchall()
                return f"Query result: {records}"
            except psycopg2.ProgrammingError:
                return f"Query executed successfully"
        except Exception as e:
            self.conn.rollback()
            return f"Failed to execute SQL query: {str(e)}"
        finally:
            cur.close()
