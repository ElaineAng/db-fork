"""
Generic SQL parsing utilities.

This module provides low-level SQL parsing functions that are independent
of any specific application logic. Use these utilities to extract structural
information from SQL statements.
"""


def get_sql_operation_keyword(sql: str) -> str:
    """
    Extract the primary SQL operation keyword from a statement.

    Handles complex SQL including:
    - CTEs (WITH clauses) - returns the main operation after the CTE
    - Comments (-- and /* */) - removes them before parsing
    - Multiple statements - uses only the first
    - Subqueries - ignores them, focuses on the main statement

    Args:
        sql: SQL statement to analyze

    Returns:
        The primary operation keyword (SELECT, INSERT, UPDATE, DELETE, WITH)
        in uppercase, or empty string if none found

    Example:
        >>> get_sql_operation_keyword("SELECT * FROM users")
        "SELECT"
        >>> get_sql_operation_keyword("WITH cte AS (SELECT 1) SELECT * FROM cte")
        "SELECT"
        >>> get_sql_operation_keyword("-- comment\\nINSERT INTO users VALUES (1)")
        "INSERT"
    """
    if not sql or not sql.strip():
        return ""

    # Remove SQL comments
    sql_clean = _remove_sql_comments(sql)

    # Split by semicolons to handle multiple statements (take first)
    statements = sql_clean.split(";")
    if statements:
        sql_clean = statements[0].strip()

    if not sql_clean:
        return ""

    # Handle CTEs: WITH clause comes before the main query
    if sql_clean.upper().lstrip().startswith("WITH"):
        main_stmt = _extract_main_statement_after_cte(sql_clean)
        if main_stmt:
            sql_clean = main_stmt

    # Get the first significant keyword
    return get_first_keyword(sql_clean)


def _remove_sql_comments(sql: str) -> str:
    """
    Remove SQL comments (-- and /* */) from a SQL string.

    Preserves string literals - comments inside quotes are not removed.

    Args:
        sql: SQL statement potentially containing comments

    Returns:
        SQL statement with comments removed
    """
    result = []
    i = 0
    in_string = False
    string_char = None

    while i < len(sql):
        # Handle string literals (don't process comments inside strings)
        if not in_string and sql[i] in ('"', "'"):
            in_string = True
            string_char = sql[i]
            result.append(sql[i])
            i += 1
        elif in_string:
            result.append(sql[i])
            if sql[i] == string_char and (i == 0 or sql[i - 1] != "\\"):
                in_string = False
            i += 1
        # Handle -- comments
        elif sql[i : i + 2] == "--":
            # Skip until end of line
            while i < len(sql) and sql[i] != "\n":
                i += 1
            if i < len(sql):
                result.append("\n")  # Keep the newline
                i += 1
        # Handle /* */ comments
        elif sql[i : i + 2] == "/*":
            # Skip until */
            i += 2
            while i < len(sql) - 1:
                if sql[i : i + 2] == "*/":
                    i += 2
                    break
                i += 1
        else:
            result.append(sql[i])
            i += 1

    return "".join(result)


def _extract_main_statement_after_cte(sql: str) -> str:
    """
    Extract the main statement after CTE (WITH clause) definitions.

    For queries with CTEs, this function returns the primary SELECT/INSERT/UPDATE/DELETE
    statement that follows the CTE definitions.

    Example:
        >>> extract_main_statement_after_cte("WITH cte AS (SELECT * FROM t) SELECT * FROM cte")
        "SELECT * FROM cte"

    Args:
        sql: SQL statement potentially containing CTEs

    Returns:
        Main statement after CTE definitions, or original SQL if no CTEs found
    """
    # Find the main statement keyword at depth 0 (outside of all parentheses)
    depth = 0

    for i, char in enumerate(sql):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth == 0 and char not in (" ", "\t", "\n", ","):
            # We're at depth 0 and hit a non-whitespace, non-comma character
            # Check if this starts a main statement keyword
            remaining = sql[i:].lstrip().upper()
            if remaining.startswith(("SELECT", "INSERT", "UPDATE", "DELETE")):
                return sql[i:].lstrip()

    # Fallback: return original if we can't parse it
    return sql


def get_first_keyword(sql: str) -> str:
    """
    Extract the first SQL keyword from a SQL statement.

    This is useful for determining the type of SQL operation (SELECT, INSERT, etc.).

    Args:
        sql: SQL statement (should ideally be cleaned of comments first)

    Returns:
        First keyword in uppercase, or empty string if none found

    Example:
        >>> get_first_keyword("SELECT * FROM users")
        "SELECT"
        >>> get_first_keyword("  INSERT INTO users VALUES (1)")
        "INSERT"
    """
    # Split by whitespace and get first non-empty token
    tokens = sql.strip().split()
    if not tokens:
        return ""

    first_token = tokens[0].upper()

    # Remove parentheses if present
    first_token = first_token.lstrip("(").rstrip(")")

    return first_token
