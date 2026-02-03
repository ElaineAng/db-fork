"""BIRD-Critic SQL debugging agent using LangGraph ReAct pattern."""

import os
from typing import Annotated, Any

from dotenv import load_dotenv
from langchain.tools import tool
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.language_models.chat_models import BaseChatModel
from langgraph.prebuilt import create_react_agent

from .data_loader import BirdCriticProblem
from .db_utils import DatabaseManager

load_dotenv()


# Global database manager (set by the agent)
_db_manager: DatabaseManager | None = None


def set_db_manager(manager: DatabaseManager) -> None:
    """Set the global database manager for tools."""
    global _db_manager
    _db_manager = manager


@tool
def execute_sql(sql: Annotated[str, "The SQL query to execute"]) -> str:
    """Execute a SQL query and return the results or error message.

    Use this tool to run SQL queries against the database. The query can be
    SELECT, INSERT, UPDATE, DELETE, or any valid PostgreSQL statement.

    Returns the query results as a formatted string, or an error message if the query fails.
    """
    if not _db_manager:
        return "Error: No database connection available"

    success, result, error = _db_manager.execute_sql(sql)

    if not success:
        return f"SQL Error: {error}"

    if "rows" in result:
        # Format SELECT results
        columns = result["columns"]
        rows = result["rows"]

        if not rows:
            return f"Query executed successfully. No rows returned.\nColumns: {columns}"

        # Limit output size
        max_rows = 20
        output = f"Columns: {columns}\n"
        output += f"Rows ({min(len(rows), max_rows)} of {len(rows)} shown):\n"
        for row in rows[:max_rows]:
            output += f"  {row}\n"

        return output
    else:
        return f"Query executed successfully. Rows affected: {result.get('rowcount', 0)}"


@tool
def get_table_schema(
    table_name: Annotated[str, "Name of the table to get schema for"],
) -> str:
    """Get the CREATE TABLE statement showing the structure of a table.

    Use this to understand the columns, data types, and constraints of a table
    before writing or fixing SQL queries.
    """
    if not _db_manager:
        return "Error: No database connection available"

    schema = _db_manager.get_table_schema(table_name)
    return schema if schema else f"Table '{table_name}' not found"


@tool
def list_tables() -> str:
    """List all tables in the current database.

    Use this to discover what tables are available in the database.
    """
    if not _db_manager:
        return "Error: No database connection available"

    tables = _db_manager.list_tables()
    if tables:
        return "Tables in database:\n" + "\n".join(f"  - {t}" for t in tables)
    return "No tables found in database"


@tool
def get_sample_rows(
    table_name: Annotated[str, "Name of the table to sample"],
    limit: Annotated[int, "Maximum number of rows to return"] = 5,
) -> str:
    """Get sample rows from a table to understand its data.

    Use this to see example data in a table, which can help understand
    the data format and relationships.
    """
    if not _db_manager:
        return "Error: No database connection available"

    result = _db_manager.get_sample_rows(table_name, limit)

    if not result:
        return f"Could not get sample rows from '{table_name}'"

    columns = result["columns"]
    rows = result["rows"]

    if not rows:
        return f"Table '{table_name}' is empty.\nColumns: {columns}"

    output = f"Sample rows from '{table_name}':\n"
    output += f"Columns: {columns}\n"
    for row in rows:
        output += f"  {row}\n"

    return output


SYSTEM_PROMPT = """You are an expert SQL debugging assistant. Your task is to fix buggy SQL queries.

You will be given:
1. A natural language description of what the user wants to achieve
2. A buggy SQL query that needs to be fixed

Your goal is to:
1. Understand what the user wants from their description
2. Analyze the buggy SQL to identify the errors
3. Use the available tools to explore the database schema and test your fixes
4. Return a corrected SQL query that achieves the user's goal

Available tools:
- execute_sql: Run SQL queries to test them
- get_table_schema: Get the CREATE TABLE statement for a table
- list_tables: List all available tables
- get_sample_rows: See example data from a table

Approach:
1. First, list the tables and examine relevant schemas
2. Understand what the buggy SQL is trying to do
3. Identify the errors (syntax, wrong column names, logic issues, etc.)
4. Write and test a corrected query
5. Return ONLY the final corrected SQL in your last message

When you have the final corrected SQL, respond with:
CORRECTED_SQL:
```sql
<your corrected SQL here>
```
"""


def create_llm(model_name: str) -> BaseChatModel:
    """Create an LLM instance based on the model name.

    Supports:
    - OpenAI models: gpt-4o, gpt-4-turbo, gpt-3.5-turbo, o1, o1-mini, etc.
    - Google Gemini models: gemini-2.0-flash, gemini-1.5-pro, etc.

    Args:
        model_name: Name of the model (e.g., "gpt-4o", "gemini-2.0-flash").

    Returns:
        A LangChain chat model instance.

    Raises:
        ValueError: If the model provider cannot be determined.
    """
    model_lower = model_name.lower()

    # OpenAI models
    if any(model_lower.startswith(prefix) for prefix in ["gpt-", "o1", "o3"]):
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(model=model_name)

    # Google Gemini models
    if model_lower.startswith("gemini"):
        from langchain_google_genai import ChatGoogleGenerativeAI

        return ChatGoogleGenerativeAI(model=model_name)

    # Anthropic Claude models
    if model_lower.startswith("claude"):
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(model=model_name)

    # Default: try OpenAI (most common)
    raise ValueError(
        f"Could not determine model provider for '{model_name}'. "
        f"Supported prefixes: gpt-, o1, o3 (OpenAI), gemini (Google), claude (Anthropic)"
    )


class BirdCriticAgent:
    """Agent for debugging SQL queries from BIRD-Critic dataset."""

    def __init__(
        self,
        db_manager: DatabaseManager,
        model_name: str = "gemini-2.0-flash",
        max_iterations: int = 10,
    ):
        """Initialize the agent.

        Args:
            db_manager: Database manager for executing queries.
            model_name: Name of the LLM model to use.
            max_iterations: Maximum number of agent iterations.
        """
        self.db_manager = db_manager
        self.model_name = model_name
        self.max_iterations = max_iterations

        # Set global db manager for tools
        set_db_manager(db_manager)

        # Create tools list
        self.tools = [
            execute_sql,
            get_table_schema,
            list_tables,
            get_sample_rows,
        ]

        # Create the LLM (supports OpenAI, Gemini, Claude)
        self.llm = create_llm(model_name)

        # Create the ReAct agent
        self.agent = create_react_agent(
            model=self.llm,
            tools=self.tools,
        )

    def solve(self, problem: BirdCriticProblem, verbose: bool = False) -> str:
        """Solve a BIRD-Critic problem.

        Args:
            problem: The problem to solve.
            verbose: Whether to print intermediate steps.

        Returns:
            The corrected SQL query.
        """
        # Run preprocessing SQL if needed
        if problem.preprocess_sql:
            self.db_manager.run_preprocess_sql(problem.preprocess_sql)

        # Build the user message
        user_message = f"""Please fix the following buggy SQL query.

## User's Goal
{problem.query}

## Buggy SQL Query
```sql
{problem.error_sql}
```

Analyze the query, explore the database schema if needed, and provide a corrected SQL query that achieves the user's goal.
"""

        # Run the agent
        messages = [
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=user_message),
        ]

        try:
            result = self.agent.invoke(
                {"messages": messages},
                {"recursion_limit": self.max_iterations},
            )

            # Print trajectory summary (always)
            tool_calls = []
            iterations = 0
            for msg in result["messages"]:
                iterations += 1
                # Check for tool calls in the message
                if hasattr(msg, "tool_calls") and msg.tool_calls:
                    for tc in msg.tool_calls:
                        tool_name = tc.get("name", "unknown")
                        tool_calls.append(tool_name)
                # Check for AIMessage with tool_calls
                elif hasattr(msg, "additional_kwargs"):
                    tcs = msg.additional_kwargs.get("tool_calls", [])
                    for tc in tcs:
                        func = tc.get("function", {})
                        tool_calls.append(func.get("name", "unknown"))

            print(
                f"    Agent: {iterations} steps, {len(tool_calls)} tool calls: {tool_calls}"
            )

            if verbose:
                for msg in result["messages"]:
                    print(f"\n{'=' * 50}")
                    print(msg.pretty_repr())

            # Extract the corrected SQL from the final message
            final_message = result["messages"][-1].content
            corrected_sql = self._extract_sql(final_message)

            if corrected_sql:
                # Show abbreviated SQL
                sql_preview = corrected_sql[:100].replace("\n", " ")
                if len(corrected_sql) > 100:
                    sql_preview += "..."
                print(f"    Predicted SQL: {sql_preview}")

            return corrected_sql

        finally:
            # Run cleanup SQL if needed
            if problem.clean_up_sql:
                self.db_manager.run_cleanup_sql(problem.clean_up_sql)

    def _extract_sql(self, text: str) -> str:
        """Extract SQL from the agent's response.

        Looks for SQL in code blocks or after CORRECTED_SQL marker.
        """
        import re

        # Look for CORRECTED_SQL marker
        if "CORRECTED_SQL:" in text:
            text = text.split("CORRECTED_SQL:")[-1]

        # Look for SQL in code blocks
        sql_match = re.search(
            r"```sql\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE
        )
        if sql_match:
            return sql_match.group(1).strip()

        # Look for any code block
        code_match = re.search(r"```\s*(.*?)\s*```", text, re.DOTALL)
        if code_match:
            return code_match.group(1).strip()

        # Return the whole text as a fallback
        return text.strip()


if __name__ == "__main__":
    # Test the agent with a simple example
    from .data_loader import BirdCriticDataset

    # Load dataset
    dataset = BirdCriticDataset()
    problems = dataset.load()

    if problems:
        # Create database manager
        db_manager = DatabaseManager.from_env()

        # Create agent
        agent = BirdCriticAgent(db_manager)

        # Solve first problem
        problem = problems[0]
        print(f"Problem: {problem.query}")
        print(f"Error SQL: {problem.error_sql}")

        with db_manager.connection_context(problem.db_id):
            corrected = agent.solve(problem, verbose=True)
            print(f"\nCorrected SQL:\n{corrected}")
