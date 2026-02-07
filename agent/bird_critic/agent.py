"""BIRD-Critic SQL debugging agent using LangGraph ReAct pattern."""

from typing import Annotated

from dotenv import load_dotenv
from langchain.tools import tool
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.language_models.chat_models import BaseChatModel
from langgraph.prebuilt import create_react_agent

from .data_loader import BirdCriticProblem
from .db_utils import DatabaseManager

# Import DBToolSuite for branching operations
from dblib.db_api import DBToolSuite

load_dotenv()


# Global database manager (set by the agent) - for simple SQL operations
_db_manager: DatabaseManager | None = None

# Global DBToolSuite (set by the agent) - for branching operations
_db_tools: DBToolSuite | None = None


def set_db_manager(manager: DatabaseManager) -> None:
    """Set the global database manager for tools."""
    global _db_manager
    _db_manager = manager


def set_db_tools(db_tools: DBToolSuite) -> None:
    """Set the global DBToolSuite for branching tools."""
    global _db_tools
    _db_tools = db_tools


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


# =============================================================================
# DBToolSuite Tools (for branching operations)
# =============================================================================


@tool
def create_branch(
    branch_name: Annotated[str, "Name of the new branch to create"],
    parent_id: Annotated[str, "ID of the parent branch (optional)"] = None,
) -> str:
    """Create a new database branch.

    Use this to create a new branch from the current branch or a specified parent.
    Branches allow you to make isolated changes that can be committed or discarded.
    """
    if not _db_tools:
        return "Error: No DBToolSuite available (branching not supported)"

    try:
        _db_tools.create_branch(branch_name, parent_id, timed=False)
        return f"Successfully created branch '{branch_name}'"
    except Exception as e:
        return f"Error creating branch: {e}"


@tool
def connect_branch(
    branch_name: Annotated[str, "Name of the branch to connect to"],
) -> str:
    """Connect to an existing database branch.

    Use this to switch to a different branch. All subsequent SQL operations
    will be executed on the connected branch.
    """
    if not _db_tools:
        return "Error: No DBToolSuite available (branching not supported)"

    try:
        _db_tools.connect_branch(branch_name, timed=False)
        return f"Successfully connected to branch '{branch_name}'"
    except Exception as e:
        return f"Error connecting to branch: {e}"


@tool
def get_current_branch() -> str:
    """Get information about the current branch.

    Returns the current branch name and ID.
    """
    if not _db_tools:
        return "Error: No DBToolSuite available (branching not supported)"

    try:
        branch_name, branch_id = _db_tools.get_current_branch()
        return f"Current branch: {branch_name} (ID: {branch_id})"
    except Exception as e:
        return f"Error getting current branch: {e}"


@tool
def commit_changes(
    message: Annotated[str, "Commit message describing the changes"] = "",
) -> str:
    """Commit pending changes to the current branch.

    Use this after making INSERT/UPDATE/DELETE operations to persist the changes.
    """
    if not _db_tools:
        return "Error: No DBToolSuite available (branching not supported)"

    try:
        _db_tools.commit_changes(timed=False, message=message)
        return (
            f"Successfully committed changes{': ' + message if message else ''}"
        )
    except Exception as e:
        return f"Error committing changes: {e}"


@tool
def execute_sql_branched(
    sql: Annotated[str, "The SQL query to execute"],
) -> str:
    """Execute a SQL query using DBToolSuite (on the current branch).

    Similar to execute_sql, but uses the branching-enabled DBToolSuite.
    Use this when working with branches.
    """
    if not _db_tools:
        return "Error: No DBToolSuite available (branching not supported)"

    try:
        result = _db_tools.execute_sql(sql, timed=False)

        if result is None:
            return "Query executed successfully. No rows returned."

        if not result:
            return "Query executed successfully. Empty result set."

        # Limit output
        max_rows = 20
        output = (
            f"Rows ({min(len(result), max_rows)} of {len(result)} shown):\n"
        )
        for row in result[:max_rows]:
            output += f"  {row}\n"

        return output
    except Exception as e:
        return f"SQL Error: {e}"


@tool
def get_table_schema_branched(
    table_name: Annotated[str, "Name of the table to get schema for"],
) -> str:
    """Get the CREATE TABLE statement using DBToolSuite (on the current branch).

    Use this when working with branches to get table schema.
    """
    if not _db_tools:
        return "Error: No DBToolSuite available (branching not supported)"

    try:
        schema = _db_tools.get_table_schema(table_name)
        return schema if schema else f"Table '{table_name}' not found"
    except Exception as e:
        return f"Error getting table schema: {e}"


# System prompt templates - built dynamically based on available tools

# Basic SQL prompt - straightforward debugging
SYSTEM_PROMPT_BASIC = """You are an expert SQL debugging assistant. Your task is to fix buggy SQL queries.

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

# Branching prompt - encourages exploration with multiple strategies
SYSTEM_PROMPT_BRANCHING = """You are an expert SQL debugging assistant with access to database branching capabilities.
Your task is to fix buggy SQL queries by exploring multiple strategies.

You will be given:
1. A natural language description of what the user wants to achieve
2. A buggy SQL query that needs to be fixed

## Key Capability: Branch-Based Exploration
You can create isolated database branches to try different fix strategies WITHOUT affecting the main database.
This allows you to:
- Test multiple approaches in parallel
- Make destructive changes safely (they're isolated to the branch)
- Compare results from different strategies
- Pick the best solution after exploration

## Available Tools:
- execute_sql_branched: Run SQL queries on the current branch
- get_table_schema_branched: Get the CREATE TABLE statement for a table
- create_branch: Create a new database branch for isolated changes
- connect_branch: Switch to a different branch
- get_current_branch: Get information about the current branch
- commit_changes: Commit pending changes so child branches can see them

## Recommended Exploration Strategy:
1. **Understand the problem**: List tables, examine schemas, understand the buggy query
2. **Identify potential approaches**: Think about 2-3 different ways to fix the query
3. **Create exploration branches**: For each approach, create a separate branch
   - Example: `create_branch("approach_1_join_fix")`, `create_branch("approach_2_subquery")`
4. **Test each approach**: Switch to each branch and test your fix
5. **Compare results**: Evaluate which approach works best
6. **Return the best solution**: Pick the query that correctly achieves the user's goal

## Example Workflow:
```
1. get_table_schema_branched("users")  # Understand schema
2. create_branch("fix_v1_inner_join")  # Try approach 1
3. execute_sql_branched("SELECT ... INNER JOIN ...")  # Test it
4. connect_branch("main")  # Go back to main
5. create_branch("fix_v2_subquery")  # Try approach 2
6. execute_sql_branched("SELECT ... WHERE id IN (SELECT ...)")  # Test it
7. Compare results and pick the best one
```

When you have the final corrected SQL, respond with:
CORRECTED_SQL:
```sql
<your corrected SQL here>
```
"""

BASIC_SQL_TOOLS_DESCRIPTION = """
- execute_sql: Run SQL queries to test them
- get_table_schema: Get the CREATE TABLE statement for a table
- list_tables: List all available tables
- get_sample_rows: See example data from a table
"""

BRANCHING_TOOLS_DESCRIPTION = """
- execute_sql_branched: Run SQL queries on the current branch
- get_table_schema_branched: Get the CREATE TABLE statement for a table
- create_branch: Create a new database branch for isolated changes
- connect_branch: Switch to a different branch
- get_current_branch: Get information about the current branch
- commit_changes: Commit pending changes to the current branch so that it can be accessed by child branches
"""


def build_system_prompt(use_db_manager: bool, use_db_tools: bool) -> str:
    """Build the system prompt based on which tools are configured.

    Args:
        use_db_manager: Whether DatabaseManager (basic SQL) tools are available.
        use_db_tools: Whether DBToolSuite (branching) tools are available.

    Returns:
        The complete system prompt with appropriate tool descriptions.
    """
    if use_db_manager and not use_db_tools:
        # Basic mode - straightforward debugging
        return SYSTEM_PROMPT_BASIC
    elif use_db_tools and not use_db_manager:
        # Branching mode - exploration with multiple strategies
        return SYSTEM_PROMPT_BRANCHING
    elif use_db_manager and use_db_tools:
        # Both available - use branching prompt (more powerful)
        return SYSTEM_PROMPT_BRANCHING
    else:
        return "No tools available."


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
        db_manager: DatabaseManager = None,
        db_tools: DBToolSuite = None,
        model_name: str = "gemini-2.0-flash",
        max_iterations: int = 10,
    ):
        """Initialize the agent.

        Args:
            db_manager: Database manager for simple SQL operations (optional).
            db_tools: DBToolSuite for branching operations (optional).
                      At least one of db_manager or db_tools must be provided.
            model_name: Name of the LLM model to use.
            max_iterations: Maximum number of agent iterations.
        """
        if not db_manager and not db_tools:
            raise ValueError(
                "At least one of db_manager or db_tools must be provided"
            )

        self.db_manager = db_manager
        self.db_tools = db_tools
        self.model_name = model_name
        self.max_iterations = max_iterations

        # Set global db manager for tools
        if db_manager:
            set_db_manager(db_manager)

        # Set global db tools for branching tools
        if db_tools:
            set_db_tools(db_tools)

        # Create tools list based on what's available
        self.tools = []

        if db_manager:
            # Add DatabaseManager-based tools
            self.tools.extend(
                [
                    execute_sql,
                    get_table_schema,
                    list_tables,
                    get_sample_rows,
                ]
            )

        if db_tools:
            # Add DBToolSuite-based tools (branching + SQL)
            self.tools.extend(
                [
                    execute_sql_branched,
                    get_table_schema_branched,
                    create_branch,
                    connect_branch,
                    get_current_branch,
                    commit_changes,
                ]
            )

        # Build system prompt based on which tools are available
        self.system_prompt = build_system_prompt(
            use_db_manager=bool(db_manager), use_db_tools=bool(db_tools)
        )

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
            self._run_setup_sql(problem.preprocess_sql, "preprocess")

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
            SystemMessage(content=self.system_prompt),
            HumanMessage(content=user_message),
        ]

        try:
            result = self.agent.invoke(
                {"messages": messages},
                {"recursion_limit": self.max_iterations},
            )

            # Print detailed trajectory
            print("    Agent trajectory:")
            step_num = 0
            for msg in result["messages"]:
                msg_type = type(msg).__name__

                # AI messages with tool calls
                if hasattr(msg, "tool_calls") and msg.tool_calls:
                    for tc in msg.tool_calls:
                        step_num += 1
                        tool_name = tc.get("name", "unknown")
                        tool_args = tc.get("args", {})
                        # Truncate long args
                        args_str = str(tool_args)
                        if len(args_str) > 100:
                            args_str = args_str[:100] + "..."
                        print(f"      [{step_num}] {tool_name}({args_str})")

                # Tool response messages
                elif msg_type == "ToolMessage":
                    content = str(msg.content) if msg.content else ""
                    # # Truncate long results
                    # if len(content) > 150:
                    #     content = content[:150] + "..."
                    content = content.replace("\n", " ")
                    print(f"          → {content}")

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
                self._run_setup_sql(problem.clean_up_sql, "cleanup")

    def _run_setup_sql(self, sql: str, phase: str) -> bool:
        """Run preprocess or cleanup SQL using available database interface.

        Args:
            sql: SQL statements to execute (may be semicolon-separated).
            phase: Either "preprocess" or "cleanup" for error messages.

        Returns:
            True if all statements succeeded.
        """
        if not sql:
            return True

        # Split by semicolons and execute each statement
        statements = [s.strip() for s in sql.split(";") if s.strip()]

        for stmt in statements:
            try:
                if self.db_manager:
                    success, _, error = self.db_manager.execute_sql(stmt)
                    if not success:
                        print(f"    {phase.capitalize()} SQL failed: {error}")
                        return False
                elif self.db_tools:
                    self.db_tools.execute_sql(stmt, timed=False)
            except Exception as e:
                print(f"    {phase.capitalize()} SQL failed: {e}")
                return False

        return True

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
