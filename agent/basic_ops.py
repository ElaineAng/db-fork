import coolname
import psycopg2
from dotenv import load_dotenv
from langchain.tools import StructuredTool
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
from langgraph.prebuilt import create_react_agent
from dblib.dolt import DoltToolSuite

load_dotenv()


def new_name(existing_names: set[str]) -> str:
    """
    Generates a new name that does not exist in the provided set.
    """
    new_name = coolname.generate(2)
    while new_name[0] in existing_names:
        if new_name[1] not in existing_names:
            return new_name[1]
        new_name = coolname.generate(2)
    return new_name[0]


if __name__ == "__main__":
    DB_URI = "postgresql://postgres:password@localhost:5432/getting_started"
    db_connection = None
    try:
        db_connection = psycopg2.connect(DB_URI)
        db_tools = DoltToolSuite(connection=db_connection)

        available_tools = [
            StructuredTool.from_function(
                func=db_tools.create_branch,
                name="create_branch",
                description="Creates a new branch in the database and allow reading and writing data to that branch.",
            ),
            StructuredTool.from_function(
                func=db_tools.connect_branch,
                name="connect_branch",
                description="Connects to an existing branch in the database to allow reading and writing data to that branch.",
            ),
            StructuredTool.from_function(
                func=db_tools.list_branches,
                name="list_branches",
                description="With the current connection context, lists all branches in the database.",
            ),
            StructuredTool.from_function(
                func=db_tools.run_sql_query,
                name="run_sql_query",
                description="Runs an SQL query in the database on the current branch. The query could be any one of a SELECT, INSERT, UPDATE, or DELETE statement.",
            ),
            StructuredTool.from_function(
                func=db_tools.get_table_schema,
                name="get_table_schema",
                description="Returns the schema of a specific table in a simplified CREATE TABLE format. Use this tool to understand the structure of a table before trying to insert data.",
            ),
            StructuredTool.from_function(
                func=new_name,
                name="new_name",
                description="Generates a new name that does not exist in the provided set of existing names.",
            ),
        ]

        model = ChatGoogleGenerativeAI(model="gemini-2.5-flash")
        agent_graph = create_react_agent(
            model=model,
            tools=available_tools,
        )

        conversation_history = []
        starting_prompt = (
            "With the provided dolt database connection, "
            "create a new branch with a new branch name, write a record "
            "into the 'employees' table with a newly created first and last name, "
            "and show me all records in the 'employees' table."
        )

        user_inputs = {"messages": []}
        init_input = input(
            "An example initial prompt is:\n"
            + starting_prompt
            + "\n\n Do you want to execute this? (y/n): "
        )
        if init_input.lower() in ["y", "yes"]:
            user_inputs["messages"].append(
                HumanMessage(content=starting_prompt)
            )

        while True:
            final_answer = None
            if len(user_inputs["messages"]) > 0:
                for event in agent_graph.stream(
                    user_inputs, stream_mode="values"
                ):
                    print("Step by Step execution : ")
                    for message in event["messages"]:
                        print(message.pretty_repr())
                    final_answer = event

            if final_answer is not None:
                user_inputs["messages"].append(final_answer["messages"][-1])

            new_input = input("Enter your next command (or 'exit' to quit): ")
            if new_input.lower() in ["exit", "quit"]:
                break

            user_inputs["messages"].append(HumanMessage(content=new_input))
    finally:
        if db_connection:
            db_connection.close()
        print("Database connection closed.")
