# BIRD-Critic SQL Debugging Agent

An agent that solves SQL debugging problems from the [BIRD-Critic PostgreSQL](https://huggingface.co/datasets/birdsql/bird-critic-1.0-postgresql) dataset.

## Overview

The BIRD-Critic benchmark presents buggy SQL queries that need to be corrected. This agent uses a LangGraph ReAct pattern with Gemini to iteratively debug and fix SQL queries by:

1. Understanding the user's natural language query
2. Analyzing the buggy SQL
3. Exploring database schema and sample data
4. Testing fixes and refining until correct

## Usage

### Basic Usage

```bash
# Run with Gemini (default)
python -m agent.bird_critic.run --limit 10

# Run with OpenAI GPT-4o
python -m agent.bird_critic.run --model gpt-4o --limit 10

# Run with Claude
python -m agent.bird_critic.run --model claude-3-5-sonnet-20241022 --limit 10

# Run with verbose output
python -m agent.bird_critic.run --model gpt-4o --limit 5 --verbose
```

### Supported Models

| Provider | Models | Env Variable |
|----------|--------|--------------|
| OpenAI | `gpt-4o`, `gpt-4-turbo`, `o1`, `o1-mini`, `o3-mini` | `OPENAI_API_KEY` |
| Google | `gemini-2.0-flash`, `gemini-1.5-pro` | `GOOGLE_API_KEY` |
| Anthropic | `claude-3-5-sonnet-20241022`, `claude-3-opus` | `ANTHROPIC_API_KEY` |

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--model` | LLM model to use | `gemini-2.0-flash` |
| `--split` | Dataset split | `train` |
| `--limit` | Max problems to evaluate | None (all) |
| `--indices` | Specific problem indices | None |
| `--db-host` | PostgreSQL host | `localhost` |
| `--db-port` | PostgreSQL port | `5432` |
| `--db-user` | PostgreSQL user | `postgres` |
| `--db-password` | PostgreSQL password | `password` |
| `--output-dir` | Results directory | `./eval_results` |
| `--verbose` | Enable detailed output | False |
| `--max-iterations` | Max agent iterations | `15` |

## Architecture

```
agent/bird_critic/
├── __init__.py       # Package init
├── data_loader.py    # HuggingFace dataset loader
├── db_utils.py       # PostgreSQL connection utilities
├── agent.py          # LangGraph ReAct agent
├── evaluate.py       # Evaluation framework
└── run.py            # CLI entry point
```

### Components

- **data_loader.py**: Loads the BIRD-Critic dataset from HuggingFace
- **db_utils.py**: PostgreSQL connection management and query execution
- **agent.py**: ReAct agent with tools for SQL execution, schema inspection, and data sampling
- **evaluate.py**: Evaluation metrics and result tracking
- **run.py**: Command-line interface

### Agent Tools

| Tool | Description |
|------|-------------|
| `execute_sql` | Run SQL queries and return results/errors |
| `get_table_schema` | Get CREATE TABLE statement for a table |
| `list_tables` | List all tables in the database |
| `get_sample_rows` | Get sample rows from a table |

## Requirements

```
datasets
psycopg2
langchain
langgraph
langchain-google-genai
python-dotenv
```

## Environment Variables

Set these in your `.env` file:

```
GOOGLE_API_KEY=your-api-key
PGHOST=localhost
PGPORT=5432
PGUSER=postgres
PGPASSWORD=password
PGDATABASE=postgres
```

## Dataset Structure

Each problem contains:
- `db_id`: Database name
- `query`: User's natural language description
- `error_sql`: The buggy SQL to fix
- `sol_sql`: Ground truth solution (may not be public)
- `preprocess_sql`: Setup SQL to run before
- `clean_up_sql`: Cleanup SQL to run after
- `test_cases`: Validation test cases
