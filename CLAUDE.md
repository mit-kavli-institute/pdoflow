# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PDOFlow is a distributed job queue system that uses PostgreSQL as its single source of truth for managing work distribution across compute nodes. It's designed for scientific computing contexts where work needs to be distributed reliably without requiring additional message queue infrastructure.

## Development Commands

### Testing and Quality Checks
```bash
# Run all tests and checks
tox

# Run tests for specific Python version
tox -e py39  # or py310, py311, py312

# Type checking
tox -e mypy

# Linting
tox -e flake8

# Generate coverage report
tox -e clean,py39,report

# Build documentation
tox -e docs
```

### Code Formatting
```bash
# Format code with Black (80 char line limit)
black src/pdoflow tests

# Sort imports
isort src/pdoflow tests
```

### Running Tests Directly
```bash
# Run all tests with pytest
pytest

# Run specific test file
pytest tests/test_cli.py

# Run with coverage
pytest --cov=pdoflow --cov-report=html

# Run tests in parallel
pytest -n auto
```

### Package Management
```bash
# Install in development mode
pip install -e .

# Install with all dev dependencies
pip install -e ".[dev]"
```

### CLI Usage
```bash
# After installation, the pdoflow CLI is available
pdoflow --help
pdoflow pool --help
pdoflow posting-status --help
pdoflow priority-stats  # View priority distribution of waiting jobs
```

## Architecture Overview

### Core Components

1. **Models** (`src/pdoflow/models.py`):
   - `JobPosting`: Represents a batch of work
   - `JobRecord`: Individual work units with retry logic
   - Uses SQLAlchemy ORM with PostgreSQL-specific features

2. **Registry** (`src/pdoflow/registry.py`):
   - Central registry for decorated functions
   - Maps function names to callables
   - Global singleton pattern for convenience

3. **Worker System** (`src/pdoflow/cluster.py`):
   - `@job` decorator registers functions for distributed execution
   - `ClusterProcess`: Individual worker with failure tracking
   - `ClusterPool`: Manages multiple workers with auto-resurrection
   - Uses PostgreSQL's `SKIP LOCKED` for concurrent work distribution

4. **CLI** (`src/pdoflow/cli.py`):
   - Click-based command interface
   - Commands: pool, posting-status, list-postings, set-posting-status, execute-job

### Key Design Patterns

- **Database-Centric**: PostgreSQL handles all coordination without additional message queues
- **Process Isolation**: Each worker maintains independent DB connections
- **Dynamic Loading**: Functions loaded via import paths with caching
- **Transactional Safety**: Leverages PostgreSQL's ACID guarantees
- **Priority Queue**: Jobs are ordered by priority (DESC) then creation time (ASC)

### Database Configuration

PDOFlow expects database configuration at `~/.config/pdoflow/db.conf`:
```ini
[postgresql]
user = your_user
password = your_password
host = localhost
port = 5432
database = pdoflow_db
```

### Testing Approach

- Uses pytest with hypothesis for property-based testing
- PostgreSQL test database via pytest-postgresql
- Custom test workers for coverage tracking
- Example package in tests for dynamic loading scenarios

### Important Constraints

1. Functions must be importable (defined in files, not REPL)
2. All arguments must be JSON-serializable (no NaN/Inf)
3. Return values are discarded (functions must have side effects)
4. Requires PostgreSQL or compatible DB with row-level locking
5. Workers should run with minimal privileges for security
6. Priority values must be within PostgreSQL INT range (-2,147,483,648 to 2,147,483,647)

### Code Style

- Black formatting with 80-character line limit
- Type hints throughout (enforced by mypy)
- SQLAlchemy plugin for mypy type checking
- Flake8 linting with 81-character limit
- isort with Black profile for imports
