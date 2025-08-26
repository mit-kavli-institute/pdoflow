===============
Developer Guide
===============

This guide provides in-depth information for developers contributing to or extending PDOFlow.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   architecture
   contributing
   testing

Overview
--------

PDOFlow is designed with the following principles:

- **Simplicity**: PostgreSQL as the single source of truth
- **Reliability**: ACID guarantees for job processing
- **Scalability**: Horizontal scaling through worker processes
- **Extensibility**: Easy to add new job types and behaviors

Development Setup
----------------

Prerequisites
~~~~~~~~~~~~

- Python 3.9 or higher
- PostgreSQL 14 or higher
- Git
- tox (for running tests)

Setting Up Development Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **Clone the repository**:

   .. code-block:: bash

      git clone https://github.com/your-org/pdoflow.git
      cd pdoflow

2. **Create virtual environment**:

   .. code-block:: bash

      python -m venv venv
      source venv/bin/activate  # On Windows: venv\Scripts\activate

3. **Install in development mode**:

   .. code-block:: bash

      pip install -e ".[dev]"

4. **Set up pre-commit hooks**:

   .. code-block:: bash

      pre-commit install

5. **Configure test database**:

   .. code-block:: bash

      # Create test configuration
      mkdir -p ~/.config/pdoflow
      cat > ~/.config/pdoflow/db.conf <<EOF
      [postgresql]
      user = pdoflow_test
      password = test_password
      host = localhost
      port = 5432
      database = pdoflow_test
      EOF

Code Style
---------

PDOFlow follows strict code style guidelines:

- **Formatting**: Black with 80-character line limit
- **Linting**: flake8 with custom configuration
- **Type Hints**: Required for all public APIs
- **Imports**: Sorted with isort

Run formatting:

.. code-block:: bash

   black src/pdoflow tests
   isort src/pdoflow tests

Check style:

.. code-block:: bash

   tox -e flake8
   tox -e mypy

Project Structure
----------------

.. code-block:: text

   pdoflow/
   ├── src/pdoflow/          # Main package
   │   ├── __init__.py       # Package exports
   │   ├── models.py         # Database models
   │   ├── cluster.py        # Worker implementation
   │   ├── registry.py       # Job registry
   │   ├── cli.py           # CLI commands
   │   ├── io.py            # Database I/O
   │   └── status.py        # Status enumerations
   ├── tests/               # Test suite
   │   ├── conftest.py      # Pytest configuration
   │   ├── strategies.py    # Hypothesis strategies
   │   └── test_*.py       # Test modules
   ├── docs/               # Documentation
   ├── tox.ini            # Test automation
   ├── pyproject.toml     # Project configuration
   └── README.md          # Project README

Key Concepts
-----------

Database-Centric Design
~~~~~~~~~~~~~~~~~~~~~

PDOFlow uses PostgreSQL features extensively:

- **Row-level locking**: ``FOR UPDATE SKIP LOCKED``
- **UUID generation**: ``pgcrypto`` extension
- **JSONB**: For storing job arguments
- **Indexes**: Optimized for queue operations

Process Isolation
~~~~~~~~~~~~~~~

Each worker process:

- Maintains its own database connection
- Has independent failure tracking
- Can be killed without affecting others
- Automatically replaced by the pool

Dynamic Loading
~~~~~~~~~~~~~

Jobs are loaded dynamically:

- Function paths stored in database
- Cached after first load
- Support for module reloading in development

Next Steps
---------

- Read :doc:`architecture` for system design details
- See :doc:`contributing` for contribution guidelines
- Review :doc:`testing` for test writing guide

Quick Links
----------

- `GitHub Repository <https://github.com/your-org/pdoflow>`_
- `Issue Tracker <https://github.com/your-org/pdoflow/issues>`_
- `CI/CD Pipeline <https://github.com/your-org/pdoflow/actions>`_
