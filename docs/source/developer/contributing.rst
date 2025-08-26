============
Contributing
============

Thank you for your interest in contributing to PDOFlow! This guide will help you get started.

Code of Conduct
--------------

Please note that this project is released with a Contributor Code of Conduct. By participating in this project you agree to abide by its terms.

Getting Started
--------------

1. **Fork the Repository**

   Visit `https://github.com/your-org/pdoflow` and click "Fork"

2. **Clone Your Fork**

   .. code-block:: bash

      git clone https://github.com/YOUR_USERNAME/pdoflow.git
      cd pdoflow
      git remote add upstream https://github.com/your-org/pdoflow.git

3. **Set Up Development Environment**

   .. code-block:: bash

      python -m venv venv
      source venv/bin/activate  # On Windows: venv\Scripts\activate
      pip install -e ".[dev]"
      pre-commit install

Development Workflow
-------------------

Creating a Feature Branch
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/issue-description

Making Changes
~~~~~~~~~~~~~

1. **Write your code** following the style guide
2. **Add tests** for new functionality
3. **Update documentation** if needed
4. **Run tests** to ensure everything works

Running Tests
~~~~~~~~~~~~

.. code-block:: bash

   # Run all tests with tox
   tox

   # Run specific Python version
   tox -e py311

   # Run just pytest
   pytest

   # Run with coverage
   pytest --cov=pdoflow

Code Quality Checks
~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Format code
   black src/pdoflow tests
   isort src/pdoflow tests

   # Check types
   tox -e mypy

   # Check style
   tox -e flake8

   # Run all checks
   tox -e flake8,mypy

Committing Changes
~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Stage changes
   git add -A

   # Commit with descriptive message
   git commit -m "feat: add priority queue support

   - Implement priority-based job selection
   - Add priority parameter to post_work
   - Update documentation"

Commit Message Format
~~~~~~~~~~~~~~~~~~~

Follow conventional commits:

- ``feat:`` New feature
- ``fix:`` Bug fix
- ``docs:`` Documentation changes
- ``style:`` Code style changes
- ``refactor:`` Code refactoring
- ``test:`` Test additions/changes
- ``chore:`` Maintenance tasks

Submitting a Pull Request
-----------------------

1. **Push to Your Fork**

   .. code-block:: bash

      git push origin feature/your-feature-name

2. **Create Pull Request**

   - Go to your fork on GitHub
   - Click "New pull request"
   - Select your branch
   - Fill out the PR template

3. **PR Checklist**

   - [ ] Tests pass locally
   - [ ] Code follows style guide
   - [ ] Documentation updated
   - [ ] Changelog entry added
   - [ ] Commits are logical

Code Style Guide
---------------

Python Style
~~~~~~~~~~~

- Follow PEP 8 with Black formatting
- 80 character line limit
- Use type hints for public APIs
- Docstrings for all public functions

Example:

.. code-block:: python

   def process_data(
       data_id: int,
       options: Optional[Dict[str, Any]] = None
   ) -> ProcessResult:
       """Process data with given options.

       Args:
           data_id: Unique identifier for data
           options: Processing options (optional)

       Returns:
           ProcessResult containing status and metadata

       Raises:
           ValueError: If data_id is invalid
           ProcessError: If processing fails
       """
       if data_id < 0:
           raise ValueError("data_id must be positive")

       options = options or {}
       # Implementation...

Import Style
~~~~~~~~~~~

Use isort with Black profile:

.. code-block:: python

   # Standard library
   import os
   import sys
   from typing import Dict, List, Optional

   # Third-party
   import click
   import sqlalchemy as sa
   from loguru import logger

   # Local
   from pdoflow.models import JobRecord
   from pdoflow.status import JobStatus

Testing Guidelines
-----------------

Test Structure
~~~~~~~~~~~~~

.. code-block:: python

   def test_descriptive_name():
       """Test that specific behavior works correctly."""
       # Arrange
       data = create_test_data()

       # Act
       result = function_under_test(data)

       # Assert
       assert result.status == "success"
       assert result.value == expected_value

Using Fixtures
~~~~~~~~~~~~~

.. code-block:: python

   @pytest.fixture
   def sample_job(db_session):
       """Create a sample job for testing."""
       job = JobRecord(
           positional_arguments=[1, 2, 3],
           keyword_arguments={"key": "value"}
       )
       db_session.add(job)
       db_session.commit()
       return job

   def test_job_execution(sample_job):
       """Test job executes correctly."""
       result = sample_job.execute()
       assert result is not None

Property-Based Testing
~~~~~~~~~~~~~~~~~~~~

Use Hypothesis for complex scenarios:

.. code-block:: python

   from hypothesis import given
   from tests.strategies import job_record

   @given(job_record())
   def test_job_properties(job):
       """Test job invariants hold."""
       assert job.tries_remaining >= 0
       assert job.priority is not None

Documentation
------------

Docstring Format
~~~~~~~~~~~~~~~

Use Google-style docstrings:

.. code-block:: python

   def complex_function(
       param1: str,
       param2: int,
       param3: Optional[List[str]] = None
   ) -> Dict[str, Any]:
       """Short description of function.

       Longer description explaining the function's behavior,
       assumptions, and any important details.

       Args:
           param1: Description of param1
           param2: Description of param2
           param3: Description of param3 (optional)

       Returns:
           Dictionary containing:
               - key1: Description
               - key2: Description

       Raises:
           ValueError: When param2 is negative
           TypeError: When param1 is not a string

       Example:
           >>> result = complex_function("test", 42)
           >>> print(result["key1"])
           'processed'

       Note:
           This function has side effects on the database.
       """

Updating Documentation
~~~~~~~~~~~~~~~~~~~~

1. Update docstrings in code
2. Update RST files in ``docs/source/``
3. Build docs to verify:

   .. code-block:: bash

      tox -e docs
      # Open .tox/docs/tmp/html/index.html

Common Tasks
-----------

Adding a New Feature
~~~~~~~~~~~~~~~~~~

1. Create feature branch
2. Write tests first (TDD)
3. Implement feature
4. Update documentation
5. Add changelog entry
6. Submit PR

Fixing a Bug
~~~~~~~~~~~

1. Create fix branch
2. Write test reproducing bug
3. Fix the bug
4. Verify test passes
5. Add changelog entry
6. Submit PR

Adding a Dependency
~~~~~~~~~~~~~~~~~

1. Add to ``pyproject.toml``
2. Update ``tox.ini`` if needed
3. Document why it's needed
4. Test with fresh environment

Performance Improvements
~~~~~~~~~~~~~~~~~~~~~~

1. Profile before optimizing
2. Write benchmark tests
3. Document performance gains
4. Ensure no regressions

Review Process
-------------

What to Expect
~~~~~~~~~~~~~

1. Automated tests run via GitHub Actions
2. Code review from maintainers
3. Possible requests for changes
4. Approval and merge

Review Criteria
~~~~~~~~~~~~~~

- **Correctness**: Does it work as intended?
- **Tests**: Are there adequate tests?
- **Style**: Does it follow conventions?
- **Documentation**: Is it documented?
- **Performance**: Any performance impact?

Responding to Feedback
~~~~~~~~~~~~~~~~~~~~

- Address all comments
- Push fixes as new commits
- Mark conversations as resolved
- Be patient and respectful

Release Process
--------------

PDOFlow follows semantic versioning:

1. **Major**: Breaking changes
2. **Minor**: New features
3. **Patch**: Bug fixes

Releases are automated when tags are pushed.

Getting Help
-----------

- **Discord**: Join our Discord server
- **Issues**: Open a GitHub issue
- **Discussions**: GitHub Discussions

Thank you for contributing to PDOFlow!
