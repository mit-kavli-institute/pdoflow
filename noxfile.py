import nox
from nox.sessions import Session


@nox.session(python=["3.9", "3.10", "3.11", "3.12"])
def tests(session: Session):
    session.install(
        ".[dev]",
        "--extra-index-url",
        "https://mit-kavli-institute.github.io/MIT-Kavli-PyPi/",
    )
    flags = session.posargs if session.posargs else []
    session.run("pytest", *flags)


@nox.session(python=["3.9", "3.10", "3.11", "3.12"])
def coverage(session: Session):
    session.install(
        ".[dev]",
        "--extra-index-url",
        "https://mit-kavli-institute.github.io/MIT-Kavli-PyPi/",
    )
    # Set up coverage for both xdist workers and subprocess coverage
    session.env["COVERAGE_FILE"] = f".coverage.{session.python}"
    session.env["COVERAGE_PROCESS_START"] = "pyproject.toml"
    # Add current directory to PYTHONPATH for sitecustomize.py
    session.env["PYTHONPATH"] = "."
    flags = session.posargs if session.posargs else []
    session.run("coverage", "run", "-m", "pytest", *flags)


@nox.session
def coverage_report(session: Session):
    """Combine coverage data and generate reports with 80% minimum."""
    session.install("coverage[toml]")
    session.run("coverage", "combine")
    session.run("coverage", "report", "--fail-under=80")
    session.run("coverage", "html")
    session.run("coverage", "json")


@nox.session
def lint(session: Session):
    """Run all linting checks (flake8, black, isort)."""
    session.install("flake8", "black==24.4.2", "isort")
    session.install(
        "-e",
        ".",
        "--extra-index-url",
        "https://mit-kavli-institute.github.io/MIT-Kavli-PyPi/",
    )

    # Run flake8
    session.log("Running flake8...")
    session.run("flake8", "src")

    # Check black formatting
    session.log("Checking black formatting...")
    session.run("black", "--check", "src", "tests")

    # Check import sorting
    session.log("Checking import sorting with isort...")
    session.run("isort", "--check-only", "src", "tests")


@nox.session(python="3.11")
def mypy(session: Session):
    """Run type checking with mypy."""
    session.install("mypy==1.10", "types-tabulate")
    session.install(
        "-e",
        ".",
        "--extra-index-url",
        "https://mit-kavli-institute.github.io/MIT-Kavli-PyPi/",
    )
    session.run("mypy", "src")


@nox.session
def format(session: Session):
    """Auto-format code with black and isort."""
    session.install("black==24.4.2", "isort")

    # Format with black
    session.log("Formatting with black...")
    session.run("black", "src", "tests")

    # Sort imports with isort
    session.log("Sorting imports with isort...")
    session.run("isort", "src", "tests")


@nox.session(python="3.11")
def docs(session: Session):
    """Build documentation."""
    session.install(
        "-e",
        ".",
        "--extra-index-url",
        "https://mit-kavli-institute.github.io/MIT-Kavli-PyPi/",
    )
    session.install("sphinx", "sphinx-rtd-theme")

    # Build HTML documentation
    session.run(
        "sphinx-build",
        "-b",
        "html",
        "docs/source",
        "docs/build/html",
        external=True,
    )
    session.log("Documentation built in docs/build/html/")


@nox.session(name="check")
def quick_check(session: Session):
    """Quick checks for development (lint + type check only)."""
    session.log("Running quick checks...")
    session.notify("lint")
    session.notify("mypy")


@nox.session
def validate(session: Session):
    """Run all validation checks (use before pushing).

    This runs all checks that GitHub Actions will run:
    - Linting (flake8, black, isort)
    - Type checking (mypy)
    - Tests with coverage
    - Coverage report with 80% minimum
    """
    session.log("Running full validation suite...")

    # Run linting first (fastest)
    session.notify("lint")

    # Type checking
    session.notify("mypy")

    # Run tests for Python 3.11 (can be changed via -p flag)
    python_version = session.posargs[0] if session.posargs else "3.11"
    session.notify(f"coverage-{python_version}")

    # Generate coverage report
    session.notify("coverage_report")

    session.log("All validation checks passed!")
