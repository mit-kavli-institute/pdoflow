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
    session.install("coverage[toml]")
    session.run("coverage", "combine")
    session.run("coverage", "report")
    session.run("coverage", "html")
    session.run("coverage", "json")
