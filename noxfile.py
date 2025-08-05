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
