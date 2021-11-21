"""Automated testing."""
import nox


@nox.session()
def formatting(session):
    """Will tests current codebase for formatting."""
    session.install("black", "flake8", "isort", "flake8-docstrings")
    session.run("isort", "entsoe/", "tests/", "--check-only")
    session.run("flake8", "entsoe/", "tests/", "--max-line-length=88", "--docstring-convention", "numpy")
    session.run("black", "entsoe/", "tests/", "--check")


@nox.session(python=['3.7', '3.9'])
def tests(session):
    """Will execute tests on codebase."""
    session.install("pytest", "pytest-cov", "pytest-mock", "requests_mock")
    session.install("-e", ".")
    session.run("pytest", "--cov=entsoe", "--cov-report=html", "tests/")
