"""
Shared pytest fixtures for offline parser tests.

The XML files in tests/fixtures/ are synthetic — constructed by reading
the parser code and producing minimal documents that exercise the tag
structures the parsers expect. They are not real ENTSO-E API responses.
"""
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def load_fixture():
    """Return a callable that loads a fixture file as a string."""
    def _load(name: str) -> str:
        return (FIXTURES_DIR / name).read_text(encoding="utf-8")
    return _load
