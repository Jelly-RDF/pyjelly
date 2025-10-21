# conftest.py
from __future__ import annotations

import importlib
import io
import sys
from pathlib import Path
from typing import Optional, Tuple

import pytest
from _pytest.config.argparsing import Parser
from _pytest.fixtures import FixtureRequest


def pytest_addoption(parser: Parser) -> None:
    parser.addoption(
        "--generic-jelly",
        action="store",
        default="nanopub_100k.jelly",
        help="Path to the .jelly file for generic parsing.",
    )
    parser.addoption(
        "--rdflib-in",
        action="store",
        default="nanopub_100k.nq",
        help="Path to the RDF input file for rdflib (e.g., .nq, .nt, .ttl).",
    )
    parser.addoption(
        "--wheels-dir",
        action="store",
        default=None,
        help="Directory containing compiled mypyc wheels to prepend to sys.path.",
    )


@pytest.fixture(scope="session", autouse=True)
def maybe_enable_wheels(request: FixtureRequest) -> None:
    wheels: Optional[str] = request.config.getoption("--wheels-dir")
    if wheels:
        p = Path(wheels).resolve()
        assert p.exists(), f"--wheels-dir not found: {p}"
        sys.path.insert(0, str(p))
        importlib.invalidate_caches()

@pytest.fixture(scope="session")
def jelly_bytes(request: FixtureRequest) -> bytes:
    p = Path(request.config.getoption("--generic-jelly"))
    data: bytes = p.read_bytes()
    assert data, f"Empty or missing file: {p}"
    return data


@pytest.fixture(scope="session")
def rdflib_input(request: FixtureRequest) -> Tuple[bytes, Optional[str]]:
    infile = Path(request.config.getoption("--rdflib-in"))
    assert infile.exists(), f"Missing rdflib input: {infile}"
    data: bytes = infile.read_bytes()

    ext = infile.suffix.lower()
    fmt: Optional[str]
    if ext in (".nq", ".nquads"):
        fmt = "nquads"
    elif ext in (".nt", ".ntriples"):
        fmt = "nt"
    elif ext == ".ttl":
        fmt = "turtle"
    elif ext in (".rdf", ".xml"):
        fmt = "xml"
    elif ext == ".jsonld":
        fmt = "json-ld"
    else:
        fmt = None

    return data, fmt