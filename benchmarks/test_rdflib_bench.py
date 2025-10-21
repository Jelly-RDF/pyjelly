# tests/test_rdflib_bench.py
from __future__ import annotations

from typing import Optional, Tuple

from pytest_benchmark.fixture import BenchmarkFixture


def test_rdflib_parse(
    benchmark: BenchmarkFixture, rdflib_input: Tuple[bytes, Optional[str]]
) -> None:
    from rdflib import Graph

    data, fmt = rdflib_input

    def run() -> Graph:
        g = Graph()
        if fmt:
            g.parse(data=data, format=fmt)
        else:
            g.parse(data=data)
        return g

    g = benchmark(run)
    assert len(g) >= 0 


def test_rdflib_serialize(
    benchmark: BenchmarkFixture, rdflib_input: Tuple[bytes, Optional[str]]
) -> None:
    from rdflib import Graph

    data, fmt = rdflib_input
    g = Graph()
    if fmt:
        g.parse(data=data, format=fmt)
    else:
        g.parse(data=data)

    def run() -> bytes:
        out: bytes = g.serialize(destination=None, format="jelly", encoding="utf-8")
        return out

    out = benchmark(run)
    assert isinstance(out, (bytes, bytearray)) and len(out) > 0
