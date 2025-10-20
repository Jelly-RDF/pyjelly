# tests/test_generic_bench.py
from __future__ import annotations

import io
from typing import Any
from pyjelly.integrations.generic.generic_sink import *
from pytest_benchmark.fixture import BenchmarkFixture


def test_generic_parse(benchmark: BenchmarkFixture, jelly_bytes: bytes) -> None:
    # Import inside test so --wheels-dir sys.path change applies before import.

    def run() -> "GenericStatementSink":
        sink = GenericStatementSink()
        with io.BytesIO(jelly_bytes) as buf:
            sink.parse(buf)
        return sink

    sink = benchmark(run)
    assert sink is not None


def test_generic_serialize(benchmark: BenchmarkFixture, jelly_bytes: bytes) -> None:
    from pyjelly.integrations.generic.generic_sink import GenericStatementSink

    # Pre-parse once (outside timed region) so serialize is isolated.
    sink = GenericStatementSink()
    with io.BytesIO(jelly_bytes) as buf:
        sink.parse(buf)

    def run() -> None:
        sink.serialize(io.BytesIO())  # fully in-memory

    benchmark(run)
