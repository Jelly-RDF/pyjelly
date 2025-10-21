# tests/benchmarks/test_generic_bench.py
from __future__ import annotations

import io
from pytest_benchmark.fixture import BenchmarkFixture
from pyjelly.integrations.generic.generic_sink import GenericStatementSink

def test_generic_parse(benchmark: BenchmarkFixture, jelly_bytes: bytes) -> None:

    def run() -> "GenericStatementSink":
        sink = GenericStatementSink()
        with io.BytesIO(jelly_bytes) as buf:
            sink.parse(buf)
        return sink

    sink = benchmark(run)
    assert sink is not None

def test_generic_serialize(benchmark: BenchmarkFixture, jelly_bytes: bytes) -> None:

    sink = GenericStatementSink()
    with io.BytesIO(jelly_bytes) as buf:
        sink.parse(buf)

    def run() -> None:
        sink.serialize(io.BytesIO())

    benchmark(run)
