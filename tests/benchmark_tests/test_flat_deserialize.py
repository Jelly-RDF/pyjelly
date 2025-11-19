from __future__ import annotations

import pytest
from pytest_benchmark.fixture import BenchmarkFixture  # type: ignore[import-not-found]

from tests.benchmark_tests.jelly_rdflib import (
    parse_jelly_quads_bytes,
    parse_jelly_triples_bytes,
    parse_nq_bytes,
    parse_nt_bytes,
)
from tests.utils.benchmark_throughput import print_throughput

pytest.importorskip(
    "pytest_benchmark",
    reason="Install bench dependency group and run with -m benchmark",
)

pytestmark = pytest.mark.benchmark


@pytest.mark.triples
def test_flat_triples_deserialize_nt(
    benchmark: BenchmarkFixture,
    nt_bytes_sliced: bytes,
    pedantic_cfg: dict[str, int],
    limit_statements: int,
) -> None:
    benchmark.pedantic(parse_nt_bytes, args=(nt_bytes_sliced,), **pedantic_cfg)
    print_throughput(benchmark, limit_statements, "triples: parse NT")


@pytest.mark.triples
def test_flat_triples_deserialize_jelly(
    benchmark: BenchmarkFixture,
    jelly_triples_bytes: bytes,
    pedantic_cfg: dict[str, int],
    limit_statements: int,
) -> None:
    benchmark.pedantic(
        parse_jelly_triples_bytes, args=(jelly_triples_bytes,), **pedantic_cfg
    )
    print_throughput(benchmark, limit_statements, "triples: parse Jelly")


@pytest.mark.quads
def test_flat_quads_deserialize_nq(
    benchmark: BenchmarkFixture,
    nq_bytes_sliced: bytes,
    pedantic_cfg: dict[str, int],
    limit_statements: int,
) -> None:
    benchmark.pedantic(parse_nq_bytes, args=(nq_bytes_sliced,), **pedantic_cfg)
    print_throughput(benchmark, limit_statements, "quads: parse NQ")


@pytest.mark.quads
def test_flat_quads_deserialize_jelly(
    benchmark: BenchmarkFixture,
    jelly_quads_bytes: bytes,
    pedantic_cfg: dict[str, int],
    limit_statements: int,
) -> None:
    benchmark.pedantic(
        parse_jelly_quads_bytes, args=(jelly_quads_bytes,), **pedantic_cfg
    )
    print_throughput(benchmark, limit_statements, "quads: parse Jelly")
