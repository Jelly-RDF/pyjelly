from __future__ import annotations

import pytest
from pytest_benchmark.fixture import BenchmarkFixture  # type: ignore[import-not-found]
from rdflib import Dataset, Graph

from tests.benchmark_tests.jelly_rdflib import (
    serialize_jelly_quads_stream,
    serialize_jelly_triples_stream,
    serialize_nq_stream,
    serialize_nt_stream,
)
from tests.utils.benchmark_throughput import print_throughput

pytest.importorskip(
    "pytest_benchmark",
    reason="Install bench dependency group and run with -m benchmark",
)

pytestmark = pytest.mark.benchmark


@pytest.mark.triples
def test_flat_triples_serialize_nt(
    benchmark: BenchmarkFixture,
    nt_graph: Graph,
    pedantic_cfg: dict[str, int],
    limit_statements: int,
) -> None:
    benchmark.pedantic(serialize_nt_stream, args=(nt_graph,), **pedantic_cfg)
    print_throughput(benchmark, limit_statements, "triples: serialize NT")


@pytest.mark.triples
def test_flat_triples_serialize_jelly(
    benchmark: BenchmarkFixture,
    nt_graph: Graph,
    pedantic_cfg: dict[str, int],
    limit_statements: int,
) -> None:
    benchmark.pedantic(serialize_jelly_triples_stream, args=(nt_graph,), **pedantic_cfg)
    print_throughput(benchmark, limit_statements, "triples: serialize Jelly")


@pytest.mark.quads
def test_flat_quads_serialize_nq(
    benchmark: BenchmarkFixture,
    nq_dataset: Dataset,
    pedantic_cfg: dict[str, int],
    limit_statements: int,
) -> None:
    benchmark.pedantic(serialize_nq_stream, args=(nq_dataset,), **pedantic_cfg)
    print_throughput(benchmark, limit_statements, "quads: serialize NQ")


@pytest.mark.quads
def test_flat_quads_serialize_jelly(
    benchmark: BenchmarkFixture,
    nq_dataset: Dataset,
    pedantic_cfg: dict[str, int],
    limit_statements: int,
) -> None:
    benchmark.pedantic(serialize_jelly_quads_stream, args=(nq_dataset,), **pedantic_cfg)
    print_throughput(benchmark, limit_statements, "quads: serialize Jelly")
