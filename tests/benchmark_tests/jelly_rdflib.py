from __future__ import annotations

import io
from contextlib import suppress

from rdflib import Dataset, Graph

from tests.utils.benchmark_io_utils import NullCounter


def parse_nt_bytes(nt_bytes: bytes) -> Graph:
    g = Graph()
    g.parse(data=nt_bytes, format="nt")
    return g


def parse_jelly_triples_bytes(jelly_bytes: bytes) -> Graph:
    g = Graph()
    g.parse(data=jelly_bytes, format="jelly")
    return g


def parse_nq_bytes(nq_bytes: bytes) -> Dataset:
    ds = Dataset()
    ds.parse(data=nq_bytes, format="nquads")
    return ds


def parse_jelly_quads_bytes(jelly_bytes: bytes) -> Dataset:
    ds = Dataset()
    ds.parse(data=jelly_bytes, format="jelly")
    return ds


def serialize_nt_stream(g: Graph) -> int:
    sink = NullCounter()
    buf = io.BufferedWriter(sink)
    g.serialize(destination=buf, format="nquads", encoding="utf-8")
    buf.flush()
    with suppress(io.UnsupportedOperation, ValueError):
        buf.detach()
    return sink.n


def serialize_jelly_triples_stream(g: Graph) -> int:
    sink = NullCounter()
    buf = io.BufferedWriter(sink)
    g.serialize(destination=buf, format="jelly", encoding="utf-8")
    buf.flush()
    with suppress(io.UnsupportedOperation, ValueError):
        buf.detach()
    return sink.n


def serialize_nq_stream(ds: Dataset) -> int:
    sink = NullCounter()
    buf = io.BufferedWriter(sink)
    ds.serialize(destination=buf, format="nquads", encoding="utf-8")
    buf.flush()
    with suppress(io.UnsupportedOperation, ValueError):
        buf.detach()
    return sink.n


def serialize_jelly_quads_stream(ds: Dataset) -> int:
    sink = NullCounter()
    buf = io.BufferedWriter(sink)
    ds.serialize(destination=buf, format="jelly", encoding="utf-8")
    buf.flush()
    with suppress(io.UnsupportedOperation, ValueError):
        buf.detach()
    return sink.n
