from __future__ import annotations

import io
from collections.abc import Generator

from rdflib import Dataset, Graph, Literal, Namespace, URIRef

from pyjelly import jelly
from pyjelly.integrations.rdflib.parse import Quad
from pyjelly.integrations.rdflib.serialize import (
    flat_stream_to_file,
    flat_stream_to_frames,
    graphs_stream_frames,
    grouped_stream_to_file,
    grouped_stream_to_frames,
    quads_stream_frames,
)
from pyjelly.options import StreamParameters
from pyjelly.serialize.streams import GraphStream, QuadStream, SerializerOptions

EX = Namespace("http://example.org/")



def _graph_gen() -> Graph:
    g: Graph = Graph()
    g.bind("ex", EX)
    g.add((URIRef(EX.s1), URIRef(EX.p1), URIRef(EX.o1)))
    g.add((URIRef(EX.s2), URIRef(EX.p2), Literal("test", lang="en")))
    g.add((URIRef(EX.s3), URIRef(EX.p3), Literal("example", lang="en")))
    return g


def _dataset_gen() -> Dataset:
    ds: Dataset = Dataset()
    g1 = ds.graph(URIRef(EX.g1))
    g1.add((URIRef(EX.a), URIRef(EX.b), Literal("example", lang="en")))
    g2 = ds.graph(URIRef(EX.g2))
    g2.add((URIRef(EX.x), URIRef(EX.y), URIRef(EX.z)))
    return ds

def _triples_gen() -> Generator[tuple[URIRef, URIRef, Literal], None, None]:
        yield (URIRef(EX.s1), URIRef(EX.p1), Literal("o1"))
        yield (URIRef(EX.s2), URIRef(EX.p2), Literal("o2"))


def test_grouped_stream_to_frames_with_graph_then_dataset() -> None:
    def gen() -> Generator[Graph | Dataset, None, None]:
        yield _graph_gen()
        yield _dataset_gen()

    frames = list(grouped_stream_to_frames(gen()))
    assert frames
    assert all(isinstance(f, jelly.RdfStreamFrame) for f in frames)


def test_grouped_stream_to_file_writes_bytes() -> None:
    def gen() -> Generator[Graph | Dataset, None, None]:
        yield _graph_gen()
        yield _graph_gen()

    buf = io.BytesIO()
    grouped_stream_to_file(gen(), buf)
    assert buf.tell() > 0


def test_flat_stream_to_frames_triples() -> None:
    frames = list(flat_stream_to_frames(_triples_gen()))
    assert len(frames) >= 1
    assert all(isinstance(f, jelly.RdfStreamFrame) for f in frames)


def test_flat_stream_to_file_quads_writes_bytes() -> None:
    def quads() -> Generator[
        tuple[URIRef, URIRef, Literal | URIRef, URIRef], None, None
    ]:
        yield (URIRef(EX.sq), URIRef(EX.pq), Literal("oq"), URIRef(EX.gq))
        yield (URIRef(EX.sq2), URIRef(EX.pq2), URIRef(EX.oq2), URIRef(EX.gq))

    buf = io.BytesIO()
    flat_stream_to_file(quads(), buf)
    assert buf.tell() > 0


def _empty_triples() -> Generator[tuple[URIRef, URIRef, Literal], None, None]:
    if False:
        yield  # type: ignore[misc]


def test_flat_stream_to_frames_empty_input_returns_immediately() -> None:
    frames = list(flat_stream_to_frames(_empty_triples()))
    assert frames == []


def test_graphs_stream_frames_namespace_declarations_enabled() -> None:
    ds: Dataset = Dataset()
    ds.bind("ex", EX)
    g1 = ds.graph(URIRef(EX.g1))
    g1.add((URIRef(EX.s1), URIRef(EX.p1), Literal("o1")))
    g2 = ds.graph(URIRef(EX.g2))
    g2.add((URIRef(EX.s2), URIRef(EX.p2), URIRef(EX.o2)))

    opts = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_DATASETS,
        params=StreamParameters(namespace_declarations=True),
    )
    stream: GraphStream = GraphStream.for_rdflib(options=opts)

    frames = list(graphs_stream_frames(stream, ds))
    assert len(frames) >= 1
    assert all(isinstance(f, jelly.RdfStreamFrame) for f in frames)


def test_graphs_stream_frames_from_quads_generator_else_branch() -> None:
    def quads() -> Generator[Quad, None, None]:
        yield Quad(
            s=URIRef(EX.sq1),
            p=URIRef(EX.pq1),
            o=Literal("oq1"),
            g=URIRef(EX.gq),
        )
        yield Quad(
            s=URIRef(EX.sq2),
            p=URIRef(EX.pq2),
            o=URIRef(EX.oq2),
            g=URIRef(EX.gq),
        )

    opts = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_DATASETS,
        params=StreamParameters(namespace_declarations=False),
    )
    stream: GraphStream = GraphStream.for_rdflib(options=opts)

    frames = list(graphs_stream_frames(stream, quads()))
    assert len(frames) >= 1
    assert all(isinstance(f, jelly.RdfStreamFrame) for f in frames)


def test_quads_stream_frames_namespace_declarations_enabled() -> None:
    ds: Dataset = Dataset()
    ds.bind("ex", EX)
    g1: Graph = ds.graph(URIRef(EX.g1))
    g1.add((URIRef(EX.s1), URIRef(EX.p1), Literal("o1")))
    g2: Graph = ds.graph(URIRef(EX.g2))
    g2.add((URIRef(EX.s2), URIRef(EX.p2), URIRef(EX.o2)))

    opts = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
        params=StreamParameters(namespace_declarations=True),
    )
    stream: QuadStream = QuadStream.for_rdflib(options=opts)

    frames = list(quads_stream_frames(stream, ds))
    assert len(frames) >= 1
    assert all(isinstance(f, jelly.RdfStreamFrame) for f in frames)


def test_quads_stream_frames_from_quads_generator_else_branch() -> None:
    def quads() -> Generator[Quad, None, None]:
        yield Quad(
            s=URIRef(EX.sq1),
            p=URIRef(EX.pq1),
            o=Literal("oq1"),
            g=URIRef(EX.gq),
        )
        yield Quad(
            s=URIRef(EX.sq2),
            p=URIRef(EX.pq2),
            o=URIRef(EX.oq2),
            g=URIRef(EX.gq),
        )

    opts = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
        params=StreamParameters(namespace_declarations=False),
    )
    stream: QuadStream = QuadStream.for_rdflib(options=opts)

    frames = list(quads_stream_frames(stream, quads()))
    assert len(frames) >= 1
    assert all(isinstance(f, jelly.RdfStreamFrame) for f in frames)
