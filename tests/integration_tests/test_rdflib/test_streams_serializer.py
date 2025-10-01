from __future__ import annotations

import io
from collections.abc import Generator

from rdflib import Dataset, Graph, Literal, Namespace, URIRef

from pyjelly import jelly
from pyjelly.integrations.rdflib.parse import Quad, Triple
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
    g1.add((URIRef(EX.p1), URIRef(EX.s1), Literal("example", lang="en")))
    g2 = ds.graph(URIRef(EX.g2))
    g2.add((URIRef(EX.p2), URIRef(EX.s2), URIRef(EX.o2)))
    return ds


def _triples_gen() -> Generator[Triple, None, None]:
    yield Triple(URIRef(EX.s1), URIRef(EX.p1), Literal("o1"))
    yield Triple(URIRef(EX.s2), URIRef(EX.p2), Literal("o2"))


def _quads_gen() -> Generator[Quad, None, None]:
    yield Quad(
        s=URIRef(EX.s1),
        p=URIRef(EX.p1),
        o=Literal("o1"),
        g=URIRef(EX.gq),
    )
    yield Quad(
        s=URIRef(EX.s2),
        p=URIRef(EX.p2),
        o=Literal("o2"),
        g=URIRef(EX.gq),
    )


def _empty_triples() -> Generator[tuple[URIRef, URIRef, Literal], None, None]:
    if False:
        yield


def test_grouped_stream_to_frames_graph_dataset() -> None:
    def _gen_graph_dataset() -> Generator[Graph | Dataset, None, None]:
        yield _graph_gen()
        yield _dataset_gen()

    frames = list(grouped_stream_to_frames(_gen_graph_dataset()))
    assert frames
    assert all(isinstance(f, jelly.RdfStreamFrame) for f in frames)


def test_grouped_stream_to_file() -> None:
    def _gen_graphs() -> Generator[Graph | Dataset, None, None]:
        yield _graph_gen()
        yield _graph_gen()

    buf = io.BytesIO()
    grouped_stream_to_file(_gen_graphs(), buf)
    assert buf.tell() > 0


def test_flat_stream_to_frames_triples() -> None:
    frames = list(flat_stream_to_frames(_triples_gen()))
    assert len(frames) >= 1
    assert all(isinstance(f, jelly.RdfStreamFrame) for f in frames)


def test_flat_stream_to_file_quads() -> None:
    buf = io.BytesIO()
    flat_stream_to_file(_quads_gen(), buf)
    assert buf.tell() > 0


def test_flat_stream_to_frames_empty() -> None:
    frames = list(flat_stream_to_frames(_empty_triples()))  # type: ignore[arg-type]
    assert frames == []


def test_graphs_stream_frames_namespace() -> None:
    ds = _dataset_gen()
    opts = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_DATASETS,
        params=StreamParameters(namespace_declarations=True),
    )
    stream: GraphStream = GraphStream.for_rdflib(options=opts)  # type: ignore[assignment]

    frames = list(graphs_stream_frames(stream, ds))
    assert len(frames) >= 1
    assert all(isinstance(f, jelly.RdfStreamFrame) for f in frames)


def test_graphs_stream_frames_from_quads_generator() -> None:
    opts = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_DATASETS,
        params=StreamParameters(namespace_declarations=False),
    )
    stream: GraphStream = GraphStream.for_rdflib(options=opts)  # type: ignore[assignment]

    frames = list(graphs_stream_frames(stream, _quads_gen()))
    assert len(frames) >= 1
    assert all(isinstance(f, jelly.RdfStreamFrame) for f in frames)


def test_quads_stream_frames_namespace() -> None:
    ds = _dataset_gen()

    opts = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
        params=StreamParameters(namespace_declarations=True),
    )
    stream: QuadStream = QuadStream.for_rdflib(options=opts)  # type: ignore[assignment]

    frames = list(quads_stream_frames(stream, ds))
    assert len(frames) >= 1
    assert all(isinstance(f, jelly.RdfStreamFrame) for f in frames)


def test_quads_stream_frames_from_quads_generator() -> None:
    opts = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
        params=StreamParameters(namespace_declarations=False),
    )
    stream: QuadStream = QuadStream.for_rdflib(options=opts)  # type: ignore[assignment]

    frames = list(quads_stream_frames(stream, _quads_gen()))
    assert len(frames) >= 1
    assert all(isinstance(f, jelly.RdfStreamFrame) for f in frames)
