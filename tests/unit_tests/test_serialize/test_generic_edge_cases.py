# tests/unit/test_generic_serialize.py
from __future__ import annotations

from collections.abc import Generator

import pytest

from pyjelly import jelly
from pyjelly.integrations.generic.generic_sink import (
    DEFAULT_GRAPH_IDENTIFIER,
    IRI,
    BlankNode,
    GenericStatementSink,
    Literal,
    Quad,
    Triple,
)
from pyjelly.integrations.generic.serialize import (
    GenericSinkTermEncoder,
    flat_stream_to_frames,
    graphs_stream_frames,
    guess_options,
    guess_stream,
    quads_stream_frames,
    split_to_graphs,
    stream_frames,
    triples_stream_frames,
)
from pyjelly.options import LookupPreset, StreamParameters
from pyjelly.serialize.encode import Slot
from pyjelly.serialize.flows import FlatTriplesFrameFlow
from pyjelly.serialize.streams import (
    GraphStream,
    QuadStream,
    SerializerOptions,
    TripleStream,
)


def _triple_sink() -> GenericStatementSink:
    s = GenericStatementSink()
    s.bind("ex", IRI("http://example.com/"))
    t = Triple(IRI("http://ex/s1"), IRI("http://ex/p1"), IRI("http://ex/o1"))
    s.add(Triple(IRI("http://ex/s"), IRI("http://ex/p"), Literal("x", "en")))
    s.add(
        Triple(
            BlankNode("b"), IRI("http://ex/p"), Literal("2000", datatype="http://date")
        )
    )
    s.add(Triple(IRI("http://ex/s2"), IRI("http://ex/p2"), t))
    return s


def _quad_sink() -> GenericStatementSink:
    s = GenericStatementSink()
    s.bind("ex", IRI("http://example.com/"))
    g1 = IRI("http://ex/g1")
    g2 = IRI("http://ex/g2")
    s.add(
        Quad(
            IRI("http://ex/s"),
            IRI("http://ex/p"),
            IRI("http://ex/o"),
            DEFAULT_GRAPH_IDENTIFIER,
        )
    )
    s.add(Quad(IRI("http://ex/sA"), IRI("http://ex/pA"), IRI("http://ex/oA"), g1))
    s.add(Quad(IRI("http://ex/sB"), IRI("http://ex/pB"), IRI("http://ex/oB"), g2))
    return s


def test_stream_frames_typeerror() -> None:
    with pytest.raises(TypeError):
        list(stream_frames(object(), GenericStatementSink()))  # type: ignore[arg-type]


def test_split_to_graphs() -> None:
    g1 = IRI("http://g1")
    g2 = IRI("http://g2")

    def gen() -> Generator[Quad]:
        yield Quad(IRI("s1"), IRI("p1"), IRI("o1"), g1)
        yield Quad(IRI("s2"), IRI("p2"), IRI("o2"), g1)
        yield Quad(IRI("s3"), IRI("p3"), IRI("o3"), g2)

    graphs = list(split_to_graphs(gen()))
    assert len(graphs) == 2
    assert graphs[0].identifier == g1
    assert graphs[1].identifier == g2


def test_guess_options_and_stream() -> None:
    t_sink = _triple_sink()
    q_sink = _quad_sink()
    t_opts = guess_options(t_sink)
    q_opts = guess_options(q_sink)
    assert t_opts.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
    assert q_opts.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS
    assert isinstance(guess_stream(t_opts, t_sink), TripleStream)
    assert isinstance(guess_stream(q_opts, q_sink), QuadStream)


def test_flat_stream_to_frames_empty_generator() -> None:
    def empty_statements() -> Generator[Triple | Quad]:
        if False:
            yield
        return

    frames = list(flat_stream_to_frames(empty_statements()))
    assert frames == []


def test_flat_stream_guesses_options() -> None:
    def gen() -> Generator[Triple]:
        yield Triple(IRI("http://s"), IRI("http://p"), Literal("http://o"))

    frames = list(flat_stream_to_frames(gen()))
    assert frames
    assert isinstance(frames[-1], jelly.RdfStreamFrame)


def test_graphs_stream_frames_from_generator() -> None:
    options = SerializerOptions()
    stream = GraphStream(
        encoder=GenericSinkTermEncoder(),
        options=options,
    )

    def gen() -> Generator[Quad]:
        yield Quad(
            IRI("http://s1"), IRI("http://p1"), IRI("http://o1"), IRI("http://g1")
        )

    out = list(graphs_stream_frames(stream, gen()))
    assert out
    assert isinstance(out[-1], jelly.RdfStreamFrame)


def test_triples_stream_frames_declaration() -> None:
    options = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
        params=StreamParameters(namespace_declarations=True),
    )
    stream = TripleStream(
        encoder=GenericSinkTermEncoder(lookup_preset=LookupPreset()),
        options=options,
    )

    sink = GenericStatementSink()
    sink.bind("ex", IRI("http://example.com/"))
    sink.add(Triple(IRI("http://ex/s"), IRI("http://ex/p"), Literal("http://ex/o")))

    frames = list(triples_stream_frames(stream, sink))
    assert frames
    assert isinstance(frames[-1], jelly.RdfStreamFrame)


def test_triples_stream_frames_no_declarations() -> None:
    options = SerializerOptions(
        flow=FlatTriplesFrameFlow(),
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
        params=StreamParameters(namespace_declarations=False),
    )
    stream = TripleStream(
        encoder=GenericSinkTermEncoder(lookup_preset=LookupPreset()),
        options=options,
    )

    sink = GenericStatementSink()
    sink.add(
        Triple(
            IRI("http://example/s"),
            IRI("http://example/p"),
            Literal("http://example/o"),
        )
    )

    frames = list(triples_stream_frames(stream, sink))
    assert len(frames) == 1
    assert isinstance(frames[-1], jelly.RdfStreamFrame)


def test_quads_stream_frames_declarations() -> None:
    options = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
        params=StreamParameters(namespace_declarations=True),
    )
    stream = QuadStream(
        encoder=GenericSinkTermEncoder(lookup_preset=LookupPreset()),
        options=options,
    )

    sink = GenericStatementSink()
    sink.bind("ex", IRI("http://example.com/"))
    sink.add(
        Quad(
            IRI("http://ex/s1"),
            IRI("http://ex/p1"),
            IRI("http://ex/o1"),
            IRI("http://ex/g1"),
        )
    )
    sink.add(
        Quad(
            IRI("http://ex/s2"),
            IRI("http://ex/p2"),
            IRI("http://ex/o2"),
            IRI("http://ex/g2"),
        )
    )

    out = list(quads_stream_frames(stream, sink))
    assert out
    assert isinstance(out[-1], jelly.RdfStreamFrame)


def test_quads_stream_frames_no_declarations() -> None:
    options = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
        params=StreamParameters(),
    )
    stream = QuadStream(
        encoder=GenericSinkTermEncoder(lookup_preset=LookupPreset()),
        options=options,
    )

    sink = GenericStatementSink()
    sink.add(
        Quad(
            IRI("http://example/s1"),
            IRI("http://example/p1"),
            IRI("http://example/o1"),
            IRI("http://example/g1"),
        )
    )
    sink.add(
        Quad(
            IRI("http://example/s2"),
            IRI("http://example/p2"),
            IRI("http://example/o2"),
            IRI("http://example/g2"),
        )
    )

    out = list(quads_stream_frames(stream, sink))
    assert out
    assert isinstance(out[-1], jelly.RdfStreamFrame)


def test_graphs_stream_frames_declarations() -> None:
    options = SerializerOptions(
        params=StreamParameters(namespace_declarations=True),
    )
    stream = GraphStream(
        encoder=GenericSinkTermEncoder(lookup_preset=LookupPreset()),
        options=options,
    )

    sink = GenericStatementSink()
    sink.bind("ex", IRI("http://example.com/"))
    sink.add(
        Quad(
            IRI("http://ex/s1"),
            IRI("http://ex/p1"),
            IRI("http://ex/o1"),
            IRI("http://ex/g1"),
        )
    )
    sink.add(
        Quad(
            IRI("http://ex/s2"),
            IRI("http://ex/p2"),
            IRI("http://ex/o2"),
            IRI("http://ex/g2"),
        )
    )

    out = list(graphs_stream_frames(stream, sink))
    assert out
    assert isinstance(out[-1], jelly.RdfStreamFrame)


def test_encoder_unsupported_raises() -> None:
    enc = GenericSinkTermEncoder(lookup_preset=LookupPreset())
    with pytest.raises(NotImplementedError, match="unsupported term type"):
        enc.encode_any(object(), Slot.subject)
