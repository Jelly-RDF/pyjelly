# tests/unit/test_generic_serialize.py
from __future__ import annotations

from collections.abc import Generator

import pytest

from pyjelly import jelly
from pyjelly.integrations.generic.generic_sink import (
    IRI,
    GenericStatementSink,
    Literal,
    Quad,
    Triple,
)
from pyjelly.integrations.generic.serialize import (
    GenericSinkTermEncoder,
    flat_stream_to_frames,
    grouped_stream_to_frames,
    guess_options,
    guess_stream,
    quads_stream_frames,
    stream_frames,
    triples_stream_frames,
)
from pyjelly.options import LookupPreset, StreamParameters
from pyjelly.serialize.encode import Slot
from pyjelly.serialize.streams import (
    QuadStream,
    SerializerOptions,
    TripleStream,
)


def test_stream_frames_typeerror() -> None:
    with pytest.raises(TypeError):
        list(stream_frames(object(), GenericStatementSink()))  # type: ignore[arg-type]


def test_guess_options_and_stream() -> None:
    def _triple_sink() -> GenericStatementSink:
        s = GenericStatementSink()
        s.bind("ex", IRI("http://example.com/"))
        s.add(Triple(IRI("http://ex/s1"), IRI("http://ex/p1"), IRI("http://ex/o1")))
        s.add(
            Triple(IRI("http://ex/s2"), IRI("http://ex/p2"), Literal("example_value"))
        )
        return s

    def _quad_sink() -> GenericStatementSink:
        s = GenericStatementSink()
        s.bind("ex", IRI("http://example.com/"))
        g1 = IRI("http://ex/g1")
        g2 = IRI("http://ex/g2")
        s.add(
            Quad(IRI("http://ex/sg1"), IRI("http://ex/pg1"), IRI("http://ex/og1"), g1)
        )
        s.add(
            Quad(IRI("http://ex/sg2"), IRI("http://ex/pg2"), IRI("http://ex/og2"), g2)
        )
        return s

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


@pytest.mark.parametrize("with_namespace", [True, False])
def test_triples_stream_frames_parametrized(*, with_namespace: bool) -> None:
    options = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
        params=StreamParameters(namespace_declarations=with_namespace),
    )
    stream = TripleStream(
        encoder=GenericSinkTermEncoder(lookup_preset=LookupPreset()),
        options=options,
    )

    sink = GenericStatementSink()
    if with_namespace:
        sink.bind("ex", IRI("http://example.com/"))
    sink.add(Triple(IRI("http://ex/s"), IRI("http://ex/p"), Literal("http://ex/o")))

    frames = list(triples_stream_frames(stream, sink))
    assert frames
    assert isinstance(frames[-1], jelly.RdfStreamFrame)


@pytest.mark.parametrize("with_namespace", [True, False])
def test_quads_stream_frames_parametrized(*, with_namespace: bool) -> None:
    options = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
        params=StreamParameters(namespace_declarations=with_namespace),
    )
    stream = QuadStream(
        encoder=GenericSinkTermEncoder(lookup_preset=LookupPreset()),
        options=options,
    )

    sink = GenericStatementSink()
    if with_namespace:
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


def test_encoder_unsupported_raises() -> None:
    enc = GenericSinkTermEncoder(lookup_preset=LookupPreset())
    with pytest.raises(NotImplementedError, match="unsupported term type"):
        enc.encode_spo(object(), Slot.subject, jelly.RdfTriple())


def test_grouped_stream_to_frames_init_stream_guess_options() -> None:
    s1 = GenericStatementSink()
    s1.add(Triple(IRI("http://ex/s1"), IRI("http://ex/p1"), Literal("http://ex/o1")))
    s2 = GenericStatementSink()
    s2.add(Triple(IRI("http://ex/s2"), IRI("http://ex/p2"), Literal("http://ex/o2")))

    def gen() -> Generator[GenericStatementSink, None, None]:
        yield s1
        yield s2

    frames = list(grouped_stream_to_frames(gen(), options=None))
    expected = 2
    assert len(frames) == expected


def test_graph_not_implemented() -> None:
    enc = GenericSinkTermEncoder(lookup_preset=LookupPreset())
    with pytest.raises(NotImplementedError, match="unsupported term type"):
        enc.encode_graph(
            Triple(IRI("http://ex/s1"), IRI("http://ex/p1"), IRI("http://ex/o1")),
            jelly.RdfQuad(),
        )
