# tests/unit/test_generic_parse.py
from __future__ import annotations

import io
from collections.abc import Iterable
from io import BytesIO
from itertools import chain
from typing import cast

import pytest

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.generic import parse as gparse
from pyjelly.integrations.generic.generic_sink import (
    DEFAULT_GRAPH_IDENTIFIER,
    IRI,
    BlankNode,
    GenericStatementSink,
    Literal,
    Prefix,
    Quad,
    Triple,
)
from pyjelly.integrations.generic.parse import (
    GenericGraphsAdapter,
    GenericStatementSinkAdapter,
    parse_jelly_flat,
    parse_jelly_grouped,
    parse_jelly_to_graph,
    parse_quads_stream,
)
from pyjelly.integrations.generic.serialize import grouped_stream_to_file
from pyjelly.options import LookupPreset, StreamParameters, StreamTypes
from pyjelly.parse.decode import ParserOptions
from pyjelly.serialize.flows import (
    DatasetsFrameFlow,
    GraphsFrameFlow,
)
from pyjelly.serialize.streams import SerializerOptions


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


def test_parse_grouped_from_flat_triples() -> None:
    sink = _triple_sink()
    out = io.BytesIO()
    opts = SerializerOptions(
        flow=GraphsFrameFlow(),
        logical_type=jelly.LOGICAL_STREAM_TYPE_GRAPHS,
    )
    grouped_stream_to_file((x for x in [sink]), out, options=opts)
    back_sinks = list(parse_jelly_grouped(io.BytesIO(out.getvalue())))
    combined = list(chain.from_iterable(list(b.store) for b in back_sinks))
    assert set(combined) == set(sink)


def test_parse_grouped_from_flat_quads() -> None:
    sink = _quad_sink()
    out = io.BytesIO()
    opts = SerializerOptions(
        flow=DatasetsFrameFlow(), logical_type=jelly.LOGICAL_STREAM_TYPE_DATASETS
    )
    grouped_stream_to_file((x for x in [sink]), out, options=opts)
    back_sinks = list(parse_jelly_grouped(io.BytesIO(out.getvalue())))
    combined = list(chain.from_iterable(list(b.store) for b in back_sinks))
    assert set(combined) == set(sink)


def test_parse_jelly_grouped() -> None:
    sink = _triple_sink()
    out = io.BytesIO()
    opts = SerializerOptions(
        flow=GraphsFrameFlow(),
        logical_type=jelly.LOGICAL_STREAM_TYPE_GRAPHS,
    )
    grouped_stream_to_file((x for x in [sink]), out, options=opts)
    back = parse_jelly_to_graph(io.BytesIO(out.getvalue()))
    assert set(back) == set(sink)


def test_parse_jelly_flat_unsupported_physical_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opts = ParserOptions(
        stream_types=StreamTypes(),
        lookup_preset=LookupPreset(),
        params=StreamParameters(),
    )
    monkeypatch.setattr(
        gparse,
        "get_options_and_frames",
        lambda _: (opts, iter(())),
    )
    with pytest.raises(NotImplementedError):
        list(parse_jelly_flat(BytesIO(b"\x00\x00\x00")))


def test_parse_jelly_grouped_unsupported_physical_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opts = ParserOptions(
        stream_types=StreamTypes(),
        lookup_preset=LookupPreset(),
        params=StreamParameters(),
    )
    monkeypatch.setattr(
        gparse,
        "get_options_and_frames",
        lambda _: (opts, iter(())),
    )
    with pytest.raises(NotImplementedError):
        list(parse_jelly_grouped(BytesIO(b"\x00\x00\x00")))


def test_namespace_declaration_returns_prefix() -> None:
    opts = ParserOptions(
        stream_types=StreamTypes(),
        lookup_preset=LookupPreset(),
        params=StreamParameters(),
    )
    adapter = GenericStatementSinkAdapter(options=opts)
    p = adapter.namespace_declaration("ex", "http://example.com/")
    assert isinstance(p, Prefix)
    assert p.prefix == "ex"
    assert p.iri == IRI("http://example.com/")


def test_graph_start_end_id() -> None:
    adapter = GenericGraphsAdapter(
        ParserOptions(
            stream_types=StreamTypes(),
            lookup_preset=LookupPreset(),
            params=StreamParameters(),
        )
    )
    adapter.graph_start(IRI("https://g1.com"))
    assert adapter._graph_id == IRI("https://g1.com")
    adapter.graph_end()
    assert adapter._graph_id is None


def test_graph_raises_without_start() -> None:
    adapter = GenericGraphsAdapter(
        ParserOptions(
            stream_types=StreamTypes(),
            lookup_preset=LookupPreset(),
            params=StreamParameters(),
        )
    )
    with pytest.raises(JellyConformanceError):
        _ = adapter.graph


def test_graphs_adapter_triple_appends_graph_id() -> None:
    adapter = GenericGraphsAdapter(
        ParserOptions(
            stream_types=StreamTypes(),
            lookup_preset=LookupPreset(),
            params=StreamParameters(),
        )
    )
    adapter.graph_start(IRI("https://g1.com"))
    s, p, o = IRI("http://s"), IRI("http://p"), IRI("http://o")
    assert adapter.triple([s, p, o]) == Quad(s, p, o, IRI("https://g1.com"))


def test_stream_graphs_adapter() -> None:
    frames: Iterable[jelly.RdfStreamFrame] = cast(
        Iterable[jelly.RdfStreamFrame], BytesIO(b"\x00\x00\x00")
    )
    options = ParserOptions(
        stream_types=StreamTypes(),
        lookup_preset=LookupPreset(),
        params=StreamParameters(),
    )

    out = list(parse_quads_stream(frames, options))
    assert out
