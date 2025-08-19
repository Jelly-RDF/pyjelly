# tests/unit/test_generic_parse.py
from __future__ import annotations

from collections.abc import Iterable
from io import BytesIO
from typing import cast

import pytest

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.generic import parse as gparse
from pyjelly.integrations.generic.generic_sink import (
    IRI,
    Prefix,
    Quad,
)
from pyjelly.integrations.generic.parse import (
    GenericGraphsAdapter,
    GenericStatementSinkAdapter,
    parse_jelly_flat,
    parse_jelly_grouped,
    parse_quads_stream,
)
from pyjelly.options import LookupPreset, StreamParameters, StreamTypes
from pyjelly.parse.decode import ParserOptions


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
