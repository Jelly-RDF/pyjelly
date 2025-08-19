# tests/unit/test_generic_parse.py
from __future__ import annotations

import io
from collections.abc import Iterator
from io import BytesIO
from typing import IO, Any

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
)
from pyjelly.options import LookupPreset, StreamParameters, StreamTypes
from pyjelly.parse.decode import ParserOptions


def test_parse_jelly_flat_unsupported_physical_type_raises() -> None:
    opts = ParserOptions(
        stream_types=StreamTypes(),
        lookup_preset=LookupPreset(),
        params=StreamParameters(),
    )
    with pytest.raises(NotImplementedError):
        list(parse_jelly_flat(io.BytesIO(b"data"), frames=iter(()), options=opts))


def test_parse_flat_get_options(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"flag": False}
    opts = ParserOptions(
        stream_types=StreamTypes(
            physical_type=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
            logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
        ),
        lookup_preset=LookupPreset(),
        params=StreamParameters(),
    )

    def dummy_options(_: IO[bytes]) -> tuple[ParserOptions, Iterator[Any]]:
        called["flag"] = True
        return opts, iter(())

    monkeypatch.setattr(gparse, "get_options_and_frames", dummy_options)

    out = list(parse_jelly_flat(io.BytesIO(b"x")))
    assert called["flag"] is True
    assert out == []


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
        list(parse_jelly_grouped(BytesIO(b"data")))


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
