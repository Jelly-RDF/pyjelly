"""Reading a Jelly stream of quads into rdflib's Graph, Dataset and ConjunctiveGraph."""

from __future__ import annotations

import io

import pytest
from rdflib import Dataset, Graph, URIRef
from rdflib.graph import ConjunctiveGraph

TRIG = """
<http://example.org/a> <http://example.org/p> <http://example.org/b> .
<http://example.org/g> {
    <http://example.org/c> <http://example.org/p> <http://example.org/d> .
}
"""
DEFAULT_TRIPLE = (
    URIRef("http://example.org/a"),
    URIRef("http://example.org/p"),
    URIRef("http://example.org/b"),
)
NAMED_TRIPLE = (
    URIRef("http://example.org/c"),
    URIRef("http://example.org/p"),
    URIRef("http://example.org/d"),
)
NAMED_GRAPH = URIRef("http://example.org/g")


def _jelly_quads() -> bytes:
    dataset = Dataset()
    dataset.parse(data=TRIG, format="trig")
    data = dataset.serialize(format="jelly", encoding="utf-8")
    assert isinstance(data, bytes)
    return data


@pytest.mark.parametrize("format_", ["jelly", "trig"])
def test_quads_into_graph(format_: str) -> None:
    data = _jelly_quads() if format_ == "jelly" else TRIG.encode()
    graph = Graph()
    graph.parse(io.BytesIO(data), format=format_)
    assert set(graph) == {DEFAULT_TRIPLE}
    named = Graph(store=graph.store, identifier=NAMED_GRAPH)
    assert set(named) == {NAMED_TRIPLE}


@pytest.mark.parametrize("format_", ["jelly", "trig"])
def test_quads_into_dataset(format_: str) -> None:
    data = _jelly_quads() if format_ == "jelly" else TRIG.encode()
    dataset = Dataset()
    dataset.parse(io.BytesIO(data), format=format_)
    assert set(dataset.default_graph) == {DEFAULT_TRIPLE}
    assert set(dataset.graph(NAMED_GRAPH)) == {NAMED_TRIPLE}


@pytest.mark.parametrize("format_", ["jelly", "trig"])
def test_quads_into_conjunctive_graph(format_: str) -> None:
    data = _jelly_quads() if format_ == "jelly" else TRIG.encode()
    graph = ConjunctiveGraph()
    graph.parse(io.BytesIO(data), format=format_)
    assert set(graph.default_context) == {DEFAULT_TRIPLE}
    assert set(graph.get_context(NAMED_GRAPH)) == {NAMED_TRIPLE}
