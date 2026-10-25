import io
from typing import Any
from typing_extensions import Self

import pytest
import rdflib
from rdflib import XSD, BNode, Dataset, Graph, Literal, URIRef
from rdflib.parser import InputSource

from pyjelly.integrations.rdflib.parse import RDFLibJellyParser, parse_jelly_to_graph


def test_parse_raises_type_no_bytestream() -> None:
    class DummyInputSource(InputSource):
        pass

    parser = RDFLibJellyParser()  # type: ignore[no-untyped-call]
    source = DummyInputSource()
    sink = Graph()

    with pytest.raises(TypeError, match="expected source to be a stream of bytes"):
        parser.parse(source, sink)


@pytest.mark.parametrize(
    ("lex", "datatype"),
    [
        ("2013-12-31T22:50:02Z", XSD.dateTime),
        ("01", XSD.integer),
        ("1.47783E9", XSD.double),
        ("not a number", XSD.integer),
    ],
)
def test_literal_lexical_form_is_kept(
    lex: str, datatype: URIRef, monkeypatch: pytest.MonkeyPatch
) -> None:
    # rdflib normalizes these by default, e.g., "01" to "1". The tests' conftest
    # turns that off, so turn it back on here
    monkeypatch.setattr(rdflib, "NORMALIZE_LITERALS", True)
    graph = Graph()
    graph.add(
        (
            URIRef("http://ex/s"),
            URIRef("http://ex/p"),
            Literal(lex, datatype=datatype, normalize=False),
        )
    )
    parsed = Graph().parse(
        data=graph.serialize(format="jelly", encoding="utf-8"), format="jelly"
    )
    ((_, _, literal),) = parsed
    assert isinstance(literal, Literal)
    assert str(literal) == lex
    assert literal.datatype == datatype


class CountingDataset(Dataset):
    """A Dataset that changes add(), so the parser must call it."""

    def __init__(self) -> None:
        super().__init__()
        self.added = 0

    def add(self, triple_or_quad: Any) -> Self:
        self.added += 1
        return super().add(triple_or_quad)


class CountingGraph(Graph):
    def __init__(self) -> None:
        super().__init__()
        self.added = 0

    def add(self, triple: Any) -> Self:
        self.added += 1
        return super().add(triple)


def test_dataset_loads_the_same_through_the_store_and_add() -> None:
    # More statements than one batch and more graphs than the context cache
    dataset = Dataset()
    for i in range(1500):
        graph = dataset.graph(URIRef(f"http://ex/g{i % 1200}") if i % 3 else BNode())
        graph.add((URIRef(f"http://ex/s{i}"), URIRef("http://ex/p"), Literal(i)))
        dataset.add((URIRef(f"http://ex/s{i}"), URIRef("http://ex/p"), Literal("d")))
    data = dataset.serialize(format="jelly", encoding="utf-8")

    fast = parse_jelly_to_graph(io.BytesIO(data))
    slow = parse_jelly_to_graph(io.BytesIO(data), dataset_factory=CountingDataset)
    assert isinstance(fast, Dataset)
    assert isinstance(slow, CountingDataset)
    assert slow.added == len(dataset) == 3000
    assert set(fast.quads()) == set(slow.quads()) == set(dataset.quads())


def test_graph_loads_the_same_through_the_store_and_add() -> None:
    graph = Graph()
    for i in range(1500):
        graph.add((URIRef(f"http://ex/s{i}"), URIRef("http://ex/p"), Literal(i)))
    data = graph.serialize(format="jelly", encoding="utf-8")

    fast = parse_jelly_to_graph(io.BytesIO(data))
    slow = parse_jelly_to_graph(io.BytesIO(data), graph_factory=CountingGraph)
    assert isinstance(slow, CountingGraph)
    assert slow.added == 1500
    assert set(fast) == set(slow) == set(graph)
