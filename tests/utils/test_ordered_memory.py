import pytest
from rdflib import BNode, Literal, URIRef

from tests.utils.ordered_memory import OrderedMemory, Triple


@pytest.mark.parametrize(
    "triples",
    [
        [
            (
                URIRef("http://foo.com/a"),
                URIRef("http://foo.bar/b"),
                URIRef("http://foo.biz/c"),
            ),
            (
                URIRef("http://bar.spam/x"),
                URIRef("http://foo.eggs/y"),
                URIRef("http://ham.spam/z"),
            ),
        ],
        [
            (
                BNode("a"),
                URIRef("http://example.org/p"),
                Literal("test"),
            ),
            (
                URIRef("http://example.org/s"),
                URIRef("http://example.org/p"),
                Literal(42),
            ),
        ],
        [
            (
                URIRef("http://x"),
                URIRef("http://y"),
                Literal("hello", lang="en"),
            ),
            (
                URIRef("http://x"),
                URIRef("http://y"),
                Literal(
                    "3.14", datatype=URIRef("http://www.w3.org/2001/XMLSchema#decimal")
                ),
            ),
        ],
    ],
)
def test_ordered_memory(triples: list[Triple]) -> None:
    store = OrderedMemory()
    for triple in triples:
        store.add(triple)
    output_triples = [t for t, _ in store.triples()]
    assert output_triples == triples
