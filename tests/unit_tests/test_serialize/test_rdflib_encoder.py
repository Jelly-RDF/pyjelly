import pytest
from rdflib import XSD, Literal, URIRef

from pyjelly import jelly
from pyjelly.integrations.rdflib.encode import RDFLibTermEncoder
from pyjelly.options import LookupPreset
from pyjelly.serialize.encode import Slot


def test_graph_not_implemented() -> None:
    enc = RDFLibTermEncoder(lookup_preset=LookupPreset())
    with pytest.raises(NotImplementedError, match="unsupported term type"):
        enc.encode_graph(
            (URIRef("http://ex/s1"), URIRef("http://ex/p1"), URIRef("http://ex/o1")),
            jelly.RdfQuad(),
        )


def test_spo_not_implemented() -> None:
    enc = RDFLibTermEncoder(lookup_preset=LookupPreset())
    with pytest.raises(NotImplementedError, match="unsupported term type"):
        enc.encode_spo(
            (URIRef("http://ex/s1"), URIRef("http://ex/p1"), URIRef("http://ex/o1")),
            Slot.subject,
            jelly.RdfQuad(),
        )


class MyURIRef(URIRef):
    __slots__ = ()


class MyLiteral(Literal):
    __slots__ = ()


@pytest.mark.parametrize(
    ("term", "subclass_term"),
    [
        (URIRef("http://ex/s1"), MyURIRef("http://ex/s1")),
        (Literal("x", lang="en"), MyLiteral("x", lang="en")),
        (Literal("1", datatype=XSD.integer), MyLiteral("1", datatype=XSD.integer)),
    ],
)
def test_subclasses_encode_like_their_base_class(
    term: URIRef | Literal, subclass_term: URIRef | Literal
) -> None:
    # Exact classes take a fast path. Subclasses must still be encoded the same
    triples = []
    for t in (term, subclass_term):
        enc = RDFLibTermEncoder(lookup_preset=LookupPreset())
        triple = jelly.RdfTriple()
        rows = enc.encode_spo(t, Slot.object, triple)
        triples.append((list(rows), triple))
    assert triples[0] == triples[1]
