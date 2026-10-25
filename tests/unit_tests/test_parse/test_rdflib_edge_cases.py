import pytest
import rdflib
from rdflib import XSD, Graph, Literal, URIRef
from rdflib.parser import InputSource

from pyjelly.integrations.rdflib.parse import RDFLibJellyParser


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
    assert str(literal) == lex
    assert literal.datatype == datatype
