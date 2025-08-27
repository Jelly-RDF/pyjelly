from rdflib import Graph, Namespace, URIRef, Literal
from pyjelly.integrations.rdflib.serialize import SerializerOptions, StreamParameters

def test_rdflib_roundtrip_keeps_prefixes(tmp_path):
    g = Graph()
    EX = Namespace("http://example.org/")
    g.namespace_manager.bind("ex", EX)
    g.add((EX.alice, URIRef("http://xmlns.com/foaf/0.1/name"), Literal("Alice")))

    options = SerializerOptions(params=StreamParameters(
        generalized_statements=False,
        rdf_star=False,
        namespace_declarations=True,
    ))

    out = tmp_path / "g.jelly"
    g.serialize(out.as_posix(), format="jelly", options=options)

    g2 = Graph()
    g2.parse(out.as_posix(), format="jelly")

    ns = dict(g2.namespaces())
    assert "ex" in ns
    assert str(ns["ex"]) == "http://example.org/"
