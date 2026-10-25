import importlib
import io
from pathlib import Path

import pytest
from rdflib import Graph, Literal, Namespace, URIRef

from pyjelly.options import VERSION_1_1, VERSION_1_2

rdflib_serialize = importlib.import_module("pyjelly.integrations.rdflib.serialize")


@pytest.mark.parametrize("version", [VERSION_1_1, VERSION_1_2])
def test_rdflib_roundtrip_keeps_prefixes(tmp_path: Path, version: int) -> None:
    g = Graph()
    ex_ns = Namespace("http://example.org/")
    g.namespace_manager.bind("ex", ex_ns)
    g.add((ex_ns.alice, URIRef("http://xmlns.com/foaf/0.1/name"), Literal("Alice")))

    options = rdflib_serialize.SerializerOptions(
        params=rdflib_serialize.StreamParameters(
            generalized_statements=False,
            rdf_star=False,
            namespace_declarations=True,
            version=version,
        )
    )

    out = tmp_path / "g.jelly"
    g.serialize(out.as_posix(), format="jelly", options=options)

    g2 = Graph()
    g2.parse(out.as_posix(), format="jelly")

    ns = dict(g2.namespaces())
    assert "ex" in ns
    assert str(ns["ex"]) == "http://example.org/"


@pytest.mark.parametrize("validate_iris", [True, False])
def test_invalid_iris_are_read(
    caplog: pytest.LogCaptureFixture, *, validate_iris: bool
) -> None:
    g = Graph()
    g.add(
        (URIRef("http://example.org/a b"), URIRef("http://example.org/p"), Literal(1))
    )
    out = io.BytesIO()
    g.serialize(out, format="jelly")
    caplog.clear()

    g2 = Graph()
    g2.parse(data=out.getvalue(), format="jelly", validate_iris=validate_iris)

    assert set(g2) == set(g)
    # rdflib only warns about an invalid IRI, if it checks
    assert ("does not look like a valid URI" in caplog.text) == validate_iris
