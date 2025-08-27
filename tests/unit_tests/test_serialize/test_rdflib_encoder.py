import pytest

from pyjelly import jelly
from pyjelly.integrations.rdflib.serialize import RDFLibTermEncoder
from pyjelly.options import LookupPreset

from rdflib import URIRef


def test_graph_not_implemented() -> None:
    enc = RDFLibTermEncoder(lookup_preset=LookupPreset())
    with pytest.raises(NotImplementedError, match="unsupported term type"):
        enc.encode_graph(
            (URIRef("http://ex/s1"), URIRef("http://ex/p1"), URIRef("http://ex/o1")),
            jelly.RdfQuad(),
        )
