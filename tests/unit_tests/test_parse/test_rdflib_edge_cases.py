import pytest
from rdflib import Graph
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
