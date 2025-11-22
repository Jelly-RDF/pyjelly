import pytest
from rdflib import BNode, Graph
from rdflib.graph import QuotedGraph

from pyjelly.integrations.rdflib.serialize import RDFLibJellySerializer, stream_frames
from pyjelly.serialize.streams import Stream


def test_rdflib_serializer_raises_quoted_graph() -> None:
    qg = QuotedGraph(store=Graph().store, identifier=BNode())
    with pytest.raises(NotImplementedError, match="N3 format is not supported"):
        RDFLibJellySerializer(qg)


def test_stream_frames_raise_unregistered() -> None:
    class DummyStream(Stream):
        def __init__(self) -> None:
            pass

    dummy = DummyStream()
    with pytest.raises(TypeError, match="invalid stream implementation"):
        list(stream_frames(dummy, Graph()))
