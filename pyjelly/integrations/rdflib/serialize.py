from __future__ import annotations

from collections.abc import Generator
from functools import singledispatch
from typing import IO, Any
from typing_extensions import override

import rdflib
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID, Dataset, Graph, QuotedGraph
from rdflib.serializer import Serializer as RDFLibSerializer

from pyjelly import jelly
from pyjelly.serialize.encode import RowsAndTerm, Slot, TermEncoder
from pyjelly.serialize.ioutils import write_delimited, write_single
from pyjelly.serialize.streams import GraphStream, QuadStream, Stream, TripleStream


class RDFLibTermEncoder(TermEncoder):
    def encode_any(self, term: object, slot: Slot) -> RowsAndTerm:
        if slot is Slot.graph and term == DATASET_DEFAULT_GRAPH_ID:
            return self.encode_default_graph()

        if isinstance(term, rdflib.URIRef):
            return self.encode_iri(term)

        if isinstance(term, rdflib.Literal):
            return self.encode_literal(
                lex=term,
                language=term.language,
                # `datatype` is cast to `str` explicitly because
                # `URIRef.__eq__` overrides `str.__eq__` in an incompatible manner
                datatype=term.datatype and str(term.datatype),
            )

        if isinstance(term, rdflib.BNode):
            return self.encode_bnode(str(term))

        return super().encode_any(term, slot)  # error if not handled


def namespace_declarations(store: Graph, stream: Stream) -> None:
    for prefix, namespace in store.namespaces():
        stream.namespace_declaration(name=prefix, iri=namespace)


@singledispatch
def stream_frames(stream: Stream, data: Graph) -> Generator[jelly.RdfStreamFrame]:  # noqa: ARG001
    msg = f"invalid stream implementation {stream}"
    raise TypeError(msg)


@stream_frames.register
def triples_stream(
    stream: TripleStream,
    data: Graph,
) -> Generator[jelly.RdfStreamFrame]:
    assert not isinstance(data, Dataset)
    stream.enroll()
    if stream.options.namespace_declarations:
        namespace_declarations(data, stream)
    for terms in data:
        if frame := stream.triple(terms):
            yield frame
    if frame := stream.flow.to_stream_frame():
        yield frame


@stream_frames.register
def quads_stream(stream: QuadStream, data: Graph) -> Generator[jelly.RdfStreamFrame]:
    assert isinstance(data, Dataset)
    stream.enroll()
    if stream.options.namespace_declarations:
        namespace_declarations(data, stream)
    for terms in data.quads():
        if frame := stream.quad(terms):
            yield frame
    if frame := stream.flow.to_stream_frame():
        yield frame


@stream_frames.register
def graphs_stream(stream: GraphStream, data: Graph) -> Generator[jelly.RdfStreamFrame]:
    assert isinstance(data, Dataset)
    stream.enroll()
    if stream.options.namespace_declarations:
        namespace_declarations(data, stream)
    for graph in data.graphs():
        yield from stream.graph(graph_id=graph.identifier, graph=graph)
    if last := stream.flow.to_stream_frame():
        yield last


class RDFLibJellySerializer(RDFLibSerializer):
    """
    RDFLib serializer for writing graphs in Jelly RDF stream format.

    Handles streaming RDF terms into Jelly frames using internal encoders.
    Supports only graphs and datasets (not quoted graphs).

    """

    def __init__(self, store: Graph) -> None:
        if isinstance(store, QuotedGraph):
            msg = "N3 format is not supported"
            raise NotImplementedError(msg)
        super().__init__(store)

    @override
    def serialize(  # type: ignore[override]
        self,
        out: IO[bytes],
        /,
        *,
        stream: Stream,
        **unused: Any,
    ) -> None:
        write = write_delimited if stream.options.delimited else write_single
        for stream_frame in stream_frames(stream, self.store):
            write(stream_frame, out)
