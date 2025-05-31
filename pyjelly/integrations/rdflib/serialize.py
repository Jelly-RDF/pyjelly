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
from pyjelly.serialize.streams import (
    GraphStream,
    QuadStream,
    SerializerOptions,
    Stream,
    TripleStream,
)


class RDFLibTermEncoder(TermEncoder):
    def encode_any(self, term: object, slot: Slot) -> RowsAndTerm:
        if slot is Slot.graph and term == DATASET_DEFAULT_GRAPH_ID:
            return self.encode_default_graph()

        if isinstance(term, rdflib.URIRef):
            return self.encode_iri(term)

        if isinstance(term, rdflib.Literal):
            return self.encode_literal(
                lex=str(term),
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


@stream_frames.register(TripleStream)
def triples_stream_frames(
    stream: TripleStream,
    data: Graph | Dataset,
) -> Generator[jelly.RdfStreamFrame]:
    stream.enroll()
    if stream.options.params.namespace_declarations:
        namespace_declarations(data, stream)
    graphs = (data,) if not isinstance(data, Dataset) else data.graphs()
    for graph in graphs:
        for terms in graph:
            if frame := stream.triple(terms):
                yield frame
        if frame := stream.flow.frame_from_graph():
            yield frame
    if stream.stream_types.flat and (frame := stream.flow.to_stream_frame()):
        yield frame


@stream_frames.register
def quads_stream_frames(
    stream: QuadStream,
    data: Dataset,
) -> Generator[jelly.RdfStreamFrame]:
    assert isinstance(data, Dataset)
    stream.enroll()
    if stream.options.params.namespace_declarations:
        namespace_declarations(data, stream)
    for terms in data.quads():
        if frame := stream.quad(terms):
            yield frame
    if frame := stream.flow.frame_from_dataset():
        yield frame
    if stream.stream_types.flat and (frame := stream.flow.to_stream_frame()):
        yield frame


@stream_frames.register
def graphs_stream_frames(
    stream: GraphStream,
    data: Dataset,
) -> Generator[jelly.RdfStreamFrame]:
    assert isinstance(data, Dataset)
    stream.enroll()
    if stream.options.params.namespace_declarations:
        namespace_declarations(data, stream)
    for graph in data.graphs():
        yield from stream.graph(graph_id=graph.identifier, graph=graph)
    if frame := stream.flow.frame_from_dataset():
        yield frame
    if stream.stream_types.flat and (frame := stream.flow.to_stream_frame()):
        yield frame


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

    def guess_options(self) -> SerializerOptions:
        """
        Guess the serializer options based on the store type.

        >>> RDFLibJellySerializer(Graph()).guess_options().logical_type
        1
        >>> RDFLibJellySerializer(Dataset()).guess_options().logical_type
        2
        """
        logical_type = (
            jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS
            if isinstance(self.store, Dataset)
            else jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
        )
        return SerializerOptions(logical_type=logical_type)

    def guess_stream(self, options: SerializerOptions) -> Stream:
        """
        Return an appropriate stream implementation for the given options.

        >>> graph_ser = RDFLibJellySerializer(Graph())
        >>> ds_ser = RDFLibJellySerializer(Dataset())

        >>> type(graph_ser.guess_stream(graph_ser.guess_options()))
        <class 'pyjelly.serialize.streams.TripleStream'>
        >>> type(ds_ser.guess_stream(ds_ser.guess_options()))
        <class 'pyjelly.serialize.streams.QuadStream'>
        """
        stream_cls: type[Stream]
        if options.logical_type != jelly.LOGICAL_STREAM_TYPE_GRAPHS and isinstance(
            self.store, Dataset
        ):
            stream_cls = QuadStream
        else:
            stream_cls = TripleStream
        return stream_cls.for_rdflib(options=options)

    @override
    def serialize(  # type: ignore[override]
        self,
        out: IO[bytes],
        /,
        *,
        stream: Stream | None = None,
        options: SerializerOptions | None = None,
        **unused: Any,
    ) -> None:
        if options is None:
            options = self.guess_options()
        if stream is None:
            stream = self.guess_stream(options)
        write = write_delimited if stream.options.params.delimited else write_single
        for stream_frame in stream_frames(stream, self.store):
            write(stream_frame, out)
