# ruff: noqa: I001
from __future__ import annotations

from collections.abc import Generator, Iterable
from functools import singledispatch
from typing import Any, IO, Optional
from typing_extensions import override

import rdflib
from rdflib.graph import (
    DATASET_DEFAULT_GRAPH_ID,
    Dataset,
    Graph,
    QuotedGraph,
    Namespace,
    Literal,
)
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

from typing import Callable, Optional, Iterable

import random
from pyjelly.options import StreamParameters


class RDFLibTermEncoder(TermEncoder):
    def encode_any(self, term: object, slot: Slot) -> RowsAndTerm:
        """
        Encode term based on its RDFLib object.

        Args:
            term (object): term to encode
            slot (Slot): its place in statement.

        Returns:
            RowsAndTerm: encoded extra rows and a jelly term to encode

        """
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
def stream_frames(
    stream: Stream,
    data: Graph,  # noqa: ARG001
) -> Generator[jelly.RdfStreamFrame]:
def stream_frames(
    stream: Stream,
    data: Graph,  # noqa: ARG001
) -> Generator[jelly.RdfStreamFrame]:
    msg = f"invalid stream implementation {stream}"
    raise TypeError(msg)


@stream_frames.register(TripleStream)
def triples_stream_frames(
    stream: TripleStream,
    data: Graph | Dataset,
) -> Generator[jelly.RdfStreamFrame]:
    """
    Serialize a Graph/Dataset into jelly frames.

    Args:
        stream (TripleStream): stream that specifies triples processing
        data (Graph | Dataset): Graph/Dataset to serialize.

    Notes:
        if Dataset is given, its graphs are unpacked and iterated over
        if flow is GraphsFrameFlow, emits a frame per graph.

    Yields:
        Generator[jelly.RdfStreamFrame]: jelly frames.

    """
    stream.enroll()
    if stream.options.params.namespace_declarations:
        namespace_declarations(data, stream)
    graphs = (data,) if not isinstance(data, Dataset) else data.graphs()
    for graph in graphs:
        for terms in graph:
            if frame := stream.triple(terms):
                yield frame
        # this part turns each graph to a frame for graphs logical type
        if frame := stream.flow.frame_from_graph():
            yield frame
    if stream.stream_types.flat and (frame := stream.flow.to_stream_frame()):
        yield frame


@stream_frames.register
def quads_stream_frames(
    stream: QuadStream,
    data: Dataset,
) -> Generator[jelly.RdfStreamFrame]:
    """
    Serialize a Dataset into jelly frames.

    Notes:
        Emits one frame per dataset if flow is of DatasetsFrameFlow.

    Args:
        stream (QuadStream): stream that specifies quads processing
        data (Dataset): Dataset to serialize.

    Yields:
        Generator[jelly.RdfStreamFrame]: jelly frames

    """
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
    """
    Serialize a Dataset into jelly frames as a stream of graphs.

    Notes:
        If flow of DatasetsFrameFlow type, the whole dataset
        will be encoded into one frame.

    Args:
        stream (GraphStream): stream that specifies graphs processing
        data (Dataset): Dataset to serialize.

    Yields:
        Generator[jelly.RdfStreamFrame]: jelly frames

    """
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


def guess_options(sink: Graph | Dataset) -> SerializerOptions:
    """
    Guess the serializer options based on the store type.

    >>> guess_options(Graph()).logical_type
    1
    >>> guess_options(Dataset()).logical_type
    2
    """
    logical_type = (
        jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS
        if isinstance(sink, Dataset)
        else jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
    )
    return SerializerOptions(logical_type=logical_type)


def guess_stream(options: SerializerOptions, sink: Graph | Dataset) -> Stream:
    """
    Return an appropriate stream implementation for the given options.

    Notes: if base(!) logical type is GRAPHS and Dataset is given,
        initializes TripleStream

    >>> graph_ser = RDFLibJellySerializer(Graph())
    >>> ds_ser = RDFLibJellySerializer(Dataset())

    >>> type(guess_stream(guess_options(graph_ser.store), graph_ser.store))
    <class 'pyjelly.serialize.streams.TripleStream'>
    >>> type(guess_stream(guess_options(ds_ser.store), ds_ser.store))
    <class 'pyjelly.serialize.streams.QuadStream'>
    """
    stream_cls: type[Stream]
    if (options.logical_type % 10) != jelly.LOGICAL_STREAM_TYPE_GRAPHS and isinstance(
        sink, Dataset
    ):
        stream_cls = QuadStream
    else:
        stream_cls = TripleStream
    return stream_cls.for_rdflib(options=options)


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
        stream: Stream | None = None,
        options: SerializerOptions | None = None,
        **unused: Any,
    ) -> None:
        """
        Serialize self.store content to Jelly format.

        Args:
            out (IO[bytes]): output buffered writer
            stream (Stream | None, optional): Jelly stream object. Defaults to None.
            options (SerializerOptions | None, optional): Serializer options
                if defined beforehand, e.g., read from a separate file.
                Defaults to None.
            **unused(Any): unused args for RDFLib serialize

        """
        if options is None:
            options = guess_options(self.store)
        if stream is None:
            stream = guess_stream(options, self.store)
        write = write_delimited if stream.options.params.delimited else write_single
        for stream_frame in stream_frames(stream, self.store):
            write(stream_frame, out)


def grouped_stream_to_frames(
    sink_generator: Generator[Graph] | Generator[Dataset],
    options: SerializerOptions | None = None,
) -> Generator[jelly.RdfStreamFrame]:
    """
    Transform Graphs/Datasets into Jelly frames, one frame per Graph/Dataset.

    Note: options are guessed if not provided.

    Args:
        sink_generator (Generator[Graph] | Generator[Dataset]): Generator of
            Graphs/Dataset to transform.
        options (SerializerOptions | None, optional): stream options to use.
            Options are guessed based on the sink store type. Defaults to None.

    Yields:
        Generator[jelly.RdfStreamFrame]: produced Jelly frames

    """
    stream = None
    for sink in sink_generator:
        if not stream:
            if options is None:
                options = guess_options(sink)
            stream = guess_stream(options, sink)
        yield from stream_frames(stream, sink)


def grouped_stream_to_file(
    stream: Generator[Graph] | Generator[Dataset],
    output_file: IO[bytes],
    **kwargs: Any,
) -> None:
    """
    Write stream of Graphs/Datasets to a binary file.

    Args:
        stream (Generator[Graph] | Generator[Dataset]): Generator of
            Graphs/Dataset to transform.
        output_file (IO[bytes]): opened output file.
        **kwargs (Any): options to pass to stream.

    """
    for frame in grouped_stream_to_frames(stream, **kwargs):
        write_delimited(frame, output_file)


def flat_stream_to_frames(
    statements: Generator[Triple | Quad],
    options: Optional[SerializerOptions] = None,
) -> Generator[jelly.RdfStreamFrame]:
    """
    Transform Triple or Quad events into Jelly flat mode frames.

    Args:
        statements (Generator[Triple | Quad]): statements to serialize.
        options (SerializerOptions | None, optional): stream options. If omitted, guessed from first statement.

    Yields:
        Generator[jelly.RdfStreamFrame]: produced jelly frames.
    """
    stream = None
    for stmt in statements:
        if stream is None:
            if options is None:
                options = guess_options(stmt)  # type: ignore[arg-type]
            stream = guess_stream(options, stmt)  # type: ignore[arg-type]

        sink = Graph()
        if isinstance(stmt, Triple):
            sink.add((stmt.s, stmt.p, stmt.o))
        elif isinstance(stmt, Quad):
            sink.add((stmt.s, stmt.p, stmt.o, stmt.g))  # type: ignore[arg-type]
        yield from stream_frames(stream, sink)

    if stream and (end := stream.flow.to_stream_frame()):
        yield end


def flat_stream_to_file(
    statements: Generator[Triple | Quad],
    output_file: IO[bytes],
    options: SerializerOptions | None = None,
) -> None:
    """
    Write Triple or Quad events to a binary file in Jelly flat format.

    Args:
        statements (Generator[Triple | Quad]): events/statements to serialize.
        output_file (IO[bytes]): open binary file handle.
        options (SerializerOptions | None, optional): stream options to use. If omitted, inferred from the first statement.
    """
    for frame in flat_stream_to_frames(statements, options):
        write_delimited(frame, output_file)
    output_file.flush()
