# ruff: noqa: I001
from __future__ import annotations
from typing import cast
from collections.abc import Generator
from functools import singledispatch
from typing import Any, IO
from itertools import chain
from pyjelly.integrations.generic.generic_sink import (
    GenericStatementSink,
    Quad,
    Triple,
    DEFAULT_GRAPH_IDENTIFIER,
    IRI,
    BlankNode,
    Literal,
    Node,
)

from pyjelly import jelly
from pyjelly.serialize.encode import RowsAndTerm, Slot, TermEncoder
from pyjelly.serialize.ioutils import write_delimited
from pyjelly.serialize.streams import (
    GraphStream,
    QuadStream,
    SerializerOptions,
    Stream,
    TripleStream,
)  # ruff: enable

QUAD_ARITY = 4


class GenericSinkTermEncoder(TermEncoder):
    def encode_any(self, term: object, slot: Slot) -> RowsAndTerm:
        """
        Encode term based on its GenericSink object.

        Args:
            term (object): term to encode
            slot (Slot): its place in statement.

        Returns:
            RowsAndTerm: encoded extra rows and a jelly term to encode

        """
        if slot is Slot.graph and term == DEFAULT_GRAPH_IDENTIFIER:
            return self.encode_default_graph()

        if isinstance(term, IRI):
            return self.encode_iri(term._iri)

        if isinstance(term, Literal):
            return self.encode_literal(
                lex=term._lex,
                language=term._langtag,
                datatype=term._datatype,
            )

        if isinstance(term, BlankNode):
            return self.encode_bnode(term._identifier)

        if isinstance(term, Triple):
            return self.encode_quoted_triple(term)

        return super().encode_any(term, slot)  # error if not handled


def namespace_declarations(store: GenericStatementSink, stream: Stream) -> None:
    for prefix, namespace in store.namespaces:
        stream.namespace_declaration(name=prefix, iri=str(namespace))


@singledispatch
def stream_frames(
    stream: Stream,
    data: GenericStatementSink | Generator[Quad | Triple],  # noqa: ARG001
) -> Generator[jelly.RdfStreamFrame]:
    msg = f"invalid stream implementation {stream}"
    raise TypeError(msg)


@stream_frames.register(TripleStream)
def triples_stream_frames(
    stream: TripleStream,
    data: GenericStatementSink | Generator[Triple],
) -> Generator[jelly.RdfStreamFrame]:
    """
    Serialize a GenericStatementSink into frames using physical type triples stream.

    Args:
        stream (TripleStream): stream that specifies triples processing
        data (GenericStatementSink | Generator[Triple]):
            GenericStatementSink/Statements to serialize.

    Yields:
        Generator[jelly.RdfStreamFrame]: jelly frames.

    """
    stream.enroll()
    if (
        isinstance(data, GenericStatementSink)
        and stream.options.params.namespace_declarations
    ):
        namespace_declarations(data, stream)

    graphs = (data,)
    for graph in graphs:
        for terms in graph:
            if frame := stream.triple(terms):
                yield frame
        if frame := stream.flow.frame_from_graph():
            yield frame
    if stream.stream_types.flat and (frame := stream.flow.to_stream_frame()):
        yield frame


@stream_frames.register(QuadStream)
def quads_stream_frames(
    stream: QuadStream,
    data: GenericStatementSink | Generator[Quad],
) -> Generator[jelly.RdfStreamFrame]:
    """
    Serialize a GenericStatementSink into jelly frames using physical type quads stream.

    Args:
        stream (QuadStream): stream that specifies quads processing
        data (GenericStatementSink | Generator[Quad]): Dataset to serialize.

    Yields:
        Generator[jelly.RdfStreamFrame]: jelly frames

    """
    stream.enroll()
    if stream.options.params.namespace_declarations:
        namespace_declarations(data, stream)  # type: ignore[arg-type]

    iterator: Generator[Quad]
    if isinstance(data, GenericStatementSink):
        iterator = cast(Generator[Quad], data.store)
    else:
        iterator = data

    for terms in iterator:
        if frame := stream.quad(terms):
            yield frame
    if frame := stream.flow.frame_from_dataset():
        yield frame
    if stream.stream_types.flat and (frame := stream.flow.to_stream_frame()):
        yield frame


@stream_frames.register(GraphStream)
def graphs_stream_frames(
    stream: GraphStream,
    data: GenericStatementSink | Generator[Quad],
) -> Generator[jelly.RdfStreamFrame]:
    """
    Serialize a GenericStatementSink into jelly frames as a stream of graphs.

    Notes:
        If flow of DatasetsFrameFlow type, the whole dataset
        will be encoded into one frame.
        Graphs are generated from the GenericStatementSink by
        iterating over statements and yielding one new GenericStatementSink
        per a sequence of quads with the same g term.

    Args:
        stream (GraphStream): stream that specifies graphs processing
        data (GenericStatementSink | Generator[Quad]): Dataset to serialize.

    Yields:
        Generator[jelly.RdfStreamFrame]: jelly frames

    """
    stream.enroll()
    if stream.options.params.namespace_declarations:
        namespace_declarations(data, stream)  # type: ignore[arg-type]

    statements: Generator[Quad]
    if isinstance(data, GenericStatementSink):
        statements = cast(Generator[Quad], data.store)
        graphs = split_to_graphs(statements)
    elif iter(data):
        statements = data
        graphs = split_to_graphs(statements)

    for graph in graphs:
        yield from stream.graph(graph_id=graph.identifier, graph=graph)

    if frame := stream.flow.frame_from_dataset():
        yield frame
    if stream.stream_types.flat and (frame := stream.flow.to_stream_frame()):
        yield frame


def split_to_graphs(data: Generator[Quad]) -> Generator[GenericStatementSink]:
    """
    Split a generator of quads to graphs.

    Notes:
        New graph is generated by
        iterating over statements and yielding one new GenericStatementSink
        per a sequence of quads with the same g term.

    Args:
        data (Generator[Quad]): generator of quads

    Yields:
        Generator[GenericStatementSink]: generator of GenericStatementSinks,
        each having triples in store and identifier set.

    """
    current_g: Node | None = None
    current_sink: GenericStatementSink | None = None
    for statement in data:
        if current_g != statement.g:
            if current_sink is not None:
                yield current_sink

            current_g = statement.g
            current_sink = GenericStatementSink(identifier=current_g)

        assert current_sink is not None
        current_sink.add(Triple(statement.s, statement.p, statement.o))

    if current_sink is not None:
        yield current_sink


def guess_options(sink: GenericStatementSink) -> SerializerOptions:
    """Guess the serializer options based on the store type."""
    logical_type = (
        jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
        if sink.is_triples_sink
        else jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS
    )
    return SerializerOptions(logical_type=logical_type)


def guess_stream(options: SerializerOptions, sink: GenericStatementSink) -> Stream:
    """
    Return an appropriate stream implementation for the given options.

    Notes: if base(!) logical type is GRAPHS and sink.is_triples_sink is false,
        initializes TripleStream
    """
    stream_cls: type[Stream]
    if (
        options.logical_type % 10
    ) != jelly.LOGICAL_STREAM_TYPE_GRAPHS and not sink.is_triples_sink:
        stream_cls = QuadStream
    else:
        stream_cls = TripleStream
    if options is not None:
        lookup_preset = options.lookup_preset
    return stream_cls(
        encoder=GenericSinkTermEncoder(lookup_preset=lookup_preset),
        options=options,
    )


def grouped_stream_to_frames(
    sink_generator: Generator[GenericStatementSink],
    options: SerializerOptions | None = None,
) -> Generator[jelly.RdfStreamFrame]:
    """
    Transform multiple GenericStatementSinks into Jelly frames.

    Notes:
        One frame per GenericStatementSink.

    Note: options are guessed if not provided.

    Args:
        sink_generator (Generator[GenericStatementSink]): Generator of
            GenericStatementSink to transform.
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
    stream: Generator[GenericStatementSink],
    output_file: IO[bytes],
    **kwargs: Any,
) -> None:
    """
    Write stream of GenericStatementSink to a binary file.

    Args:
        stream (Generator[GenericStatementSink]): Generator of
            GenericStatementSink to serialize.
        output_file (IO[bytes]): output buffered writer.
        **kwargs (Any): options to pass to stream.

    """
    for frame in grouped_stream_to_frames(stream, **kwargs):
        write_delimited(frame, output_file)


def flat_stream_to_frames(
    statements: Generator[Triple | Quad],
    options: SerializerOptions | None = None,
) -> Generator[jelly.RdfStreamFrame]:
    """
    Serialize a stream of raw GenericStatementSink's triples or quads into Jelly frames.

    Args:
        statements (Generator[Triple | Quad]):
          s/p/o triples or s/p/o/g quads to serialize.
        options (SerializerOptions | None, optional):
            if omitted, guessed based on the first tuple.

    Yields:
        Generator[jelly.RdfStreamFrame]: generated frames.

    """
    first = next(statements, None)
    if first is None:
        return

    sink = GenericStatementSink()
    sink.add(first)
    if options is None:
        options = guess_options(sink)
    stream = guess_stream(options, sink)

    combined: Generator[Triple | Quad] | GenericStatementSink = (
        item for item in chain([first], statements)
    )

    yield from stream_frames(stream, combined)


def flat_stream_to_file(
    statements: Generator[Triple | Quad],
    output_file: IO[bytes],
    options: SerializerOptions | None = None,
) -> None:
    """
    Write Triple or Quad events to a binary file.

    Args:
        statements (Generator[Triple | Quad]): statements to serialize.
        output_file (IO[bytes]): output buffered writer.
        options (SerializerOptions | None, optional): stream options.

    """
    for frame in flat_stream_to_frames(statements, options):
        write_delimited(frame, output_file)
