# ruff: noqa: I001
from __future__ import annotations
from typing import cast
from collections.abc import Generator
from functools import singledispatch
from typing import Any, IO
from itertools import chain
from pyjelly.options import StreamParameters
from pyjelly.integrations.generic.generic_sink import (
    GenericStatementSink,
    Quad,
    Triple,
    DefaultGraph,
    IRI,
    BlankNode,
    Literal,
)

from pyjelly import jelly
from pyjelly.serialize.encode import EncodedTerm, TermEncoder
from pyjelly.serialize.ioutils import write_delimited
from pyjelly.serialize.streams import (
    QuadStream,
    SerializerOptions,
    Stream,
    TripleStream,
)

QUAD_ARITY = 4


class GenericSinkTermEncoder(TermEncoder):
    def same_term(self, a: object, b: object) -> bool:
        """
        Return whether two terms are equal, for repeated terms.

        IRIs, blank nodes and literals are compared by their strings, without a
        call to their __eq__.
        """
        if a is b:
            return True
        cls = type(a)
        if cls is not type(b):
            return False
        if cls is IRI:
            return cast(IRI, a)._iri == cast(IRI, b)._iri
        if cls is BlankNode:
            return cast(BlankNode, a)._id == cast(BlankNode, b)._id
        if cls is Literal:
            x = cast(Literal, a)
            y = cast(Literal, b)
            return (
                x._lex == y._lex
                and x._langtag == y._langtag
                and x._datatype == y._datatype
                and x._direction == y._direction
            )
        return a == b

    def encode_spo(self, term: object, out: EncodedTerm) -> None:
        """
        Encode term based on its GenericSink object.

        Args:
            term (object): term to encode
            out (EncodedTerm): where to put the encoded term.

        """
        if isinstance(term, IRI):
            self.encode_iri(term._iri, out)
            return

        if isinstance(term, Literal):
            self.encode_literal(
                lex=term._lex,
                language=term._langtag,
                datatype=term._datatype,
                direction=term._direction,
                out=out,
            )
            return

        if isinstance(term, BlankNode):
            self.encode_bnode(term._id, out)
            return

        if isinstance(term, Triple):
            self.encode_quoted_triple(term, out)
            return

        super().encode_spo(term, out)  # raises NotImplementedError

    def encode_graph(self, term: object, out: EncodedTerm) -> None:
        """
        Encode graph term based on its GenericSink object.

        Args:
            term (object): term to encode
            out (EncodedTerm): where to put the encoded graph name.

        """
        if term is DefaultGraph:
            self.encode_default_graph(out)
            return
        if isinstance(term, IRI):
            self.encode_iri(term._iri, out)
            return

        if isinstance(term, Literal):
            self.encode_literal(
                lex=term._lex,
                language=term._langtag,
                datatype=term._datatype,
                out=out,
            )
            return

        if isinstance(term, BlankNode):
            self.encode_bnode(term._id, out)
            return
        super().encode_graph(term, out)  # raises NotImplementedError


def namespace_declarations(
    store: GenericStatementSink, stream: Stream
) -> Generator[jelly.RdfStreamFrame]:
    for prefix, namespace in store.namespaces:
        if frame := stream.namespace_declaration(name=prefix, iri=namespace._iri):
            yield frame


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
        yield from namespace_declarations(data, stream)

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
        yield from namespace_declarations(data, stream)  # type: ignore[arg-type]

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


def guess_options(
    sink: GenericStatementSink, *, grouped: bool = False
) -> SerializerOptions:
    """
    Guess the serializer options based on the store type.

    For a stream of graphs or datasets (`grouped`), each is an RDF Message in
    Jelly-RDF 1.2, and one frame in Jelly-RDF 1.1.
    """
    if sink.is_triples_sink:
        logical_type = (
            jelly.LOGICAL_STREAM_TYPE_GRAPHS
            if grouped
            else jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
        )
    else:
        logical_type = (
            jelly.LOGICAL_STREAM_TYPE_DATASETS
            if grouped
            else jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS
        )
    # Generic sink supports both RDF-star and generalized statements by default
    # as it can handle any term types including quoted triples and generalized RDF terms
    params = StreamParameters(generalized_statements=True, rdf_star=True)
    return SerializerOptions(logical_type=logical_type, params=params)


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
                options = guess_options(sink, grouped=True)
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
