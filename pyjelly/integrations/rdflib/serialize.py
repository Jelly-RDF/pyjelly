# ruff: noqa: I001
from __future__ import annotations
from collections.abc import Callable, Generator
from functools import singledispatch
from typing import Any, IO
from typing_extensions import override
from itertools import chain
from pyjelly.integrations.rdflib.parse import Quad, Triple
from pyjelly.integrations.rdflib.walk import Tree, gspo_trees, spo_tree, spo_triples
from pyjelly.options import StreamParameters

import rdflib
from rdflib import Graph
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID, Dataset, QuotedGraph
from rdflib.serializer import Serializer as RDFLibSerializer

from mypy_extensions import mypyc_attr

from pyjelly import jelly
from pyjelly.serialize.encode import Rows, Slot, TermEncoder, Statement, HasGraph
from pyjelly.serialize.ioutils import write_delimited, write_single
from pyjelly.serialize.streams import (
    GraphStream,
    QuadStream,
    SerializerOptions,
    Stream,
    TripleStream,
)

QUAD_ARITY = 4


@mypyc_attr(allow_interpreted_subclasses=True)
class RDFLibTermEncoder(TermEncoder):
    def encode_spo(self, term: object, slot: Slot, statement: Statement) -> Rows:
        """
        Encode s/p/o term based on its RDFLib object.

        Args:
            term (object): term to encode
            slot (Slot): its place in statement.
            statement (Statement): Triple/Quad message to fill with s/p/o terms.

        Returns:
            Rows: encoded extra rows

        """
        if isinstance(term, rdflib.URIRef):
            iri = self.get_iri_field(statement, slot)
            return self.encode_iri(term, iri)

        if isinstance(term, rdflib.Literal):
            literal = self.get_literal_field(statement, slot)
            return self.encode_literal(
                lex=str(term),
                language=term.language,
                # `datatype` is cast to `str` explicitly because
                # `URIRef.__eq__` overrides `str.__eq__` in an incompatible manner
                datatype=term.datatype and str(term.datatype),
                literal=literal,
            )

        if isinstance(term, rdflib.BNode):
            self.set_bnode_field(statement, slot, str(term))
            return ()

        return super().encode_spo(term, slot, statement)  # error if not handled

    def encode_graph(self, term: object, statement: HasGraph) -> Rows:
        """
        Encode graph name term based on its RDFLib object.

        Args:
            term (object): term to encode
            statement (HasGraph): Quad/GraphStart message to fill g_{} in.

        Returns:
            Rows: encoded extra rows

        """
        if term == DATASET_DEFAULT_GRAPH_ID:
            return self.encode_default_graph(statement.g_default_graph)

        if isinstance(term, rdflib.URIRef):
            return self.encode_iri(term, statement.g_iri)

        if isinstance(term, rdflib.BNode):
            statement.g_bnode = str(term)
            return ()
        return super().encode_graph(term, statement)  # error if not handled


def namespace_declarations(store: Graph, stream: Stream) -> None:
    for prefix, namespace in store.namespaces():
        stream.namespace_declaration(name=prefix, iri=namespace)


@singledispatch
def stream_frames(
    stream: Stream,
    data: Graph | Generator[Quad | Triple],  # noqa: ARG001
) -> Generator[jelly.RdfStreamFrame]:
    msg = f"invalid stream implementation {stream}"
    raise TypeError(msg)


def _differs(previous: object, term: object) -> bool:
    return previous is not term and previous != term


def _emit_tree(
    stream: Stream,
    tree: Tree,
    emit: Callable[[], jelly.RdfStreamFrame | None],
) -> Generator[jelly.RdfStreamFrame]:
    """
    Emit the statements of a subject -> predicate -> objects tree.

    Uses the stream's cursor API, so the stream doesn't compare terms. In a tree,
    each subject differs from the one before it, and so do the predicates of one
    subject and the objects of one predicate. Only the first term of a group can
    be equal to the term before it (e.g., rdf:type as the first predicate of
    every subject), so only those are compared here, once per group.

    Args:
        stream (Stream): stream to write to
        tree (Tree): statements to write
        emit (Callable): the stream's emit_triple or emit_quad

    Yields:
        Generator[jelly.RdfStreamFrame]: jelly frames

    """
    terms = stream.repeated_terms
    subjects = iter(tree.items())
    for s, predicates in subjects:
        # The first subject of the tree
        if _differs(terms[0], s):
            stream.set_subject(s)
        yield from _emit_predicates(stream, predicates, emit)
        break
    for s, predicates in subjects:
        stream.set_subject(s)
        yield from _emit_predicates(stream, predicates, emit)


def _emit_predicates(
    stream: Stream,
    predicates: Any,
    emit: Callable[[], jelly.RdfStreamFrame | None],
) -> Generator[jelly.RdfStreamFrame]:
    terms = stream.repeated_terms
    first = True
    for p, objects in predicates.items():
        if not first or _differs(terms[1], p):
            stream.set_predicate(p)
        first = False
        # The first object of a predicate is rarely equal to the object before
        # it, and most predicates have only one object: comparing would cost
        # more than it saves
        for o in objects:
            stream.set_object(o)
            if frame := emit():
                yield frame


@stream_frames.register(TripleStream)
def triples_stream_frames(
    stream: TripleStream,
    data: Graph | Dataset | Generator[Triple],
) -> Generator[jelly.RdfStreamFrame]:
    """
    Serialize a Graph/Dataset into jelly frames.

    Args:
        stream (TripleStream): stream that specifies triples processing
        data (Graph | Dataset | Generator[Triple]):
            Graph/Dataset/Statements to serialize.

    Notes:
        if Dataset is given, its graphs are unpacked and iterated over
        if flow is GraphsFrameFlow, emits a frame per graph.

    Yields:
        Generator[jelly.RdfStreamFrame]: jelly frames.

    """
    stream.enroll()
    if isinstance(data, Graph) and stream.options.params.namespace_declarations:
        namespace_declarations(data, stream)

    graphs = (data,) if not isinstance(data, Dataset) else data.graphs()
    for graph in graphs:
        tree = spo_tree(graph) if isinstance(graph, Graph) else None
        if tree is not None:
            # An in-memory graph, grouped by subject and predicate
            yield from _emit_tree(stream, tree, stream.emit_triple)
        else:
            # Any other store, or a generator of triples, in its own order
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
    data: Dataset | Generator[Quad],
) -> Generator[jelly.RdfStreamFrame]:
    """
    Serialize a Dataset into jelly frames.

    Notes:
        Emits one frame per dataset if flow is of DatasetsFrameFlow.

    Args:
        stream (QuadStream): stream that specifies quads processing
        data (Dataset | Generator[Quad]): Dataset to serialize.

    Yields:
        Generator[jelly.RdfStreamFrame]: jelly frames

    """
    stream.enroll()
    if stream.options.params.namespace_declarations:
        namespace_declarations(data, stream)  # type: ignore[arg-type]
    trees = gspo_trees(data) if isinstance(data, Dataset) else None
    if trees is not None:
        # An in-memory dataset, grouped by graph, subject and predicate
        for g, tree in trees:
            if _differs(stream.repeated_terms[3], g):
                stream.set_graph(g)
            yield from _emit_tree(stream, tree, stream.emit_quad)
    else:
        # Any other store, or a generator of quads, in its own order
        quads = (
            data.quads((None, None, None, None)) if isinstance(data, Dataset) else data
        )
        for terms in quads:
            if frame := stream.quad(terms):
                yield frame
    if frame := stream.flow.frame_from_dataset():
        yield frame
    if stream.stream_types.flat and (frame := stream.flow.to_stream_frame()):
        yield frame


@stream_frames.register(GraphStream)
def graphs_stream_frames(
    stream: GraphStream,
    data: Dataset | Generator[Quad],
) -> Generator[jelly.RdfStreamFrame]:
    """
    Serialize a Dataset into jelly frames as a stream of graphs.

    Notes:
        If flow of DatasetsFrameFlow type, the whole dataset
        will be encoded into one frame.

    Args:
        stream (GraphStream): stream that specifies graphs processing
        data (Dataset | Generator[Quad]): Dataset to serialize.

    Yields:
        Generator[jelly.RdfStreamFrame]: jelly frames

    """
    stream.enroll()
    if stream.options.params.namespace_declarations:
        namespace_declarations(data, stream)  # type: ignore[arg-type]

    if isinstance(data, Dataset):
        graphs = data.graphs()
    else:
        ds = Dataset()
        for quad in data:
            ctx = ds.get_context(quad.g)
            ctx.add((quad.s, quad.p, quad.o))
        graphs = ds.graphs()

    for graph in graphs:
        yield from stream.graph(graph_id=graph.identifier, graph=spo_triples(graph))

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
    # RDFLib doesn't support RDF-star and generalized statements by default
    # as it requires specific handling for quoted triples and non-standard RDF terms
    params = StreamParameters(generalized_statements=False, rdf_star=False)
    return SerializerOptions(logical_type=logical_type, params=params)


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
    Serialize a stream of raw triples or quads into Jelly frames.

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

    sink = Dataset() if len(first) == QUAD_ARITY else Graph()
    if options is None:
        options = guess_options(sink)
    stream = guess_stream(options, sink)

    combined: Generator[Triple | Quad] | Graph = (
        item for item in chain([first], statements)
    )

    yield from stream_frames(stream, combined)


def flat_stream_to_file(
    statements: Generator[Triple | Quad],
    output_file: IO[bytes],
    options: SerializerOptions | None = None,
) -> None:
    """
    Write Triple or Quad events to a binary file in Jelly flat format.

    Args:
        statements (Generator[Triple | Quad]): statements to serialize.
        output_file (IO[bytes]): output buffered writer.
        options (SerializerOptions | None, optional): stream options.

    """
    for frame in flat_stream_to_frames(statements, options):
        write_delimited(frame, output_file)
