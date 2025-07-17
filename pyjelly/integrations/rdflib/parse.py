from __future__ import annotations

from collections.abc import Generator, Iterable
from itertools import chain
from typing import IO, Any, Callable, Union
from typing_extensions import Never, Self, override

import rdflib
from rdflib import Node
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID, Dataset, Graph
from rdflib.parser import InputSource
from rdflib.parser import Parser as RDFLibParser

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.options import StreamTypes
from pyjelly.parse.decode import Adapter, Decoder, ParserOptions
from pyjelly.parse.ioutils import get_options_and_frames

GraphName = Union[rdflib.URIRef, rdflib.BNode, str]


class Triple(tuple[Node, Node, Node]):
    """
    Describe RDFLib triple.

    Args:
        tuple (Node, Node, Node): s/p/o tuple of RDFLib Nodes.

    Returns:
        Triple: triple as tuple.

    """

    __slots__ = ()

    def __new__(cls, s: Node, p: Node, o: Node) -> Self:
        return tuple.__new__(cls, (s, p, o))

    @property
    def s(self) -> Node:
        return self[0]

    @property
    def p(self) -> Node:
        return self[1]

    @property
    def o(self) -> Node:
        return self[2]


class Quad(tuple[Node, Node, Node, GraphName]):
    """
    Describe RDFLib quad.

    Args:
        tuple (Node, Node, Node, GraphName):
            s/p/o/g as a tuple of RDFLib nodes and a GraphName,

    Returns:
        Quad: quad as tuple.

    """

    __slots__ = ()

    def __new__(cls, s: Node, p: Node, o: Node, g: GraphName) -> Self:
        return tuple.__new__(cls, (s, p, o, g))

    @property
    def s(self) -> Node:
        return self[0]

    @property
    def p(self) -> Node:
        return self[1]

    @property
    def o(self) -> Node:
        return self[2]

    @property
    def g(self) -> GraphName:
        return self[3]


Statement = Union[Triple, Quad]


class Prefix(tuple[str, rdflib.URIRef]):
    """
    Describe RDF Prefix(i.e, namespace declaration).

    Args:
        tuple (str, rdflib.URIRef): expects prefix as a string,
            and full namespace URI as Rdflib.URIRef.

    Returns:
        Prefix: prefix as tuple(prefix, iri).

    """

    __slots__ = ()

    def __new__(cls, prefix: str, iri: rdflib.URIRef) -> Self:
        return tuple.__new__(cls, (prefix, iri))

    @property
    def prefix(self) -> str:
        return self[0]

    @property
    def iri(self) -> rdflib.URIRef:
        return self[1]


class RDFLibAdapter(Adapter):
    """
    RDFLib adapter class, is extended by triples and quads implementations.

    Args:
        Adapter (): abstract adapter class

    """

    @override
    def iri(self, iri: str) -> rdflib.URIRef:
        return rdflib.URIRef(iri)

    @override
    def bnode(self, bnode: str) -> rdflib.BNode:
        return rdflib.BNode(bnode)

    @override
    def default_graph(self) -> rdflib.URIRef:
        return DATASET_DEFAULT_GRAPH_ID

    @override
    def literal(
        self,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
    ) -> rdflib.Literal:
        return rdflib.Literal(lex, lang=language, datatype=datatype)

    @override
    def namespace_declaration(self, name: str, iri: str) -> Prefix:
        return Prefix(name, self.iri(iri))


def _adapter_missing(feature: str, *, stream_types: StreamTypes) -> Never:
    """
    Raise error if functionality is missing in adapter.

    Args:
        feature (str): function which is not implemented
        stream_types (StreamTypes): what combination of physical/logical types
            triggered the error

    Raises:
        NotImplementedError: raises error with message with missing functionality
            and types encountered

    Returns:
        Never: only raises errors

    """
    physical_type_name = jelly.PhysicalStreamType.Name(stream_types.physical_type)
    logical_type_name = jelly.LogicalStreamType.Name(stream_types.logical_type)
    msg = (
        f"adapter with {physical_type_name} and {logical_type_name} "
        f"does not implement {feature}"
    )
    raise NotImplementedError(msg)


class RDFLibTriplesAdapter(RDFLibAdapter):
    """
    Triples adapter RDFLib implementation.

    Notes: returns triple/namespace declaration as soon as receives them.
    """

    def __init__(
        self,
        options: ParserOptions,
    ) -> None:
        super().__init__(options=options)

    @override
    def triple(self, terms: Iterable[Any]) -> Triple:
        return Triple(*terms)


class RDFLibQuadsBaseAdapter(RDFLibAdapter):
    def __init__(self, options: ParserOptions) -> None:
        super().__init__(options=options)


class RDFLibQuadsAdapter(RDFLibQuadsBaseAdapter):
    """
    Extended RDFLib adapter for the QUADS physical type.

    Args:
        RDFLibQuadsBaseAdapter (RDFLibAdapter): base quads adapter
            (shared with graphs physical type)

    """

    @override
    def quad(self, terms: Iterable[Any]) -> Quad:
        return Quad(*terms)


class RDFLibGraphsAdapter(RDFLibQuadsBaseAdapter):
    """
    Extension of RDFLibQuadsBaseAdapter for the GRAPHS physical type.

    Notes: introduces graph start/end, checks if graph exists.

    Args:
        RDFLibQuadsBaseAdapter (RDFLibAdapter): base adapter for quads management.

    Raises:
        JellyConformanceError: if no graph_start was encountered

    """

    _graph_id: str | None

    def __init__(
        self,
        options: ParserOptions,
    ) -> None:
        super().__init__(options=options)
        self._graph_id = None

    @property
    def graph(self) -> None:
        if self._graph_id is None:
            msg = "new graph was not started"
            raise JellyConformanceError(msg)

    @override
    def graph_start(self, graph_id: str) -> None:
        self._graph_id = graph_id

    @override
    def triple(self, terms: Iterable[Any]) -> Quad:
        return Quad(*chain(terms, [self._graph_id]))

    @override
    def graph_end(self) -> None:
        self._graph_id = None


def parse_triples_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
) -> Generator[Iterable[Triple | Prefix]]:
    """
    Parse flat triple stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options

    Yields:
        Generator[Iterable[Triple | Prefix]]:
            Generator of iterables of Triple or Prefix objects,
            one iterable per frame.

    """
    adapter = RDFLibTriplesAdapter(options)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        yield decoder.iter_rows(frame)
    return


def parse_quads_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
) -> Generator[Iterable[Quad | Prefix]]:
    """
    Parse flat quads stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options

    Yields:
        Generator[Iterable[Quad | Prefix]]:
            Generator of iterables of Quad or Prefix objects,
            one iterable per frame.

    """
    adapter_class: type[RDFLibQuadsBaseAdapter]
    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_QUADS:
        adapter_class = RDFLibQuadsAdapter
    else:
        adapter_class = RDFLibGraphsAdapter
    adapter = adapter_class(options=options)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        yield decoder.iter_rows(frame)
    return


def parse_jelly_grouped(
    inp: IO[bytes],
    graph_factory: Callable[[], Graph] = lambda: Graph(),
    dataset_factory: Callable[[], Dataset] = lambda: Dataset(),
) -> Generator[Graph] | Generator[Dataset]:
    """
    Take jelly file and return generators based on the detected physical type.

    Yields one graph/dataset per frame.

    Args:
        inp (IO[bytes]): input jelly buffered binary stream
        graph_factory (Callable): lambda to construct a Graph.
            By default creates an empty in-memory Graph,
            but you can pass something else here.
        dataset_factory (Callable): lambda to construct a Dataset.
            By default creates an empty in-memory Dataset,
            but you can pass something else here.

    Raises:
        NotImplementedError: is raised if a physical type is not implemented

    Yields:
        Generator[Graph] | Generator[Dataset]:
            returns generators for graphs/datasets based on the type of input

    """
    options, frames = get_options_and_frames(inp)
    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        for graph in parse_triples_stream(
            frames=frames,
            options=options,
        ):
            sink = graph_factory()
            for graph_item in graph:
                if isinstance(graph_item, Prefix):
                    sink.bind(graph_item.prefix, graph_item.iri)
                else:
                    sink.add(graph_item)
            yield sink
        return
    elif options.stream_types.physical_type in (
        jelly.PHYSICAL_STREAM_TYPE_QUADS,
        jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
    ):
        for dataset in parse_quads_stream(
            frames=frames,
            options=options,
        ):
            sink = dataset_factory()
            for item in dataset:
                if isinstance(item, Prefix):
                    sink.bind(item.prefix, item.iri)
                else:
                    s, p, o, graph_name = item
                    context = sink.get_context(graph_name)
                    sink.add((s, p, o, context))
            yield sink
        return

    physical_type_name = jelly.PhysicalStreamType.Name(
        options.stream_types.physical_type
    )
    msg = f"the stream type {physical_type_name} is not supported "
    raise NotImplementedError(msg)


def parse_jelly_to_graph(
    inp: IO[bytes],
    graph_factory: Callable[[], Graph] = lambda: Graph(),
    dataset_factory: Callable[[], Dataset] = lambda: Dataset(),
) -> Graph | Dataset:
    """
    Add statements from Generator to provided Graph/Dataset.

    Args:
        inp (IO[bytes]): input jelly stream.
        graph_factory (Callable[[], Graph]): factory to create Graph.
            By default creates an empty in-memory Graph,
            but you can pass something else here.
        dataset_factory (Callable[[], Dataset]): factory to create Dataset.
            By default creates an empty in-memory Dataset,
            but you can pass something else here.

    Returns:
        Dataset | Graph: Dataset or Graph with statements.

    """
    options, frames = get_options_and_frames(inp)

    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        sink = graph_factory()
    if options.stream_types.physical_type in (
        jelly.PHYSICAL_STREAM_TYPE_QUADS,
        jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
    ):
        quad_sink = dataset_factory()
        sink = quad_sink

    for item in parse_jelly_flat(inp=inp, frames=frames, options=options):
        if isinstance(item, Prefix):
            sink.bind(item.prefix, item.iri)
        if isinstance(item, Triple):
            sink.add(item)
        if isinstance(item, Quad):
            s, p, o, graph_name = item
            context = quad_sink.get_context(graph_name)
            quad_sink.add((s, p, o, context))
    return sink


def parse_jelly_flat(
    inp: IO[bytes],
    frames: Iterable[jelly.RdfStreamFrame] | None = None,
    options: ParserOptions | None = None,
) -> Generator[Statement | Prefix]:
    """
    Parse jelly file with FLAT logical type into a Generator of stream events.

    Args:
        inp (IO[bytes]): input jelly buffered binary stream.
        frames (Iterable[jelly.RdfStreamFrame | None):
            jelly frames if read before.
        options (ParserOptions | None): stream options
            if read before.

    Raises:
        NotImplementedError: if physical type is not supported

    Yields:
        Generator[Statement | Prefix]: Generator of stream events

    """
    if not frames or not options:
        options, frames = get_options_and_frames(inp)

    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        for triples in parse_triples_stream(frames=frames, options=options):
            yield from triples
        return
    if options.stream_types.physical_type in (
        jelly.PHYSICAL_STREAM_TYPE_QUADS,
        jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
    ):
        for quads in parse_quads_stream(
            frames=frames,
            options=options,
        ):
            yield from quads
        return
    physical_type_name = jelly.PhysicalStreamType.Name(
        options.stream_types.physical_type
    )
    msg = f"the stream type {physical_type_name} is not supported "
    raise NotImplementedError(msg)


class RDFLibJellyParser(RDFLibParser):
    def parse(self, source: InputSource, sink: Graph) -> None:
        """
        Parse jelly file into provided RDFLib Graph.

        Args:
            source (InputSource): jelly file as buffered binary stream InputSource obj
            sink (Graph): RDFLib Graph

        Raises:
            TypeError: raises error if invalid input

        """
        inp = source.getByteStream()  # type: ignore[no-untyped-call]
        if inp is None:
            msg = "expected source to be a stream of bytes"
            raise TypeError(msg)
        parse_jelly_to_graph(
            inp,
            graph_factory=lambda: Graph(store=sink.store, identifier=sink.identifier),
            dataset_factory=lambda: Dataset(store=sink.store),
        )
