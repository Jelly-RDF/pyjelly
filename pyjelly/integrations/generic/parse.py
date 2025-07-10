from __future__ import annotations

from collections.abc import Generator, Iterable
from itertools import chain
from typing import IO, Any, Callable, Union
from typing_extensions import override

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.generic.generic_sink import (
    IRI,
    BlankNode,
    GenericStatementSink,
    Literal,
    Prefix,
    Quad,
    Triple,
)
from pyjelly.parse.decode import Adapter, Decoder, ParserOptions
from pyjelly.parse.ioutils import get_options_and_frames

Statement = Union[Triple, Quad]


class GenericStatementSinkAdapter(Adapter):
    """
    Implement Adapter for generic statements.

    Notes:
        Returns custom RDF terms expected by GenericStatementSink,
        handles namespace declarations, and quoted triples.

    Args:
        Adapter (_type_): base Adapter class

    """

    @override
    def iri(self, iri: str) -> IRI:
        return IRI(iri)

    @override
    def bnode(self, bnode: str) -> BlankNode:
        return BlankNode(bnode)

    @override
    def default_graph(self) -> str:
        return ""

    @override
    def literal(
        self,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
    ) -> Literal:
        return Literal(lex, language, datatype)

    @override
    def namespace_declaration(self, name: str, iri: str) -> Prefix:
        return Prefix(name, self.iri(iri))

    @override
    def quoted_triple(self, terms: Iterable[Any]) -> Triple:
        return Triple(*terms)


class GenericTriplesAdapter(GenericStatementSinkAdapter):
    """
    Triples adapted implementation for GenericStatementSink.

    Args:
        GenericStatementSinkAdapter (_type_): base GenericStatementSink
            adapter implementation that handles terms and namespaces.

    """

    def __init__(
        self,
        options: ParserOptions,
    ) -> None:
        super().__init__(options=options)

    @override
    def triple(self, terms: Iterable[Any]) -> Triple:
        return Triple(*terms)


class GenericQuadsBaseAdapter(GenericStatementSinkAdapter):
    def __init__(self, options: ParserOptions) -> None:
        super().__init__(options=options)


class GenericQuadsAdapter(GenericQuadsBaseAdapter):
    """
    Extends GenericQuadsBaseAdapter for QUADS physical type.

    Args:
        GenericQuadsBaseAdapter (_type_): quads adapter that handles
            base quads processing.

    """

    @override
    def quad(self, terms: Iterable[Any]) -> Quad:
        return Quad(*terms)


class GenericGraphsAdapter(GenericQuadsBaseAdapter):
    """
    Extends GenericQuadsBaseAdapter for GRAPHS physical type.

    Notes:
        introduces graph start/end, checks if graph exists.

    Args:
        GenericQuadsBaseAdapter (_type_): quads adapter that handles
            base quads processing.

    Raises:
        JellyConformanceError: raised if graph start message was not received.

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
    adapter = GenericTriplesAdapter(options)
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
    adapter_class: type[GenericQuadsBaseAdapter]
    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_QUADS:
        adapter_class = GenericQuadsAdapter
    else:
        adapter_class = GenericGraphsAdapter
    adapter = adapter_class(options=options)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        yield decoder.iter_rows(frame)
    return


def parse_jelly_grouped(
    inp: IO[bytes],
    sink_factory: Callable[[], GenericStatementSink] = lambda: GenericStatementSink(),
) -> Generator[GenericStatementSink]:
    """
    Take a jelly file and return generators of generic statements sinks.

    Yields one generic statements sink per frame.

    Args:
        inp (IO[bytes]): input jelly buffered binary stream
        sink_factory (Callable): lambda to construct a statement sink.
            By default creates an empty in-memory GenericStatementSink.

    Raises:
        NotImplementedError: is raised if a physical type is not implemented

    Yields:
        Generator[GenericStatementSink]:
            returns generators for GenericStatementSink, regardless of stream type.

    """
    options, frames = get_options_and_frames(inp)
    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        for graph in parse_triples_stream(
            frames=frames,
            options=options,
        ):
            sink = sink_factory()
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
            sink = sink_factory()
            for item in dataset:
                if isinstance(item, Prefix):
                    sink.bind(item.prefix, item.iri)
                else:
                    sink.add(item)
            yield sink
        return

    physical_type_name = jelly.PhysicalStreamType.Name(
        options.stream_types.physical_type
    )
    msg = f"the stream type {physical_type_name} is not supported "
    raise NotImplementedError(msg)


def parse_jelly_to_graph(
    inp: IO[bytes],
    sink_factory: Callable[[], GenericStatementSink] = lambda: GenericStatementSink(),
) -> GenericStatementSink:
    """
    Add statements from Generator to GenericStatementSink.

    Args:
        inp (IO[bytes]): input jelly stream.
        sink_factory (Callable[[], GenericStatementSink]): factory to create
            statement sink.
            By default creates an empty in-memory GenericStatementSink.
            Has no division for datasets/graphs,
            utilizes the same underlying data structures.

    Returns:
        GenericStatementSink: GenericStatementSink with statements.

    """
    options, frames = get_options_and_frames(inp)
    sink = sink_factory()

    for item in parse_jelly_flat(inp=inp, frames=frames, options=options):
        if isinstance(item, Prefix):
            sink.bind(item.prefix, item.iri)
        else:
            sink.add(item)
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
