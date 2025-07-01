from __future__ import annotations

from collections.abc import Generator, Iterable
from itertools import chain
from typing import IO, Any, Callable
from typing_extensions import Never, override

import rdflib
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID, Dataset, Graph
from rdflib.parser import InputSource
from rdflib.parser import Parser as RDFLibParser

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.options import StreamTypes
from pyjelly.parse.decode import (
    Adapter,
    Decoder,
    ParserOptions,
    ParsingMode,
    Prefix,
    Quad,
    Triple,
)
from pyjelly.parse.ioutils import get_options_and_frames
from pyjelly.parse.statement_sink import StatementSink


class RDFLibAdapter(Adapter):
    """
    RDFLib adapter class, is extended by triples and quads implementations.

    Args:
        Adapter (_type_): abstract adapter class

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
    def namespace_declaration(self, name: str, iri: str) -> Any:
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
        parsing_mode: ParsingMode = ParsingMode.FLAT,
    ) -> None:
        super().__init__(options=options, parsing_mode=parsing_mode)
        self.parsing_mode = parsing_mode

    @override
    def triple(self, terms: Iterable[Any]) -> Any:
        return Triple(*terms)


class RDFLibQuadsBaseAdapter(RDFLibAdapter):
    def __init__(
        self,
        options: ParserOptions,
        parsing_mode: ParsingMode = ParsingMode.FLAT,
    ) -> None:
        super().__init__(options=options, parsing_mode=parsing_mode)


class RDFLibQuadsAdapter(RDFLibQuadsBaseAdapter):
    """
    Extended RDFLib adapter for the QUADS physical type.

    Args:
        RDFLibQuadsBaseAdapter (_type_): base quads adapter
            (shared with graphs physical type)

    """

    @override
    def quad(self, terms: Iterable[Any]) -> Any:
        return Quad(*terms)


class RDFLibGraphsAdapter(RDFLibQuadsBaseAdapter):
    """
    Extension of RDFLibQuadsBaseAdapter for the GRAPHS physical type.

    Notes: introduces graph start/end, checks if graph exists.

    Args:
        RDFLibQuadsBaseAdapter (_type_): base adapter for quads management.

    Raises:
        JellyConformanceError: if no graph_start was encountered

    """

    _graph_id: str | None

    def __init__(
        self,
        options: ParserOptions,
        parsing_mode: ParsingMode = ParsingMode.FLAT,
    ) -> None:
        super().__init__(
            options=options,
            parsing_mode=parsing_mode,
        )
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
    def triple(self, terms: Iterable[Any]) -> Any:
        return Quad(*chain(terms, [self._graph_id]))

    @override
    def graph_end(self) -> None:
        self._graph_id = None


def parse_triples_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    parsing_mode: ParsingMode = ParsingMode.FLAT,
) -> Generator[Generator[Iterable[Any]]]:
    """
    Parse flat triple stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options
        parsing_mode (ParsingMode): specifies whether this is
            a flat or grouped parsing.

    Yields:
        Generator[Any]: Generator of statements per frame.

    """
    adapter = RDFLibTriplesAdapter(options, parsing_mode=parsing_mode)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        if parsing_mode is ParsingMode.FLAT:
            yield from decoder.iter_rows(frame)
        else:
            yield from decoder.decode_frame(frame)
    return


def parse_quads_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    parsing_mode: ParsingMode = ParsingMode.FLAT,
) -> Generator[Any]:
    """
    Parse flat quads stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options
        parsing_mode (ParsingMode): specifies whether this is
            a flat or grouped parsing.

    Yields:
        Generator[Any]: Generator of statements per frame.

    """
    adapter_class: type[RDFLibQuadsBaseAdapter]
    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_QUADS:
        adapter_class = RDFLibQuadsAdapter
    else:
        adapter_class = RDFLibGraphsAdapter
    adapter = adapter_class(
        options=options,
        parsing_mode=parsing_mode,
    )
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        if parsing_mode is ParsingMode.FLAT:
            yield from decoder.iter_rows(frame)
        else:
            yield from decoder.decode_frame(frame)
    return


def parse_jelly_grouped(
    inp: IO[bytes],
    graph_factory: Callable[[], StatementSink],
    dataset_factory: Callable[[], StatementSink],
) -> Generator[Any] | Generator[StatementSink]:
    """
    Take jelly file and return generators based on the detected logical type.

    Yields one graph/dataset per frame.

    Args:
        inp (IO[bytes]): input jelly buffered binary stream
        graph_factory (Callable): lambda to construct a Graph
        dataset_factory (Callable): lambda to construct a Dataset

    Raises:
        NotImplementedError: is raised if a logical type is not implemented

    Yields:
        Generator[Any] | Generator[StatementSink]:
            returns generators for graphs/datasets based on the type of input

    """
    options, frames = get_options_and_frames(inp)
    sink_factory = None
    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        sink_factory = graph_factory
        parsing_function = parse_triples_stream
    if options.stream_types.physical_type in (
        jelly.PHYSICAL_STREAM_TYPE_QUADS,
        jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
    ):
        sink_factory = dataset_factory
        parsing_function = parse_quads_stream
    if sink_factory:
        for element in parsing_function(
            frames=frames,
            options=options,
            parsing_mode=ParsingMode.GROUPED,
        ):
            sink = sink_factory()
            for item in element:
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
    graph_factory: Callable[[], Graph],
    dataset_factory: Callable[[], Dataset],
) -> Any | Graph | Dataset:
    """
    Add statements from Generator to provided Graph/Dataset.

    Args:
        inp (IO[bytes]): input jelly stream.
        graph_factory (Callable[[], Graph]): factory to create Graph.
        dataset_factory (Callable[[], Dataset]): factory to create Dataset.

    Returns:
        Any | Dataset | Graph: Dataset or Graph with statements.

    """
    options, frames = get_options_and_frames(inp)

    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        sink = graph_factory()
    if options.stream_types.physical_type in (
        jelly.PHYSICAL_STREAM_TYPE_QUADS,
        jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
    ):
        sink = dataset_factory()

    for item in parse_jelly_flat(inp=inp, frames=frames, options=options):
        if isinstance(item, Prefix):
            sink.bind(item.prefix, item.iri)
        else:
            sink.add(item)
    return sink


def parse_jelly_flat(
    inp: IO[bytes] | InputSource,
    frames: Iterable[jelly.RdfStreamFrame] | None = None,
    options: ParserOptions | None = None,
) -> Generator[Any, None, None]:
    """
    Parse jelly file with FLAT physical type into a Generator of stream events.

    Args:
        inp (IO[bytes] | InputSource): input jelly buffered binary stream
            or raw InputSource if called as a standalone.
        frames (Iterable[jelly.RdfStreamFrame | None):
            jelly frames if read before.
        options (ParserOptions | None): stream options
            if read before.

    Raises:
        NotImplementedError: if physical type is not supported

    Yields:
        Generator[Any, None, None]: Generator of stream events

    """
    if not frames or not options:
        if hasattr(inp, "getByteStream"):
            stream: IO[bytes] = inp.getByteStream()
        else:
            stream = inp
        options, frames = get_options_and_frames(stream)

    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        yield from parse_triples_stream(
            frames=frames, options=options, parsing_mode=ParsingMode.FLAT
        )
        return
    if options.stream_types.physical_type in (
        jelly.PHYSICAL_STREAM_TYPE_QUADS,
        jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
    ):
        yield from parse_quads_stream(
            frames=frames,
            options=options,
            parsing_mode=ParsingMode.FLAT,
        )
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
