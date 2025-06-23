from __future__ import annotations

from collections.abc import Generator, Iterable
from typing import IO, Any, Callable
from typing_extensions import Never, override

import rdflib
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID, Dataset, Graph
from rdflib.parser import InputSource
from rdflib.parser import Parser as RDFLibParser

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.options import StreamTypes
from pyjelly.parse.decode import Adapter, Decoder, ParserOptions, ParsingMode
from pyjelly.parse.ioutils import get_options_and_frames


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


def _adapter_missing(feature: str, *, stream_types: StreamTypes) -> Never:
    """
    Raise error if functionality is missing in adapter.

    TODO: currently not used anywhere due to logical types being removed

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

    Notes: has internal graph object which tracks
        triples and namespaces and can get flushed between frames.
    """

    def __init__(
        self,
        options: ParserOptions,
        graph_factory: Callable[[], Graph],
        parsing_mode: ParsingMode = ParsingMode.FLAT,
    ) -> None:
        super().__init__(options=options, parsing_mode=parsing_mode)
        self.graph = graph_factory()
        self.graph_factory = graph_factory
        self.parsing_mode = parsing_mode

    @override
    def triple(self, terms: Iterable[Any]) -> Any:
        self.graph.add(tuple(terms))

    @override
    def namespace_declaration(self, name: str, iri: str) -> None:
        self.graph.bind(name, self.iri(iri))

    def frame(self) -> Graph:
        """
        Finalize one frame in triples stream.

        Returns:
           Graph: frame content as a separate Graph
                and starts a new Graph

        """
        this_graph = self.graph
        self.graph = self.graph_factory()
        return this_graph


class RDFLibQuadsBaseAdapter(RDFLibAdapter):
    def __init__(
        self,
        options: ParserOptions,
        dataset_factory: Callable[[], Dataset],
        parsing_mode: ParsingMode = ParsingMode.FLAT,
    ) -> None:
        super().__init__(options=options, parsing_mode=parsing_mode)
        self.dataset = dataset_factory()
        self.dataset_factory = dataset_factory

    @override
    def frame(self) -> Dataset:
        current_dataset = self.dataset
        self.dataset = self.dataset_factory()
        return current_dataset


class RDFLibQuadsAdapter(RDFLibQuadsBaseAdapter):
    """
    Extended RDFLib adapter for the QUADS physical type.

    Notes:
        Adds triples and namespaces directly to
        dataset, so RDFLib handles the rest.

    Args:
        RDFLibQuadsBaseAdapter (_type_): base quads adapter
            (shared with graphs physical type)

    """

    @override
    def namespace_declaration(self, name: str, iri: str) -> None:
        self.dataset.bind(name, self.iri(iri))

    @override
    def quad(self, terms: Iterable[Any]) -> Any:
        self.dataset.add(tuple(terms))


class RDFLibGraphsAdapter(RDFLibQuadsBaseAdapter):
    """
    Extension of RDFLibQuadsBaseAdapter for the GRAPHS physical type.

    Notes: introduces graph start/end, checks if graph exists,
        dataset store management.

    Args:
        RDFLibQuadsBaseAdapter (_type_): base adapter for quads management.

    Raises:
        JellyConformanceError: if no graph_start was encountered

    """

    _graph_id: str | None

    def __init__(
        self,
        options: ParserOptions,
        dataset_factory: Callable[[], Dataset],
        parsing_mode: ParsingMode = ParsingMode.FLAT,
    ) -> None:
        super().__init__(
            options=options,
            dataset_factory=dataset_factory,
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
    def namespace_declaration(self, name: str, iri: str) -> None:
        self.dataset.bind(name, self.iri(iri))

    @override
    def triple(self, terms: Iterable[Any]) -> None:
        self.dataset.add((*terms, self._graph_id))

    @override
    def graph_end(self) -> None:
        self._graph_id = None


def parse_flat_triples_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    graph_factory: Callable[[], Graph],
    parsing_mode: ParsingMode = ParsingMode.FLAT,
) -> Generator[Graph]:
    """
    Parse flat triple stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options
        graph_factory (Callable): Lambda to construct a graph
        parsing_mode (ParsingMode): specifies whether this is
            a flat or grouped parsing.

    Yields:
        Generator[Graph]: RDFLib Graph(s)

    """
    adapter = RDFLibTriplesAdapter(
        options, graph_factory=graph_factory, parsing_mode=parsing_mode
    )
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        g = decoder.decode_frame(frame)
        if g is not None:
            yield g

    if parsing_mode is ParsingMode.FLAT:
        yield adapter.graph


def parse_flat_quads_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    dataset_factory: Callable[[], Dataset],
    parsing_mode: ParsingMode = ParsingMode.FLAT,
) -> Generator[Dataset]:
    """
    Parse flat quads stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options
        dataset_factory (Callable): Lambda to construct a dataset
        parsing_mode (ParsingMode): specifies whether this is
            a flat or grouped parsing.

    Yields:
        Generator[Dataset]: RDFLib dataset(s)

    """
    adapter_class: type[RDFLibQuadsBaseAdapter]
    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_QUADS:
        adapter_class = RDFLibQuadsAdapter
    else:
        adapter_class = RDFLibGraphsAdapter
    adapter = adapter_class(
        options=options,
        dataset_factory=dataset_factory,
        parsing_mode=parsing_mode,
    )
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        ds = decoder.decode_frame(frame)
        if ds is not None:
            yield ds

    if parsing_mode is ParsingMode.FLAT:
        yield adapter.dataset


def parse_jelly_grouped(
    inp: IO[bytes],
    graph_factory: Callable[[], Graph],
    dataset_factory: Callable[[], Dataset],
) -> Generator[Any] | Generator[Graph] | Generator[Dataset]:
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
        Generator[Any] | Generator[Dataset] | Generator[Graph]:
            returns generators for graphs/datasets based on the type of input

    """
    options, frames = get_options_and_frames(inp)

    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        yield from parse_flat_triples_stream(
            frames=frames,
            options=options,
            graph_factory=graph_factory,
            parsing_mode=ParsingMode.GROUPED,
        )
        return

    if options.stream_types.physical_type in (
        jelly.PHYSICAL_STREAM_TYPE_QUADS,
        jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
    ):
        yield from parse_flat_quads_stream(
            frames=frames,
            options=options,
            dataset_factory=dataset_factory,
            parsing_mode=ParsingMode.GROUPED,
        )
        return

    physical_type_name = jelly.PhysicalStreamType.Name(
        options.stream_types.physical_type
    )
    msg = f"the stream type {physical_type_name} is not supported "
    raise NotImplementedError(msg)


def parse_jelly_flat(
    inp: IO[bytes],
    graph_factory: Callable[[], Graph],
    dataset_factory: Callable[[], Dataset],
) -> Any | Dataset | Graph:
    """
    Parse jelly file with FLAT physical type into one Graph/Dataset.

    Args:
        inp (IO[bytes]): input jelly buffered binary stream
        graph_factory (Callable): lambda to construct a Graph
        dataset_factory (Callable): lambda to construct a Dataset

    Raises:
        NotImplementedError: if physical type is not supported

    Returns:
        RDFLib Graph or Dataset

    """
    options, frames = get_options_and_frames(inp)

    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        return next(
            parse_flat_triples_stream(
                frames=frames,
                options=options,
                graph_factory=graph_factory,
                parsing_mode=ParsingMode.FLAT,
            )
        )

    if options.stream_types.physical_type in (
        jelly.PHYSICAL_STREAM_TYPE_QUADS,
        jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
    ):
        return next(
            parse_flat_quads_stream(
                frames=frames,
                options=options,
                dataset_factory=dataset_factory,
                parsing_mode=ParsingMode.FLAT,
            )
        )
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
        parse_jelly_flat(
            inp,
            graph_factory=lambda: Graph(store=sink.store, identifier=sink.identifier),
            dataset_factory=lambda: Dataset(store=sink.store),
        )
