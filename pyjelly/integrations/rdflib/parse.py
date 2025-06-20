from __future__ import annotations

from collections.abc import Generator, Iterable
from typing import IO, Any, Callable
from typing_extensions import Never, override

import rdflib
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID, Dataset, Graph
from rdflib.parser import InputSource
from rdflib.parser import Parser as RDFLibParser
from rdflib.store import Store

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.options import StreamTypes
from pyjelly.parse.decode import Adapter, Decoder, ParserOptions
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

    def __init__(self, options: ParserOptions, graph: Graph) -> None:
        super().__init__(options=options)
        self.graph = graph

    @override
    def triple(self, terms: Iterable[Any]) -> Any:
        self.graph.add(tuple(terms))

    @override
    def namespace_declaration(self, name: str, iri: str) -> None:
        self.graph.bind(name, self.iri(iri))

    def frame(self) -> None:
        """
        Finalize one frame in triples stream.

        Returns:
           None
           TODO: this logic breaks conformance tests bc it keeps adding
                triples to one graph despite frames division

        """


class RDFLibQuadsBaseAdapter(RDFLibAdapter):
    def __init__(
        self,
        options: ParserOptions,
        dataset: Dataset,
    ) -> None:
        super().__init__(options=options)
        self.dataset = dataset

    @override
    def frame(self) -> None:
        """
        Finalize one frame in quads stream.

        TODO: what should happen here?

        """


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
        dataset: Dataset,
    ) -> None:
        super().__init__(options=options, dataset=dataset)
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

    def frame(self) -> None:
        """
        Finalize one frame in graphs stream.

        Returns:
            Falls back to default quads frame logic (do nothing).

        """
        return super().frame()


def parse_flat_triples_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    graph: Graph,
) -> Graph:
    """
    Parse flat triple stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options
        graph (Graph): RDFLib Graph

    Returns:
        Graph: RDFLib Graph

    """
    adapter = RDFLibTriplesAdapter(options, graph=graph)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        decoder.decode_frame(frame=frame)
    return adapter.graph


def parse_flat_quads_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    dataset: Dataset,
) -> Dataset:
    """
    Parse flat quads stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options
        dataset (Dataset): RDFLib dataset

    Returns:
        Dataset: RDFLib dataset (one!)

    """
    adapter_class: type[RDFLibQuadsBaseAdapter]
    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_QUADS:
        adapter_class = RDFLibQuadsAdapter
    else:  # jelly.PHYSICAL_STREAM_TYPE_GRAPHS
        adapter_class = RDFLibGraphsAdapter
    adapter = adapter_class(options=options, dataset=dataset)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        decoder.decode_frame(frame=frame)
    return adapter.dataset


def parse_graph_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    dataset: Dataset,
) -> Generator[Dataset]:
    """
    Parse graph stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options
        dataset: RDFLib dataset. Defaults to "default".

    Yields:
        Generator[Graph]: returns one graph per frame

    """
    # TODO (Nastya): this should be adapted further
    adapter = RDFLibTriplesAdapter(options, graph=dataset)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        yield decoder.decode_frame(frame=frame)


def graphs_from_jelly(
    inp: IO[bytes],
    store: Store | str = "default",
) -> Generator[Any]:
    """
    Take jelly file and return generators based on the detected logical type.

    TODO: ig should be made logical-type-agnostic but we need to decide on
        the default logical-type agnostic behaviour

    Args:
        inp (IO[bytes]): input jelly buffered binary stream
        store (Store | str, optional): Defaults to "default".
            Store is just an rdflib Store or a custom OrderedMemory extension of rdflib
        "default" is also the part of the rdflib
    Raises:
        NotImplementedError: is raised if a logical type is not implemented

    Yields:
        Generator[Any] | Generator[Dataset] | Generator[Graph]:
            returns generators for graphs/datasets based on the type of input
            TODO: should be aligned with the default behaviour

    """
    options, frames = get_options_and_frames(inp)

    if options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES:
        yield parse_flat_triples_stream(frames=frames, options=options, store=store)  # type: ignore[call-arg]
        return

    if options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS:
        yield parse_flat_quads_stream(frames=frames, options=options, store=store)  # type: ignore[call-arg]
        return

    if options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_GRAPHS:
        yield from parse_graph_stream(frames=frames, options=options, store=store)  # type: ignore[call-arg]
        return

    logical_type_name = jelly.LogicalStreamType.Name(options.stream_types.logical_type)
    msg = f"the stream type {logical_type_name} is not supported "
    raise NotImplementedError(msg)


def parse_jelly_flat(
    inp: IO[bytes],
    graph_factory: Callable[[], Graph],
    dataset_factory: Callable[[], Dataset],
) -> Any | Dataset | Graph:
    """
    Parse jelly file with FLAT physical type.

    Notes:
        Compared to previous function, modifies existing Graph/Dataset.

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
        graph = graph_factory()
        parse_flat_triples_stream(frames=frames, options=options, graph=graph)
        return graph

    if options.stream_types.physical_type in (
        jelly.PHYSICAL_STREAM_TYPE_QUADS,
        jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
    ):
        dataset = dataset_factory()
        parse_flat_quads_stream(
            frames=frames,
            options=options,
            dataset=dataset,
        )
        return dataset
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
