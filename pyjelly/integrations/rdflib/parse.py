from __future__ import annotations

from collections.abc import Generator, Iterable
from typing import IO, Any
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

    graph: Graph

    def __init__(self, options: ParserOptions, store: Store | str = "default") -> None:
        super().__init__(options=options)
        self.graph = Graph(store=store)

    @override
    def triple(self, terms: Iterable[Any]) -> Any:
        self.graph.add(terms)  # type: ignore[arg-type]

    @override
    def namespace_declaration(self, name: str, iri: str) -> None:
        self.graph.bind(name, self.iri(iri))

    def frame(self) -> Graph | None:
        """
        Finalize one frame in triples stream.

        Returns:
            Graph | None: graph if logical type is GRAPHS and starts a new graph
                if flat/unspecified, does not wrap each frame into a separate graph
                and just leaves everything in one graph
                TODO: this logic breaks conformance tests bc it keeps adding
                triples to one graph despite frames division

        """
        if self.options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_GRAPHS:
            this_graph = self.graph
            self.graph = Graph(store=self.graph.store)
            return this_graph
        if self.options.stream_types.logical_type in (
            jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED,
            jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
        ):
            return None
        return _adapter_missing(
            "interpreting frames",
            stream_types=self.options.stream_types,
        )


class RDFLibQuadsBaseAdapter(RDFLibAdapter):
    def __init__(
        self,
        options: ParserOptions,
        store: Store | str,
    ) -> None:
        super().__init__(options=options)
        self.store = store
        self.dataset = self.new_dataset()

    def new_dataset(self) -> Dataset:
        return Dataset(store=self.store, default_union=True)

    @override
    def frame(self) -> Dataset | None:
        """
        Finalize one frame in quads stream.

        Returns:
            Dataset | None: returns frame content as dataset if
                logical type = DATASETS and starts a new dataset,
                otherwise, do nothing

        """
        if self.options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_DATASETS:
            this_dataset = self.dataset
            self.dataset = self.new_dataset()
            return this_dataset
        if self.options.stream_types.logical_type in (
            jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED,
            jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
        ):
            return None
        return _adapter_missing(
            "interpreting frames", stream_types=self.options.stream_types
        )


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
        self.dataset.add(terms)  # type: ignore[arg-type]


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

    _graph: Graph | None = None

    def __init__(
        self,
        options: ParserOptions,
        store: Store | str,
    ) -> None:
        super().__init__(options=options, store=store)
        self._graph = None

    @property
    def graph(self) -> Graph:
        if self._graph is None:
            msg = "new graph was not started"
            raise JellyConformanceError(msg)
        return self._graph

    @override
    def graph_start(self, graph_id: str) -> None:
        self._graph = Graph(store=self.dataset.store, identifier=graph_id)

    @override
    def namespace_declaration(self, name: str, iri: str) -> None:
        self.graph.bind(name, self.iri(iri))

    @override
    def triple(self, terms: Iterable[Any]) -> None:
        self.graph.add(terms)  # type: ignore[arg-type]

    @override
    def graph_end(self) -> None:
        self.dataset.store.add_graph(self.graph)
        self._graph = None

    def frame(self) -> Dataset | None:
        """
        Finalize one frame in graphs stream.

        Returns:
            Dataset | None: if logical type DATASETS, returns current dataset
                and flushes current graph and dataset.
                Otherwise, falls back to default quads frame logic (do nothing).

        """
        if self.options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_DATASETS:
            this_dataset = self.dataset
            self._graph = None
            self.dataset = self.new_dataset()
            return this_dataset
        return super().frame()


def parse_flat_triples_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    store: Store | str = "default",
    identifier: str | None = None,
) -> Dataset | Graph:
    """
    Parse flat triple stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options
        store (Store | str, optional): RDFLib store. Defaults to "default".
        identifier (str | None, optional): graph identifier. Defaults to None.

    Returns:
        Dataset | Graph: TODO: based on RDFLibTriplesAdapter
            it looks like it can only return Graph

    """
    assert options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
    adapter = RDFLibTriplesAdapter(options, store=store)
    if identifier is not None:
        adapter.graph = Graph(identifier=identifier, store=store)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        decoder.decode_frame(frame=frame)
    return adapter.graph


def parse_flat_quads_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    store: Store | str = "default",
    identifier: str | None = None,
) -> Dataset:
    """
    Parse flat quads stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options
        store (Store | str, optional): RDFLib store. Defaults to "default"
        identifier (str | None, optional): RDFLib identifier for default context.
            Defaults to None.

    Returns:
        Dataset: RDFLib dataset (one!, because LOGICAL_STREAM_TYPE_FLAT_QUADS)

    """
    assert options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS
    adapter_class: type[RDFLibQuadsBaseAdapter]
    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_QUADS:
        adapter_class = RDFLibQuadsAdapter
    else:  # jelly.PHYSICAL_STREAM_TYPE_GRAPHS
        adapter_class = RDFLibGraphsAdapter
    adapter = adapter_class(options=options, store=store)
    adapter.dataset.default_context = Graph(identifier=identifier, store=store)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        decoder.decode_frame(frame=frame)
    return adapter.dataset


def parse_graph_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    store: Store | str = "default",
) -> Generator[Graph]:
    """
    Parse graph stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options
        store (Store | str, optional): RDFLib store.. Defaults to "default".

    Yields:
        Generator[Graph]: returns one graph per frame

    """
    assert options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_GRAPHS
    adapter = RDFLibTriplesAdapter(options, store=store)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        yield decoder.decode_frame(frame=frame)


def graphs_from_jelly(
    inp: IO[bytes],
    store: Store | str = "default",
) -> Generator[Any] | Generator[Dataset] | Generator[Graph]:
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
        yield parse_flat_triples_stream(frames=frames, options=options, store=store)
        return

    if options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS:
        yield parse_flat_quads_stream(frames=frames, options=options, store=store)
        return

    if options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_GRAPHS:
        yield from parse_graph_stream(frames=frames, options=options, store=store)
        return

    logical_type_name = jelly.LogicalStreamType.Name(options.stream_types.logical_type)
    msg = f"the stream type {logical_type_name} is not supported "
    raise NotImplementedError(msg)


def graph_from_jelly(
    inp: IO[bytes],
    store: Store | str = "default",
    identifier: str | None = None,
) -> Any | Dataset | Graph:
    """
    Parse graph from jelly.

    Notes:
        Compared to previous function, returns actual
        graph/datasets.

    Args:
        inp (IO[bytes]): input jelly buffered binary stream
        store (Store | str, optional): RDFLib store. Defaults to "default".
        identifier (str | None, optional): Graph identifier from RDFLib obj.
            Defaults to None.

    Raises:
        NotImplementedError: raises an error if multiple datasets are in the stream?
            TODO: probably should not be a problem for logical-type agnostic parsing?
        NotImplementedError: if logical type is not supported

    Returns:
        Any | Dataset | Graph: returns dataset if logical type is QUADS or GRAPHS
            with passed identifier assigned to a default context

    """
    options, frames = get_options_and_frames(inp)

    if options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_DATASETS:
        msg = (
            "the stream contains multiple datasets and cannot be parsed into "
            "a single dataset"
        )
        raise NotImplementedError(msg)

    if options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES:
        return parse_flat_triples_stream(
            frames=frames,
            options=options,
            store=store,
            identifier=identifier,
        )

    if options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS:
        return parse_flat_quads_stream(
            frames=frames,
            options=options,
            store=store,
            identifier=identifier,
        )

    if options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_GRAPHS:
        ds = Dataset(store=store, default_union=True)
        ds.default_context = Graph(identifier=identifier, store=store)

        for graph in parse_graph_stream(frames=frames, options=options, store=store):
            ds.add_graph(graph)

        return ds

    logical_type_name = jelly.LogicalStreamType.Name(options.stream_types.logical_type)
    msg = f"the stream type {logical_type_name} is not supported "
    raise NotImplementedError(msg)


class RDFLibJellyParser(RDFLibParser):
    def parse(self, source: InputSource, sink: Graph) -> None:
        """
        Parse jelly file to RDFLib graph.

        Args:
            source (InputSource): jelly file as buffered binary stream InputSource obj
            sink (Graph): RDFLib graph

        Raises:
            TypeError: raises error if invalid input

        """
        inp = source.getByteStream()  # type: ignore[no-untyped-call]
        if inp is None:
            msg = "expected source to be a stream of bytes"
            raise TypeError(msg)
        graph_from_jelly(
            inp,
            identifier=sink.identifier,
            store=sink.store,
        )
