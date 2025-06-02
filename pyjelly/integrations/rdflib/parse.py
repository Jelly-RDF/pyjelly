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
    physical_type_name = jelly.PhysicalStreamType.Name(stream_types.physical_type)
    logical_type_name = jelly.LogicalStreamType.Name(stream_types.logical_type)
    msg = (
        f"adapter with {physical_type_name} and {logical_type_name} "
        f"does not implement {feature}"
    )
    raise NotImplementedError(msg)


class RDFLibTriplesAdapter(RDFLibAdapter):
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
    @override
    def namespace_declaration(self, name: str, iri: str) -> None:
        self.dataset.bind(name, self.iri(iri))

    @override
    def quad(self, terms: Iterable[Any]) -> Any:
        self.dataset.add(terms)  # type: ignore[arg-type]


class RDFLibGraphsAdapter(RDFLibQuadsBaseAdapter):
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
    assert options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_GRAPHS
    adapter = RDFLibTriplesAdapter(options, store=store)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        yield decoder.decode_frame(frame=frame)


def graphs_from_jelly(
    inp: IO[bytes],
    store: Store | str = "default",
) -> Generator[Any] | Generator[Dataset] | Generator[Graph]:
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
        inp = source.getByteStream()  # type: ignore[no-untyped-call]
        if inp is None:
            msg = "expected source to be a stream of bytes"
            raise TypeError(msg)
        graph_from_jelly(
            inp,
            identifier=sink.identifier,
            store=sink.store,
        )
