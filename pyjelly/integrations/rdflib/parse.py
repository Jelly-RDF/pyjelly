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
from pyjelly.options import StreamOptions
from pyjelly.parse.decode import Adapter, Decoder
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


def _adapter_missing(feature: str, *, options: StreamOptions) -> Never:
    physical_type_name = jelly.PhysicalStreamType.Name(
        options.stream_types.physical_type
    )
    logical_type_name = jelly.LogicalStreamType.Name(options.stream_types.logical_type)
    msg = (
        f"adapter with {physical_type_name} and {logical_type_name} "
        f"does not implement {feature}"
    )
    raise NotImplementedError(msg)


class RDFLibTriplesAdapter(RDFLibAdapter):
    graph: Graph

    def __init__(self, graph: Graph, options: StreamOptions) -> None:
        super().__init__(options=options)
        self.graph = graph

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
        return _adapter_missing("interpreting frames", options=self.options)


class RDFLibQuadsBaseAdapter(RDFLibAdapter):
    def __init__(self, dataset: Dataset, options: StreamOptions) -> None:
        super().__init__(options=options)
        self.dataset = dataset

    @override
    def frame(self) -> Dataset | None:
        if self.options.stream_types.logical_type == jelly.LOGICAL_STREAM_TYPE_DATASETS:
            this_dataset = self.dataset
            self.dataset = Dataset(store=self.dataset.store)
            return this_dataset
        if self.options.stream_types.logical_type in (
            jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED,
            jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
        ):
            return None
        return _adapter_missing("interpreting frames", options=self.options)


class RDFLibQuadsAdapter(RDFLibQuadsBaseAdapter):
    @override
    def namespace_declaration(self, name: str, iri: str) -> None:
        self.dataset.bind(name, self.iri(iri))

    @override
    def quad(self, terms: Iterable[Any]) -> Any:
        self.dataset.add(terms)  # type: ignore[arg-type]


class RDFLibGraphsAdapter(RDFLibQuadsBaseAdapter):
    _graph: Graph | None = None

    def __init__(self, dataset: Dataset, options: StreamOptions) -> None:
        super().__init__(dataset=dataset, options=options)
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
            self.dataset = Dataset(store=self.dataset.store)
            return this_dataset
        return super().frame()


def parse_flat_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    sink: Graph,
    options: StreamOptions,
) -> Dataset | Graph:
    assert options.stream_types.flat
    ds = None

    adapter: Adapter
    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        adapter = RDFLibTriplesAdapter(graph=sink, options=options)
    else:
        ds = Dataset(store=sink.store, default_union=True)
        ds.default_context = sink

        if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_QUADS:
            adapter = RDFLibQuadsAdapter(dataset=ds, options=options)

        else:  # jelly.PHYSICAL_STREAM_TYPE_GRAPHS
            adapter = RDFLibGraphsAdapter(dataset=ds, options=options)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        decoder.decode_frame(frame=frame)
    return ds or sink


def parse_grouped_graph_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    sink: Graph,
    options: StreamOptions,
) -> Dataset:
    adapter = RDFLibTriplesAdapter(graph=sink, options=options)
    ds = Dataset(store=sink.store, default_union=True)
    ds.default_context = sink
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        graph = decoder.decode_frame(frame=frame)
        ds.add_graph(graph)
    return ds


def parse_grouped_dataset_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: StreamOptions,
    store: Store | str = "default",
) -> Generator[Dataset]:
    adapter = RDFLibGraphsAdapter(dataset=Dataset(store=store), options=options)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        yield decoder.decode_frame(frame=frame)


def datasets_from_jelly(
    inp: IO[bytes],
    store: Store | str = "default",
) -> Generator[Dataset]:
    options, frames = get_options_and_frames(inp)
    assert not options.stream_types.flat, "stream does not yield datasets"
    assert options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES, (
        "stream does not yield datasets"
    )
    yield from parse_grouped_dataset_stream(
        frames=frames,
        options=options,
        store=store,
    )


def graph_or_dataset_from_jelly(
    inp: IO[bytes],
    sink: Graph,
) -> Dataset | Graph:
    options, frames = get_options_and_frames(inp)

    if options.stream_types.flat:
        return parse_flat_stream(frames=frames, sink=sink, options=options)

    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        return parse_grouped_graph_stream(frames=frames, sink=sink, options=options)

    msg = "the stream contains multiple datasets; use datasets_from_jelly() instead"
    raise TypeError(msg)


class RDFLibJellyParser(RDFLibParser):
    def parse(
        self,
        source: InputSource,
        sink: Graph,
    ) -> None:
        inp = source.getByteStream()  # type: ignore[no-untyped-call]
        if inp is None:
            msg = "expected source to be a stream of bytes"
            raise TypeError(msg)
        graph_or_dataset_from_jelly(inp, sink=sink)
