from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import rdflib
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID, Dataset, Graph
from rdflib.parser import InputSource
from rdflib.parser import Parser as RDFLibParser

from pyjelly import jelly
from pyjelly.consuming.decoder import Adapter, Decoder
from pyjelly.consuming.ioutils import get_options_and_frames
from pyjelly.errors import JellyConformanceError
from pyjelly.options import ConsumerStreamOptions


class RDFLibAdapter(Adapter):
    def iri(self, iri: str) -> rdflib.URIRef:
        return rdflib.URIRef(iri)

    def bnode(self, bnode: str) -> rdflib.BNode:
        return rdflib.BNode(bnode)

    def default_graph(self) -> rdflib.URIRef:
        return DATASET_DEFAULT_GRAPH_ID

    def literal(
        self,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
    ) -> rdflib.Literal:
        return rdflib.Literal(lex, lang=language, datatype=datatype)


class RDFLibTriplesAdapter(RDFLibAdapter):
    graph: Graph

    def __init__(self, graph: Graph, options: ConsumerStreamOptions) -> None:
        super().__init__(options=options)
        self.graph = graph

    def triple(self, terms: Iterable[Any]) -> Any:
        self.graph.add(terms)  # type: ignore[arg-type]

    def namespace_declaration(self, name: str, iri: str) -> None:
        self.graph.bind(name, self.iri(iri))


class RDFLibQuadsAdapter(RDFLibAdapter):
    def __init__(self, dataset: Dataset, options: ConsumerStreamOptions) -> None:
        super().__init__(options=options)
        self.dataset = dataset

    def namespace_declaration(self, name: str, iri: str) -> None:
        self.graph.bind(name, self.iri(iri))  # type: ignore[attr-defined]

    def quad(self, terms: Iterable[Any]) -> Any:
        self.dataset.add(terms)  # type: ignore[arg-type]


class RDFLibGraphsAdapter(RDFLibAdapter):
    _graph: Graph | None = None

    def __init__(self, dataset: Dataset, options: ConsumerStreamOptions) -> None:
        super().__init__(options=options)
        self.dataset = dataset
        self._graph = None

    def graph_start(self, graph_id: str) -> None:
        self._graph = Graph(store=self.dataset.store, identifier=graph_id)

    def namespace_declaration(self, name: str, iri: str) -> None:
        self.graph.bind(name, self.iri(iri))

    def triple(self, terms: Iterable[Any]) -> None:
        self.graph.add(terms)  # type: ignore[arg-type]

    def graph_end(self) -> None:
        self.dataset.store.add_graph(self.graph)
        self._graph = None

    @property
    def graph(self) -> Graph:
        if self._graph is None:
            msg = "new graph was not started"
            raise JellyConformanceError(msg)
        return self._graph


class RDFLibJellyParser(RDFLibParser):
    def parse(
        self,
        source: InputSource,
        sink: Graph,
    ) -> None:
        input_stream = source.getByteStream()  # type: ignore[no-untyped-call]
        if input_stream is None:
            msg = "expected source to be a stream of bytes"
            raise TypeError(msg)

        options, frames = get_options_and_frames(input_stream)

        adapter: Adapter

        if options.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
            adapter = RDFLibTriplesAdapter(graph=sink, options=options)
        else:
            ds = Dataset(store=sink.store, default_union=True)
            ds.default_context = sink

            if options.physical_type == jelly.PHYSICAL_STREAM_TYPE_QUADS:
                adapter = RDFLibQuadsAdapter(dataset=ds, options=options)

            else:  # options.physical_type == jelly.PHYSICAL_STREAM_TYPE_GRAPHS
                adapter = RDFLibGraphsAdapter(dataset=ds, options=options)

        decoder = Decoder(adapter=adapter)
        for frame in frames:
            decoder.decode_frame(frame=frame)
