from __future__ import annotations

from collections.abc import Iterable

import rdflib
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID, Dataset, Graph
from rdflib.parser import InputSource
from rdflib.parser import Parser as RDFLibParser

from pyjelly import jelly
from pyjelly.consuming.decoder import Decoder
from pyjelly.consuming.ioutils import get_options_and_frames

GRAPH_END = object()


class RDFLibDecoder(Decoder):
    def transform_iri(self, iri: str) -> rdflib.URIRef:
        return rdflib.URIRef(iri)

    def transform_bnode(self, bnode: str) -> rdflib.BNode:
        return rdflib.BNode(bnode)

    def transform_default_graph(self) -> rdflib.URIRef:
        return DATASET_DEFAULT_GRAPH_ID

    def transform_literal(
        self,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
    ) -> rdflib.Literal:
        return rdflib.Literal(lex, lang=language, datatype=datatype)

    def transform_graph_start(
        self,
        graph_id: rdflib.URIRef | rdflib.BNode,
    ) -> rdflib.URIRef | rdflib.BNode:
        return graph_id

    def transform_graph_end(self) -> object:
        return GRAPH_END


class RDFLibJellyParser(RDFLibParser):
    def parse_triples(
        self,
        frames: Iterable[jelly.RdfStreamFrame],
        decoder: RDFLibDecoder,
        sink: Graph,
    ) -> None:
        for frame in frames:
            for triple in decoder.decode_stream_frame(frame):
                sink.add(triple)

    def parse_quads(
        self,
        frames: Iterable[jelly.RdfStreamFrame],
        decoder: RDFLibDecoder,
        dataset: Dataset,
    ) -> None:
        for frame in frames:
            for quad in decoder.decode_stream_frame(frame):
                dataset.add(quad)

    def parse_graphs(
        self,
        frames: Iterable[jelly.RdfStreamFrame],
        decoder: RDFLibDecoder,
        dataset: Dataset,
    ) -> None:
        current_graph = None

        for frame in frames:
            for obj in decoder.decode_stream_frame(frame):
                if isinstance(obj, tuple):
                    assert current_graph is not None
                    current_graph.add(obj)
                    continue
                if obj is GRAPH_END and current_graph is not None:
                    dataset.store.add_graph(current_graph)
                    current_graph = None
                    continue
                current_graph = Graph(store=dataset.store, identifier=obj)

    def parse(self, source: InputSource, sink: Graph) -> None:
        input_stream = source.getByteStream()  # type: ignore[no-untyped-call]
        if input_stream is None:
            msg = "expected source to be a stream of bytes"
            raise TypeError(msg)

        options, frames = get_options_and_frames(input_stream)
        decoder = RDFLibDecoder(options)

        if options.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
            self.parse_triples(frames=frames, decoder=decoder, sink=sink)
            return

        ds = Dataset(store=sink.store, default_union=True)
        ds.default_context = sink

        if options.physical_type == jelly.PHYSICAL_STREAM_TYPE_QUADS:
            self.parse_quads(frames=frames, decoder=decoder, dataset=ds)
            return

        if options.physical_type == jelly.PHYSICAL_STREAM_TYPE_GRAPHS:
            self.parse_graphs(frames=frames, decoder=decoder, dataset=ds)
            return

        msg = f"type {options.physical_type} is not yet supported"
        raise NotImplementedError(msg)
