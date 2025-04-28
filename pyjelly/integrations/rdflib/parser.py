from __future__ import annotations

import rdflib
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID, Graph
from rdflib.parser import InputSource
from rdflib.parser import Parser as RDFLibParser

from pyjelly import jelly
from pyjelly.consuming.decoder import Decoder
from pyjelly.consuming.ioutils import get_options_and_frames


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


class RDFLibJellyParser(RDFLibParser):
    def parse(self, source: InputSource, sink: Graph) -> None:
        input_stream = source.getByteStream()  # type: ignore[no-untyped-call]
        if input_stream is None:
            msg = "expected source to be a stream of bytes"
            raise TypeError(msg)

        options, frames = get_options_and_frames(input_stream)
        decoder = RDFLibDecoder(options)

        if options.physical_type is jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
            for frame in frames:
                for triple in decoder.decode_stream_frame(frame):
                    sink.add(triple)
            return

        ds = rdflib.Dataset(store=sink.store, default_union=True)
        ds.default_context = sink

        if options.physical_type is jelly.PHYSICAL_STREAM_TYPE_QUADS:
            for frame in frames:
                for quad in decoder.decode_stream_frame(frame):
                    ds.add(quad)
            return

        msg = f"type {options.physical_type} is not yet supported"
        raise NotImplementedError(msg)
