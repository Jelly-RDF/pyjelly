from __future__ import annotations

import rdflib
from rdflib.graph import Graph
from rdflib.parser import InputSource
from rdflib.parser import Parser as RDFLibParser

from pyjelly.consuming.decoder import Decoder
from pyjelly.consuming.ioutils import get_options_and_frames


class RDFLibDecoder(Decoder):
    def transform_iri(self, iri: str) -> rdflib.URIRef:
        return rdflib.URIRef(iri)

    def transform_bnode(self, iri: str) -> rdflib.BNode:
        return rdflib.BNode(iri)

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

        for frame in frames:
            for row in decoder.decode_stream_frame(frame):
                sink.add(row)
