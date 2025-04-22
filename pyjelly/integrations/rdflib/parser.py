from __future__ import annotations

from collections.abc import Iterator
from itertools import chain
from typing import IO

import rdflib
from google.protobuf.proto import parse_length_prefixed
from rdflib.graph import Graph
from rdflib.parser import InputSource
from rdflib.parser import Parser as RDFLibParser

from pyjelly import jelly
from pyjelly.consuming import options_from_frame
from pyjelly.consuming.decoder import Decoder


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


def get_frames(inp: IO[bytes]) -> Iterator[jelly.RdfStreamFrame]:
    while frame := parse_length_prefixed(jelly.RdfStreamFrame, inp):
        yield frame


class RDFLibJellyParser(RDFLibParser):
    def parse(self, source: InputSource, sink: Graph) -> None:
        inp = source.getByteStream()  # type: ignore[no-untyped-call]
        if inp is None:
            msg = "expected source to be a stream of bytes"
            raise TypeError(msg)

        frames = get_frames(inp)
        first_frame = next(frames)
        options = options_from_frame(first_frame, delimited=True)
        decoder = RDFLibDecoder(options)

        for frame in chain((first_frame,), frames):
            for row in decoder.decode_stream_frame(frame):
                sink.add(row)


print(__package__)
