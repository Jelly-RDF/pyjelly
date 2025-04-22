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


def is_delimited_jelly(header: bytes) -> bool:
    """
    Detect whether a Jelly file is delimited from its first 3 bytes.

    Truth table (notation: 0A = 0x0A, NN = not 0x0A, ?? = don't care):

    | Byte 1 | Byte 2 | Byte 3 | Result                                   |
    |--------|--------|--------|------------------------------------------|
    | NN     |  ??    |  ??    | Delimited                                |
    | 0A     |  NN    |  ??    | Non-delimited                            |
    | 0A     |  0A    |  NN    | Delimited (size = 10)                    |
    | 0A     |  0A    |  0A    | Non-delimited (stream options size = 10) |

    >>> is_delimited_jelly(bytes([0x00, 0x00, 0x00]))
    True

    >>> is_delimited_jelly(bytes([0x00, 0x00, 0x0A]))
    True

    >>> is_delimited_jelly(bytes([0x00, 0x0A, 0x00]))
    True

    >>> is_delimited_jelly(bytes([0x00, 0x0A, 0x0A]))
    True

    >>> is_delimited_jelly(bytes([0x0A, 0x00, 0x00]))
    False

    >>> is_delimited_jelly(bytes([0x0A, 0x00, 0x0A]))
    False

    >>> is_delimited_jelly(bytes([0x0A, 0x0A, 0x00]))
    True

    >>> is_delimited_jelly(bytes([0x0A, 0x0A, 0x0A]))
    False
    """
    return len(header) == 3 and (  # noqa: PLR2004
        header[0] != 0x0A or (header[1] == 0x0A and header[2] != 0x0A)  # noqa: PLR2004
    )


class RDFLibDecoder(Decoder):
    def make_iri(self, iri: str) -> rdflib.URIRef:
        return rdflib.URIRef(iri)

    def make_bnode(self, iri: str) -> rdflib.BNode:
        return rdflib.BNode(iri)

    def make_literal(
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
            for row in decoder.decode_frame(frame):
                sink.add(row)
