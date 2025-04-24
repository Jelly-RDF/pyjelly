from __future__ import annotations

from collections.abc import Iterable
from typing import IO, Any
from typing_extensions import override

import rdflib
from google.protobuf.proto import serialize_length_prefixed
from rdflib.graph import Graph, QuotedGraph
from rdflib.serializer import Serializer as RDFLibSerializer
from rdflib.term import Node

from pyjelly import jelly
from pyjelly.options import StreamOptions
from pyjelly.producing import stream_to_frame, stream_to_frames
from pyjelly.producing.encoder import Encoder, TermName
from pyjelly.producing.producers import FlatProducer, Producer


def serialize_delimited(
    out: IO[bytes],
    *,
    producer: Producer,
    encoder: Encoder,
    statements: Iterable[Iterable[Node]],
) -> None:
    for frame in stream_to_frames(
        encoder=encoder,
        statements=statements,
        producer=producer,
    ):
        serialize_length_prefixed(frame, out)


def serialize(
    out: IO[bytes],
    *,
    encoder: Encoder,
    statements: Iterable[Iterable[Node]],
) -> None:
    frame = stream_to_frame(encoder=encoder, statements=statements)
    out.write(frame.SerializeToString(deterministic=True))


class RDFLibEncoder(Encoder):
    @override
    def encode_term(self, term: Node, name: TermName) -> None:
        if term is None and name == "g":
            return
        if isinstance(term, rdflib.URIRef):
            self.encode_iri(term, term_name=name)
        elif isinstance(term, rdflib.Literal):
            self.encode_literal(
                lex=term,
                language=term.language,
                datatype=term.datatype,
                term_name=name,
            )
        elif isinstance(term, rdflib.BNode):
            self.encode_bnode(term, term_name=name)
        else:
            msg = f"term of type {type(term)} is unsupported"
            raise TypeError(msg)


class RDFLibJellySerializer(RDFLibSerializer):
    """
    RDFLib serializer for writing graphs in Jelly RDF stream format.

    Handles streaming RDF terms into Jelly frames using internal encoders.
    Supports only graphs and datasets (not quoted graphs).

    """

    def __init__(self, store: Graph) -> None:
        if isinstance(store, QuotedGraph):
            msg = "N3 format is not supported"
            raise NotImplementedError(msg)
        super().__init__(store)

    @override
    def serialize(  # type: ignore[override]
        self,
        out: IO[bytes],
        /,
        *,
        quads: bool = False,
        producer: Producer | None = None,
        options: StreamOptions | None = None,
        **unused: Any,
    ) -> None:
        store = self.store
        statements: Iterable[Iterable[Any]] = store
        if isinstance(store, rdflib.Dataset):
            if quads:
                physical_type = jelly.PHYSICAL_STREAM_TYPE_QUADS
                statements = store.quads()
            else:
                physical_type = jelly.PHYSICAL_STREAM_TYPE_GRAPHS
                statements = store.graphs()
        else:
            physical_type = jelly.PHYSICAL_STREAM_TYPE_TRIPLES
        encoder = RDFLibEncoder(physical_type=physical_type, options=options)
        if encoder.options.delimited:
            serialize_delimited(
                out,
                producer=producer or FlatProducer(targets_quads=quads),
                encoder=encoder,
                statements=statements,
            )
        else:
            serialize(out, encoder=encoder, statements=statements)
