from __future__ import annotations

from collections.abc import Generator
from typing import IO, Any
from typing_extensions import override

import rdflib
from google.protobuf.proto import serialize_length_prefixed
from rdflib.graph import Dataset, Graph, QuotedGraph
from rdflib.serializer import Serializer as RDFLibSerializer

from pyjelly import jelly
from pyjelly.options import StreamOptions
from pyjelly.producing import Stream
from pyjelly.producing.encoder import RowsAndTerm, Slot, TermEncoder
from pyjelly.producing.producers import FlatFrameProducer

DEFAULT_GRAPH_IRI = rdflib.URIRef("urn:x-rdflib:default")


class RDFLibTermEncoder(TermEncoder):
    def encode_any(self, term: object, slot: Slot) -> RowsAndTerm:
        if slot is Slot.graph and term == DEFAULT_GRAPH_IRI:
            return self.encode_default_graph()

        if isinstance(term, rdflib.URIRef):
            return self.encode_iri(term)

        if isinstance(term, rdflib.Literal):
            return self.encode_literal(
                lex=term,
                language=term.language,
                datatype=term.datatype,
            )

        if isinstance(term, rdflib.BNode):
            return self.encode_bnode(str(term))

        return super().encode_any(term, slot)  # error if not handled


def triple_stream(
    store: Graph,
    options: StreamOptions,
) -> Generator[jelly.RdfStreamFrame]:
    stream = Stream(
        encoder=RDFLibTermEncoder(
            name_lookup_size=options.name_lookup_size,
            prefix_lookup_size=options.prefix_lookup_size,
            datatype_lookup_size=options.datatype_lookup_size,
        ),
        producer=FlatFrameProducer(quads=False),
        physical_type=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
    )
    stream.begin(options)
    for triple in store:
        if frame := stream.triple(triple):
            yield frame
    if remaining := stream.producer.to_stream_frame():
        yield remaining


def quad_stream(
    store: Graph,
    options: StreamOptions,
) -> Generator[jelly.RdfStreamFrame]:
    stream = Stream(
        encoder=RDFLibTermEncoder(
            name_lookup_size=options.name_lookup_size,
            prefix_lookup_size=options.prefix_lookup_size,
            datatype_lookup_size=options.datatype_lookup_size,
        ),
        producer=FlatFrameProducer(quads=True),
        physical_type=jelly.PHYSICAL_STREAM_TYPE_QUADS,
    )
    stream.begin(options)
    for quad in store:
        if frame := stream.quad(quad):
            yield frame
    if remaining := stream.producer.to_stream_frame():
        yield remaining


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
        options: StreamOptions | None = None,
        **unused: Any,
    ) -> None:
        if options is None:
            options = StreamOptions.big()
        if isinstance(self.store, Dataset):
            frames = quad_stream(self.store, options=options)
        else:
            frames = triple_stream(self.store, options=options)

        if options.delimited:
            for frame in frames:
                serialize_length_prefixed(frame, out)
        else:
            for frame in frames:
                out.write(frame.SerializeToString(deterministic=True))
