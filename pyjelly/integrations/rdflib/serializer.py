from __future__ import annotations

from collections.abc import Generator
from typing import IO, Any
from typing_extensions import override

import rdflib
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID, Dataset, Graph, QuotedGraph
from rdflib.serializer import Serializer as RDFLibSerializer

from pyjelly import jelly
from pyjelly.options import StreamOptions
from pyjelly.producing import GraphStream, QuadStream, TripleStream
from pyjelly.producing.encoder import RowsAndTerm, Slot, TermEncoder
from pyjelly.producing.ioutils import write_delimited, write_single
from pyjelly.producing.producers import FrameProducer


class RDFLibTermEncoder(TermEncoder):
    def encode_any(self, term: object, slot: Slot) -> RowsAndTerm:
        if slot is Slot.graph and term == DATASET_DEFAULT_GRAPH_ID:
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

    def triples_stream(
        self,
        *,
        producer: FrameProducer | None = None,
        options: StreamOptions,
    ) -> Generator[jelly.RdfStreamFrame]:
        assert not isinstance(self.store, Dataset)
        stream = TripleStream(
            options=options,
            producer=producer,
            encoder_class=RDFLibTermEncoder,
        )
        stream.begin()
        for terms in self.store:
            if frame := stream.triple(terms):
                yield frame
        if last := stream.producer.to_stream_frame():
            yield last

    def quads_stream(
        self,
        *,
        producer: FrameProducer | None = None,
        options: StreamOptions,
    ) -> Generator[jelly.RdfStreamFrame]:
        assert isinstance(self.store, Dataset)
        stream = QuadStream(
            options=options,
            producer=producer,
            encoder_class=RDFLibTermEncoder,
        )
        stream.begin()
        for terms in self.store.quads():
            if frame := stream.quad(terms):
                yield frame
        if last := stream.producer.to_stream_frame():
            yield last

    def graphs_stream(
        self,
        *,
        options: StreamOptions,
        producer: FrameProducer | None = None,
    ) -> Generator[jelly.RdfStreamFrame]:
        assert isinstance(self.store, Dataset)
        stream = GraphStream(
            options=options,
            producer=producer,
            encoder_class=RDFLibTermEncoder,
        )
        stream.begin()
        for graph in self.store.graphs():
            yield from stream.graph(graph_id=graph.identifier, graph=graph)
        if last := stream.producer.to_stream_frame():
            yield last

    @override
    def serialize(  # type: ignore[override]
        self,
        output_stream: IO[bytes],
        /,
        *,
        quads: bool = False,
        options: StreamOptions | None = None,
        producer: FrameProducer | None = None,
        **unused: Any,
    ) -> None:
        if options is None:
            options = StreamOptions.big()
        write = write_delimited if options.delimited else write_single
        if isinstance(self.store, Dataset):
            stream = self.quads_stream if quads else self.graphs_stream
        else:
            stream = self.triples_stream

        for frame in stream(options=options, producer=producer):
            write(frame, output_stream)
