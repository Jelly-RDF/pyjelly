from __future__ import annotations

from collections.abc import Generator, Iterable
from typing import Any, ClassVar

from pyjelly import jelly
from pyjelly.options import StreamOptions, validate_type_compatibility
from pyjelly.producing.encoder import (
    Slot,
    TermEncoder,
    encode_namespace_declaration,
    encode_quad,
    encode_triple,
    new_repeated_terms,
)
from pyjelly.producing.producers import FlatFrameProducer, FrameProducer


class Stream:
    physical_type: ClassVar[jelly.PhysicalStreamType]

    def __init__(
        self,
        *,
        options: StreamOptions,
        encoder_class: type[TermEncoder],
        producer: FrameProducer | None = None,
    ) -> None:
        self.options = options
        self.encoder = encoder_class(
            prefix_lookup_size=options.prefix_lookup_size,
            name_lookup_size=options.name_lookup_size,
            datatype_lookup_size=options.datatype_lookup_size,
        )
        self.producer = producer or self.create_default_producer()
        validate_type_compatibility(self.physical_type, self.producer.jelly_type)
        self.repeated_terms = new_repeated_terms()

    def create_default_producer(self) -> FrameProducer:
        quads = self.physical_type != jelly.PHYSICAL_STREAM_TYPE_TRIPLES
        return FlatFrameProducer(quads=quads)

    def calculate_version(self) -> int:
        return 1

    def encode_options(self) -> jelly.RdfStreamOptions:
        return jelly.RdfStreamOptions(
            stream_name=self.options.stream_name,
            physical_type=self.physical_type,
            generalized_statements=self.options.generalized_statements,
            rdf_star=self.options.rdf_star,
            max_name_table_size=self.options.name_lookup_size,
            max_prefix_table_size=self.options.prefix_lookup_size,
            max_datatype_table_size=self.options.datatype_lookup_size,
            logical_type=self.producer.jelly_type,
            version=self.options.version,
        )

    def begin(self) -> None:
        row = jelly.RdfStreamRow(options=self.encode_options())
        self.producer.add_stream_rows((row,))

    def namespace_declaration(self, name: str, iri: str) -> None:
        rows = encode_namespace_declaration(
            name=name,
            value=iri,
            term_encoder=self.encoder,
        )
        self.producer.add_stream_rows(rows)


class TripleStream(Stream):
    physical_type = jelly.PHYSICAL_STREAM_TYPE_TRIPLES

    def triple(self, terms: Iterable[object]) -> jelly.RdfStreamFrame | None:
        new_rows = encode_triple(
            terms,
            term_encoder=self.encoder,
            repeated_terms=self.repeated_terms,
        )
        self.producer.add_stream_rows(new_rows)
        if self.producer.stream_frame_ready:
            return self.producer.to_stream_frame()
        return None


class QuadStream(Stream):
    physical_type = jelly.PHYSICAL_STREAM_TYPE_QUADS

    def quad(self, terms: Iterable[object]) -> jelly.RdfStreamFrame | None:
        new_rows = encode_quad(
            terms,
            term_encoder=self.encoder,
            repeated_terms=self.repeated_terms,
        )
        self.producer.add_stream_rows(new_rows)
        if self.producer.stream_frame_ready:
            return self.producer.to_stream_frame()
        return None


class GraphStream(TripleStream):
    physical_type = jelly.PHYSICAL_STREAM_TYPE_GRAPHS

    def graph(
        self,
        graph_id: object,
        graph: Iterable[Iterable[object]],
    ) -> Generator[jelly.RdfStreamFrame]:
        [*graph_rows], graph_node = self.encoder.encode_any(graph_id, Slot.graph)
        kw_name = f"{Slot.graph}_{self.encoder.TERM_ONEOF_NAMES[type(graph_node)]}"
        kws: dict[Any, Any] = {kw_name: graph_node}
        start_row = jelly.RdfStreamRow(graph_start=jelly.RdfGraphStart(**kws))
        graph_rows.append(start_row)
        self.producer.add_stream_rows(graph_rows)
        for triple in graph:
            if frame := self.triple(triple):
                yield frame
        end_row = jelly.RdfStreamRow(graph_end=jelly.RdfGraphEnd())
        self.producer.add_stream_rows((end_row,))
        if self.producer.stream_frame_ready:
            yield self.producer.to_stream_frame()  # type: ignore[misc]
