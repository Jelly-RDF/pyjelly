from __future__ import annotations

from collections.abc import Iterable

from pyjelly import jelly
from pyjelly.options import StreamOptions
from pyjelly.producing.encoder import (
    TermEncoder,
    encode_quad,
    encode_triple,
    new_repeated_terms,
)
from pyjelly.producing.producers import FrameProducer


class Stream:
    def __init__(
        self,
        *,
        encoder: TermEncoder,
        producer: FrameProducer,
        physical_type: jelly.PhysicalStreamType,
    ) -> None:
        self.encoder = encoder
        self.producer = producer
        self.physical_type = physical_type
        self.repeated_terms = new_repeated_terms()

    def create_jelly_options(self, options: StreamOptions) -> jelly.RdfStreamOptions:
        return jelly.RdfStreamOptions(
            stream_name=options.stream_name,
            physical_type=self.physical_type,
            generalized_statements=True,
            rdf_star=False,
            max_name_table_size=options.name_lookup_size,
            max_prefix_table_size=options.prefix_lookup_size,
            max_datatype_table_size=options.datatype_lookup_size,
            logical_type=self.producer.jelly_type,
            version=1,
        )

    def begin(self, options: StreamOptions) -> None:
        row = jelly.RdfStreamRow(options=self.create_jelly_options(options))
        self.producer.add_stream_rows((row,))

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
