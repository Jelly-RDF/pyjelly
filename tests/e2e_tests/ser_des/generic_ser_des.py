from __future__ import annotations

import io

from pyjelly.integrations.generic.generic_sink import (
    GenericStatementSink,
)
from pyjelly.integrations.generic.parse import parse_jelly_to_graph
from pyjelly.integrations.generic.serialize import grouped_stream_to_file, flat_stream_to_file
from pyjelly.options import LookupPreset
from pyjelly.serialize.flows import FlatQuadsFrameFlow, FlatTriplesFrameFlow
from pyjelly.serialize.streams import SerializerOptions


class GenericSerDes:
    def __init__(self) -> None:
        self.name = "generic"

    def read_quads(self, in_bytes: bytes) -> GenericStatementSink:
        return parse_jelly_to_graph(io.BytesIO(in_bytes))

    def write_quads(self, in_graph: GenericStatementSink) -> bytes:
        out = io.BytesIO()
        flat_stream_to_file(in_graph.store, out)
        return out.getvalue()

    def read_quads_jelly(self, in_bytes: bytes) -> GenericStatementSink:
        return self.read_quads(in_bytes)

    def write_quads_jelly(
        self, in_graph: GenericStatementSink, preset: LookupPreset, frame_size: int
    ) -> bytes:
        out = io.BytesIO()
        options = SerializerOptions(
            flow=FlatQuadsFrameFlow(frame_size=frame_size), lookup_preset=preset
        )
        grouped_stream_to_file((g for g in [in_graph]), out, options=options)
        return out.getvalue()

    def read_triples(self, in_bytes: bytes) -> GenericStatementSink:
        return parse_jelly_to_graph(io.BytesIO(in_bytes))

    def write_triples(self, in_graph: GenericStatementSink) -> bytes:
        out = io.BytesIO()
        flat_stream_to_file(in_graph.store, out)
        return out.getvalue()

    def read_triples_jelly(self, in_bytes: bytes) -> GenericStatementSink:
        return self.read_triples(in_bytes)

    def write_triples_jelly(
        self, in_graph: GenericStatementSink, preset: LookupPreset, frame_size: int
    ) -> bytes:
        out = io.BytesIO()
        options = SerializerOptions(
            flow=FlatTriplesFrameFlow(frame_size=frame_size), lookup_preset=preset
        )
        grouped_stream_to_file((g for g in [in_graph]), out, options=options)
        return out.getvalue()
