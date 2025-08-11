from __future__ import annotations

import io

from pyjelly.integrations.generic.generic_sink import (
    DEFAULT_GRAPH_IDENTIFIER,
    GenericStatementSink,
    Quad,
    Triple,
)
from pyjelly.integrations.generic.parse import parse_jelly_to_graph
from pyjelly.integrations.generic.serialize import grouped_stream_to_file
from pyjelly.options import LookupPreset
from pyjelly.serialize.flows import FlatQuadsFrameFlow, FlatTriplesFrameFlow
from pyjelly.serialize.streams import SerializerOptions

# Could inherit from abstaction from BaseSerDes but it would require ugly casting


class GenericSerDes:
    def __init__(self) -> None:
        self.name = "generic"

    def read_quads(self, in_bytes: bytes) -> GenericStatementSink:
        return parse_jelly_to_graph(io.BytesIO(in_bytes))

    def write_quads(self, in_graph: GenericStatementSink) -> bytes:
        out = io.BytesIO()
        grouped_stream_to_file((g for g in [in_graph]), out)
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
        sink = parse_jelly_to_graph(io.BytesIO(in_bytes))
        if len(sink) == 0 or sink.is_triples_sink:
            return sink
        t_only = GenericStatementSink()
        for st in sink.store:
            if isinstance(st, Triple):
                t_only.add(st)
            elif isinstance(st, Quad) and st.g == DEFAULT_GRAPH_IDENTIFIER:
                t_only.add(Triple(st.s, st.p, st.o))
        return t_only

    def write_triples(self, in_graph: GenericStatementSink) -> bytes:
        out = io.BytesIO()
        grouped_stream_to_file((g for g in [in_graph]), out)
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
