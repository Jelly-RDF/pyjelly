from __future__ import annotations

import io

from pyjelly import jelly
from pyjelly.integrations.generic.generic_sink import (
    GenericStatementSink,
    Prefix,
)
from pyjelly.integrations.generic.parse import parse_jelly_flat
from pyjelly.integrations.generic.serialize import flat_stream_to_file
from pyjelly.options import LookupPreset
from pyjelly.parse.ioutils import get_options_and_frames
from pyjelly.serialize.flows import FlatQuadsFrameFlow, FlatTriplesFrameFlow
from pyjelly.serialize.streams import SerializerOptions


class GenericSerDes:
    def __init__(self) -> None:
        self.name = "generic"

    def read_quads(self, in_bytes: bytes) -> GenericStatementSink:
        bio = io.BytesIO(in_bytes)
        options, frames = get_options_and_frames(bio)
        sink = GenericStatementSink()
        for item in parse_jelly_flat(inp=bio, frames=frames, options=options):
            if isinstance(item, Prefix):
                sink.bind(item.prefix, item.iri)
            else:
                sink.add(item)
        return sink

    def write_quads(self, in_graph: GenericStatementSink) -> bytes:
        out = io.BytesIO()
        options = SerializerOptions(logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS)
        flat_stream_to_file(in_graph.store, out, options=options)
        return out.getvalue()

    def write_quads_jelly(
        self, in_graph: GenericStatementSink, preset: LookupPreset, frame_size: int
    ) -> bytes:
        out = io.BytesIO()
        options = SerializerOptions(
            flow=FlatQuadsFrameFlow(frame_size=frame_size),
            logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
            lookup_preset=preset,
        )
        flat_stream_to_file(in_graph.store, out, options=options)
        return out.getvalue()

    def read_triples(self, in_bytes: bytes) -> GenericStatementSink:
        bio = io.BytesIO(in_bytes)
        options, frames = get_options_and_frames(bio)
        sink = GenericStatementSink()
        for item in parse_jelly_flat(inp=bio, frames=frames, options=options):
            if isinstance(item, Prefix):
                sink.bind(item.prefix, item.iri)
            else:
                sink.add(item)
        return sink

    def write_triples(self, in_graph: GenericStatementSink) -> bytes:
        out = io.BytesIO()
        options = SerializerOptions(logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES)
        flat_stream_to_file(in_graph.store, out, options=options)
        return out.getvalue()

    def write_triples_jelly(
        self, in_graph: GenericStatementSink, preset: LookupPreset, frame_size: int
    ) -> bytes:
        out = io.BytesIO()
        options = SerializerOptions(
            flow=FlatTriplesFrameFlow(frame_size=frame_size),
            logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
            lookup_preset=preset,
        )
        flat_stream_to_file(in_graph.store, out, options=options)
        return out.getvalue()
