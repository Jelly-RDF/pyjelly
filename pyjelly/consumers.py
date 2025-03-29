from __future__ import annotations

from pyjelly.contexts import (
    GraphContext,
    PbEntry,
    StreamContext,
)
from pyjelly.pb2 import rdf_pb2 as pb


def consume_entry(context: StreamContext, entry: PbEntry) -> None:
    lookup = context.lookups[type(entry)]
    new, _ = lookup.find(entry.value, compress=True)
    if entry.id != new:
        msg = f"expected {entry} to use identifer {new}"
        raise LookupError(msg)


def consume_graph_start(context: GraphContext, start: pb.RdfGraphStart) -> None:
    context.current_graph = getattr(start, start.WhichOneof("graph"))


def consume_graph_end(context: GraphContext, _: pb.RdfGraphEnd) -> None:
    context.current_graph = None
