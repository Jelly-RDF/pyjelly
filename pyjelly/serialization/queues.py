from __future__ import annotations

from collections.abc import Callable
from typing import ClassVar
from typing_extensions import override

from pyjelly.pb2 import rdf_pb2 as pb


class Queue:
    _pb_type: ClassVar[pb.LogicalStreamType] = pb.LOGICAL_STREAM_TYPE_UNSPECIFIED

    rows: list[pb.RdfStreamRow]

    def __init__(self, on_full: Callable[[pb.RdfStreamFrame], None]) -> None:
        self.rows = []
        self.on_full = on_full

    def should_flush(self, *, next_row: pb.RdfStreamRow) -> bool:
        raise NotImplementedError

    def add(self, row: pb.RdfStreamRow | None = None) -> None:
        if row is None:
            return
        if self.should_flush(next_row=row):
            self.flush()
        self.rows.append(row)

    def flush(self) -> None:
        self.on_full(self.clear())

    def clear(self) -> pb.RdfStreamFrame:
        pb_frame = pb.RdfStreamFrame(rows=self.rows)
        self.rows.clear()
        return pb_frame


class FlatTripleQueue(Queue):
    """Stream logic where _elements are RDF triples_."""

    _pb_type = pb.LOGICAL_STREAM_TYPE_FLAT_TRIPLES

    @override
    def should_flush(self, *, next_row: pb.RdfStreamRow) -> bool:
        return len(self.rows) >= 1000


class FlatQuadQueue(Queue):
    """Stream logic where _elements are RDF quads_."""

    _pb_type = pb.LOGICAL_STREAM_TYPE_FLAT_QUADS


class GraphQueue(Queue):
    """Stream logic where _elements are RDF graphs_."""

    _pb_type = pb.LOGICAL_STREAM_TYPE_GRAPHS


class SubjectGraphQueue(Queue):
    """Stream logic where _elements are RDF graphs_ and _each element has a subject node_."""

    _pb_type = pb.LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS


class DatasetQueue(Queue):
    """Stream logic where _elements are RDF datasets_."""

    _pb_type = pb.LOGICAL_STREAM_TYPE_DATASETS


class NamedGraphQueue(Queue):
    """Stream logic where _elements are RDF datasets_ and _each element has exactly one named graph_."""

    _pb_type = pb.LOGICAL_STREAM_TYPE_NAMED_GRAPHS


class TimestampedNamedGraphQueue(Queue):
    """
    Stream logic where _the default graph contains a timestamp triple_.

    Otherwise like [`NamedGraphStreamLogic`][].
    """

    _pb_type = pb.LOGICAL_STREAM_TYPE_TIMESTAMPED_NAMED_GRAPHS
