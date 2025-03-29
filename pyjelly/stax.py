from __future__ import annotations

from collections.abc import Callable
from typing import ClassVar
from typing_extensions import override

from pyjelly.contexts import GraphContext, QuadContext, StreamContext, TripleContext
from pyjelly.pb2 import rdf_pb2 as pb


class StreamLogic:
    """Base class for stream logical types implementations."""

    _pb_type: ClassVar[pb.LogicalStreamType] = pb.LOGICAL_STREAM_TYPE_UNSPECIFIED
    _standard_contexts: ClassVar[type[StreamContext] | tuple[type[StreamContext], ...]]

    current_frame: list[pb.RdfStreamRow]

    def __init__(
        self,
        context: StreamContext,
        *,
        flush_callback: Callable[[pb.RdfStreamFrame], object],
        enforce_standard: bool = True,
    ) -> None:
        if enforce_standard and not isinstance(context, self._standard_contexts):
            msg = (
                f"{self._pb_type} logical stream type is only supported for "
                f"{self._standard_contexts}, not {type(context)} "
                "(call with enforce_standard=False to disable this check)"
            )
            raise TypeError(msg)

        self.context = context
        self.current_frame = []
        self.flush_callback = flush_callback

    def should_flush(self, *, next_row: pb.RdfStreamRow) -> bool:
        raise NotImplementedError

    def add_row(self, row: pb.RdfStreamRow, *, autoflush: bool = True) -> None:
        if autoflush and self.should_flush(next_row=row):
            self.flush()
        self.current_frame.append(row)

    def clear(self) -> pb.RdfStreamFrame:
        pb_frame = pb.RdfStreamFrame(rows=self.current_frame)
        self.current_frame.clear()
        return pb_frame

    def flush(self) -> None:
        self.flush_callback(self.clear())


class FlatTripleStreamLogic(StreamLogic):
    """Stream logic where _elements are RDF triples_."""

    _pb_type = pb.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
    _standard_contexts = TripleContext

    @override
    def should_flush(self, *, next_row: pb.RdfStreamRow) -> bool:
        return len(self.current_frame) >= 1000


class FlatQuadStreamLogic(StreamLogic):
    """Stream logic where _elements are RDF quads_."""

    _pb_type = pb.LOGICAL_STREAM_TYPE_FLAT_QUADS
    _standard_contexts = QuadContext, GraphContext


class GraphStreamLogic(StreamLogic):
    """Stream logic where _elements are RDF graphs_."""

    _pb_type = pb.LOGICAL_STREAM_TYPE_GRAPHS
    _standard_contexts = TripleContext


class SubjectGraphStreamLogic(StreamLogic):
    """Stream logic where _elements are RDF graphs_ and _each element has a subject node_."""

    _pb_type = pb.LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS
    _standard_contexts = TripleContext


class DatasetStreamLogic(StreamLogic):
    """Stream logic where _elements are RDF datasets_."""

    _pb_type = pb.LOGICAL_STREAM_TYPE_DATASETS
    _standard_contexts = QuadContext, GraphContext


class NamedGraphStreamLogic(StreamLogic):
    """Stream logic where _elements are RDF datasets_ and _each element has exactly one named graph_."""

    _pb_type = pb.LOGICAL_STREAM_TYPE_NAMED_GRAPHS
    _standard_contexts = QuadContext, GraphContext


class TimestampedNamedGraphStreamLogic(StreamLogic):
    """
    Stream logic where _the default graph contains a timestamp triple_.

    Otherwise like [`NamedGraphStreamLogic`][].
    """

    _pb_type = pb.LOGICAL_STREAM_TYPE_TIMESTAMPED_NAMED_GRAPHS
    _standard_contexts = QuadContext, GraphContext
