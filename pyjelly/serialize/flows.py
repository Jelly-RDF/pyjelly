from __future__ import annotations

from collections import UserList
from collections.abc import Iterable
from dataclasses import dataclass
from typing import ClassVar
from typing_extensions import override

from pyjelly import jelly

DEFAULT_FRAME_SIZE = 250


class FrameFlow(UserList[jelly.RdfStreamRow]):
    """
    Abstract base class for producing Jelly frames from RDF stream rows.

    Collects stream rows and assembles them into RdfStreamFrame objects when ready.
    """

    logical_type: jelly.LogicalStreamType
    registry: ClassVar[dict[jelly.LogicalStreamType, type[FrameFlow]]] = {}

    def frame_from_graph(self) -> jelly.RdfStreamFrame | None:
        """
        Treat the current rows as a graph and produce a frame.

        Default implementation returns None.
        """
        return None

    def frame_from_dataset(self) -> jelly.RdfStreamFrame | None:
        """
        Treat the current rows as a dataset and produce a frame.

        Default implementation returns None.
        """
        return None

    def frame_from_bounds(self) -> jelly.RdfStreamFrame | None:
        return None

    def to_stream_frame(self) -> jelly.RdfStreamFrame | None:
        if not self:
            return None
        frame = jelly.RdfStreamFrame(rows=self)
        self.clear()
        return frame


class ManualFrameFlow(FrameFlow):
    """
    Produces frames only when manually requested (never automatically).

    !!! warning
        All stream rows are kept in memory until `to_stream_frame()` is called.
        This may lead to high memory usage for large streams.

    Used for non-delimited serialization.
    """

    logical_type = jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED

    def __init__(
        self,
        initlist: Iterable[jelly.RdfStreamRow] | None = None,
        *,
        logical_type: jelly.LogicalStreamType = jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED,
    ) -> None:
        super().__init__(initlist)
        self.logical_type = logical_type


@dataclass
class BoundedFrameFlow(FrameFlow):
    """
    Produces frames automatically when a fixed number of rows is reached.

    Used for delimited encoding (default mode).
    """

    logical_type = jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED
    frame_size: int

    def __init__(
        self,
        initlist: Iterable[jelly.RdfStreamRow] | None = None,
        *,
        frame_size: int | None = None,
    ) -> None:
        super().__init__(initlist)
        self.frame_size = frame_size or DEFAULT_FRAME_SIZE

    @override
    def frame_from_bounds(self) -> jelly.RdfStreamFrame | None:
        if len(self) >= self.frame_size:
            return self.to_stream_frame()
        return None


class FlatTriplesFrameFlow(BoundedFrameFlow):
    logical_type = jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES


class FlatQuadsFrameFlow(BoundedFrameFlow):
    logical_type = jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS


class GraphsFrameFlow(FrameFlow):
    logical_type = jelly.LOGICAL_STREAM_TYPE_GRAPHS

    def frame_from_graph(self) -> jelly.RdfStreamFrame | None:
        return self.to_stream_frame()


class DatasetsFrameFlow(FrameFlow):
    logical_type = jelly.LOGICAL_STREAM_TYPE_DATASETS

    def frame_from_dataset(self) -> jelly.RdfStreamFrame | None:
        return self.to_stream_frame()


FLOW_DISPATCH: dict[jelly.LogicalStreamType, type[FrameFlow]] = {
    jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES: FlatTriplesFrameFlow,
    jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS: FlatQuadsFrameFlow,
    jelly.LOGICAL_STREAM_TYPE_GRAPHS: GraphsFrameFlow,
    jelly.LOGICAL_STREAM_TYPE_DATASETS: DatasetsFrameFlow,
}


def flow_for_type(logical_type: jelly.LogicalStreamType) -> type[FrameFlow]:
    try:
        return FLOW_DISPATCH[logical_type]
    except KeyError:
        msg = (
            "unsupported logical stream type: "
            f"{jelly.LogicalStreamType.Name(logical_type)}"
        )
        raise NotImplementedError(msg) from None
