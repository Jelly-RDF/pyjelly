from __future__ import annotations

from collections import UserList
from collections.abc import Iterable
from dataclasses import dataclass
from typing import ClassVar
from typing_extensions import override

from pyjelly import jelly


class FrameFlow(UserList[jelly.RdfStreamRow]):
    """
    Abstract base class for producing Jelly frames from RDF stream rows.

    Collects stream rows and assembles them into RdfStreamFrame objects when ready.
    """

    logical_type: ClassVar[jelly.LogicalStreamType]
    registry: ClassVar[dict[jelly.LogicalStreamType, type[FrameFlow]]] = {}

    def frame_from_bounds(self) -> jelly.RdfStreamFrame | None:
        return None

    def to_stream_frame(self) -> jelly.RdfStreamFrame | None:
        if not self:
            return None
        frame = jelly.RdfStreamFrame(rows=self)
        self.clear()
        return frame

    def __init_subclass__(cls) -> None:
        """
        Register subclasses of FrameFlow with their logical stream type.

        This allows for dynamic dispatch based on the logical stream type.
        """
        if cls.logical_type != jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED:
            cls.registry[cls.logical_type] = cls


class ManualFrameFlow(FrameFlow):
    """
    Produces frames only when manually requested (never automatically).

    !!! warning
        All stream rows are kept in memory until `to_stream_frame()` is called.
        This may lead to high memory usage for large streams.

    Used for non-delimited serialization.
    """

    logical_type = jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED


@dataclass
class BoundedFrameFlow(FrameFlow):
    """
    Produces frames automatically when a fixed number of rows is reached.

    Used for delimited encoding (default mode).
    """

    logical_type = jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED

    frame_size: int
    default_frame_size: ClassVar[int] = 250

    def __init__(
        self,
        initlist: Iterable[jelly.RdfStreamRow] | None = None,
        *,
        frame_size: int | None = None,
    ) -> None:
        super().__init__(initlist)
        self.frame_size = frame_size or self.default_frame_size

    @override
    def frame_from_bounds(self) -> jelly.RdfStreamFrame | None:
        if len(self) >= self.frame_size:
            return self.to_stream_frame()
        return None

    def to_stream_frame(self) -> jelly.RdfStreamFrame | None:
        if not self:
            return None
        to_serialize = self.data[: self.frame_size]
        frame = jelly.RdfStreamFrame(rows=to_serialize)
        self.clear()
        return frame


# Fall back to FlatFrameFlow for unspecified logical types
FrameFlow.registry[jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED] = BoundedFrameFlow


class FlatTriplesFrameFlow(BoundedFrameFlow):
    logical_type = jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES


class FlatQuadsFrameFlow(BoundedFrameFlow):
    logical_type = jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS


class GraphFrameFlow(FrameFlow):
    logical_type = jelly.LOGICAL_STREAM_TYPE_GRAPHS


class DatasetFrameFlow(FrameFlow):
    logical_type = jelly.LOGICAL_STREAM_TYPE_DATASETS
