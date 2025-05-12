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

    @property
    def stream_frame_ready(self) -> bool:
        """Determine if a new frame should be emitted."""
        raise NotImplementedError

    def graph_ended(self) -> jelly.RdfStreamFrame | None:
        """Handle the end of a graph; may be overridden by subclasses."""
        return None

    def dataset_ended(self) -> jelly.RdfStreamFrame | None:
        """Handle the end of a dataset; may be overridden by subclasses."""
        return None

    def to_stream_frame(self) -> jelly.RdfStreamFrame | None:
        """Return the current frame and clears the row buffer if there are any rows."""
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

    @property
    @override
    def stream_frame_ready(self) -> bool:
        """Always returns False; user must manually flush the frame."""
        return False


@dataclass
class FlatFrameFlow(FrameFlow):
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

    @property
    @override
    def stream_frame_ready(self) -> bool:
        """Return True when enough rows have been accumulated to emit a frame."""
        return len(self) >= self.frame_size


# Fall back to FlatFrameFlow for unspecified logical types
FrameFlow.registry[jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED] = FlatFrameFlow


class FlatTriplesFrameFlow(FlatFrameFlow):
    logical_type = jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES


class FlatQuadsFrameFlow(FlatFrameFlow):
    logical_type = jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS


class GraphFrameFlow(FrameFlow):
    logical_type = jelly.LOGICAL_STREAM_TYPE_GRAPHS

    @override
    def graph_ended(self) -> jelly.RdfStreamFrame | None:
        return self.to_stream_frame()


class DatasetFrameFlow(FrameFlow):
    logical_type = jelly.LOGICAL_STREAM_TYPE_DATASETS

    @override
    def dataset_ended(self) -> jelly.RdfStreamFrame | None:
        return self.to_stream_frame()
