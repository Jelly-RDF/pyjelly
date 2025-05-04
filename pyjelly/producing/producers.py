from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import ClassVar
from typing_extensions import override

from pyjelly import jelly


class FrameProducer(metaclass=ABCMeta):
    """
    Abstract base class for producing Jelly frames from RDF stream rows.

    Collects stream rows and assembles them into RdfStreamFrame objects when ready.
    """

    def __init__(self) -> None:
        self._rows: list[jelly.RdfStreamRow] = []

    def add_stream_rows(self, rows: Iterable[jelly.RdfStreamRow]) -> None:
        """Add stream rows to the current batch."""
        self._rows.extend(rows)

    @property
    @abstractmethod
    def stream_frame_ready(self) -> bool:
        """Determine if a new frame should be emitted."""
        raise NotImplementedError

    @property
    @abstractmethod
    def jelly_type(self) -> jelly.LogicalStreamType:
        """Return the logical stream type for this producer."""
        raise NotImplementedError

    def to_stream_frame(self) -> jelly.RdfStreamFrame | None:
        """Return the current frame and clears the row buffer if there are any rows."""
        if not self._rows:
            return None
        frame = jelly.RdfStreamFrame(rows=self._rows)
        self._rows.clear()
        return frame


@dataclass
class ManualFrameProducer(FrameProducer):
    """
    Produces frames only when manually requested (never automatically).

    !!! warning
        All stream rows are kept in memory until `to_stream_frame()` is called.
        This may lead to high memory usage for large streams.

    Used for non-delimited serialization.
    """

    def __init__(
        self,
        *,
        jelly_type: jelly.LogicalStreamType = jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED,
    ) -> None:
        super().__init__()
        self.jelly_type = jelly_type

    @property
    @override
    def stream_frame_ready(self) -> bool:
        """Always returns False; user must manually flush the frame."""
        return False


@dataclass
class FlatFrameProducer(FrameProducer):
    """
    Produces frames automatically when a fixed number of rows is reached.

    Used for delimited encoding (default mode).
    """

    quads: bool
    frame_size: int

    default_frame_size: ClassVar[int] = 250

    def __init__(self, *, quads: bool, frame_size: int | None = None) -> None:
        super().__init__()
        self.quads = quads
        self.frame_size = frame_size or self.default_frame_size

    @property
    @override
    def stream_frame_ready(self) -> bool:
        """Return True when enough rows have been accumulated to emit a frame."""
        return len(self._rows) >= self.frame_size

    @cached_property
    @override
    def jelly_type(self) -> jelly.LogicalStreamType:
        """Determine the logical type based on whether quads are used."""
        return (
            jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS
            if self.quads
            else jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
        )
