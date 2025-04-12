from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import ClassVar
from typing_extensions import override

from pyjelly._pb2 import rdf_pb2 as pb


class FrameLogic:
    """
    Base class for RDF stream framing strategies.

    Maintains an internal buffer of `RdfStreamRow`s and controls
    when to flush them into a `RdfStreamFrame`.

    Subclasses define readiness logic and frame type.
    """

    _rows: list[pb.RdfStreamRow]

    def __init__(self) -> None:
        self._rows = []

    def add(self, *rows: pb.RdfStreamRow) -> pb.RdfStreamFrame | None:
        """
        Add rows to the frame buffer and emit a frame if ready.

        Parameters
        ----------
        rows
            One or more `RdfStreamRow` objects.

        Returns
        -------
        RdfStreamFrame or None
            The completed frame, or None if buffer not ready.
        """
        if not rows:
            return None
        self._rows.extend(rows)
        if not self.ready:
            return None
        return self._pack()

    @property
    def ready(self) -> bool:
        """
        Whether the current buffer is ready to be packed into a frame.

        Returns
        -------
        bool
            True if `add()` should return a frame, otherwise False.
        """
        raise NotImplementedError

    @cached_property
    def _protobuf_type(self) -> pb.LogicalStreamType:
        return pb.LOGICAL_STREAM_TYPE_UNSPECIFIED

    def _pack(self) -> pb.RdfStreamFrame:
        """
        Construct and return a `RdfStreamFrame` from the current buffer.

        Returns
        -------
        RdfStreamFrame
            Protobuf frame with accumulated rows.
        """
        frame = pb.RdfStreamFrame(rows=self._rows)
        self._rows.clear()
        return frame


@dataclass
class FlatFrameLogic(FrameLogic):
    """
    Construct and return a `RdfStreamFrame` from the current buffer.

    Returns
    -------
    RdfStreamFrame
        Protobuf frame with accumulated rows.
    """

    default_frame_size: ClassVar[int] = 1000

    frame_size: int
    quads: bool

    def __init__(self, frame_size: int | None = None, *, quads: bool = False) -> None:
        """
        Parameters
        ----------
        frame_size
            Number of rows after which a frame is emitted.
        quads
            If True, emit frames with `LOGICAL_STREAM_TYPE_FLAT_QUADS`.
            Otherwise, emit as flat triples.
        """
        self.frame_size = frame_size or self.default_frame_size
        self.quads = quads

    @cached_property
    def _protobuf_type(self) -> pb.LogicalStreamType:
        return (
            pb.LOGICAL_STREAM_TYPE_FLAT_QUADS
            if self.quads
            else pb.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
        )

    @property
    @override
    def ready(self) -> bool:
        """
        Whether the current buffer has enough rows to emit a frame.

        Returns
        -------
        bool
            True if row count ≥ `frame_size`.
        """
        return len(self._rows) >= self.frame_size
