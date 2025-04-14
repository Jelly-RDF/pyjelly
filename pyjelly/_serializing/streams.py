from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import ClassVar
from typing_extensions import override

from pyjelly._pb2 import rdf_pb2 as pb


class Stream:
    """
    Base class for RDF stream framing strategies.

    Maintains an internal buffer of `RdfStreamRow`s and controls
    when to emit it in a `RdfStreamFrame`.

    Subclasses define readiness logic and frame type.
    """

    _rows: list[pb.RdfStreamRow]

    def __init__(self) -> None:
        self._rows = []

    def add_row(self, *rows: pb.RdfStreamRow) -> pb.RdfStreamFrame | None:
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
        if not self.frame_ready:
            return None
        return self.make_frame()

    @property
    def frame_ready(self) -> bool:
        """
        Whether the current row list is ready to be emitted in a frame.

        Returns
        -------
        bool
            True if `add()` should return a frame, otherwise False.

        """
        raise NotImplementedError

    @cached_property
    def protobuf_type(self) -> pb.LogicalStreamType:
        return pb.LOGICAL_STREAM_TYPE_UNSPECIFIED

    def make_frame(self) -> pb.RdfStreamFrame:
        """
        Construct and return a `RdfStreamFrame` from the current row list.

        Returns
        -------
        RdfStreamFrame
            Protobuf frame with accumulated rows.

        """
        frame = pb.RdfStreamFrame(rows=self._rows)
        self._rows.clear()
        return frame


@dataclass
class FlatStream(Stream):
    """
    Construct and return a `RdfStreamFrame` from the current buffer.

    Parameters
    ----------
    frame_size
        Number of rows after which a frame is emitted.
    quads
        If True, emit frames with `LOGICAL_STREAM_TYPE_FLAT_QUADS`.
        Otherwise, emit as flat triples.

    Returns
    -------
    RdfStreamFrame
        Protobuf frame with accumulated rows.

    """

    frame_size: int
    quads: bool

    default_frame_size: ClassVar[int] = 250

    def __init__(self, frame_size: int | None = None, *, quads: bool = False) -> None:
        super().__init__()
        self.frame_size = frame_size or self.default_frame_size
        self.quads = quads

    @cached_property
    def protobuf_type(self) -> pb.LogicalStreamType:
        return (
            pb.LOGICAL_STREAM_TYPE_FLAT_QUADS
            if self.quads
            else pb.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
        )

    @property
    @override
    def frame_ready(self) -> bool:
        """
        Whether the current buffer has enough rows to emit a frame.

        Returns
        -------
        bool
            True if row count ≥ `frame_size`.

        """
        return len(self._rows) >= self.frame_size
