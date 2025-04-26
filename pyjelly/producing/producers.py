from __future__ import annotations

from abc import ABCMeta
from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import ClassVar
from typing_extensions import override

from pyjelly import jelly


class FrameProducer(metaclass=ABCMeta):
    _rows: list[jelly.RdfStreamRow]

    def __init__(self) -> None:
        self._rows = []

    def add_stream_rows(self, rows: Iterable[jelly.RdfStreamRow]) -> None:
        self._rows.extend(rows)

    @property
    def stream_frame_ready(self) -> bool:
        return False

    @cached_property
    def jelly_type(self) -> jelly.LogicalStreamType:
        return jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES

    def to_stream_frame(self) -> jelly.RdfStreamFrame | None:
        if not self._rows:
            return None
        frame = jelly.RdfStreamFrame(rows=self._rows)
        self._rows.clear()
        return frame


@dataclass
class FlatFrameProducer(FrameProducer):
    _rows: list[jelly.RdfStreamRow]

    def __init__(self, *, quads: bool = False) -> None:
        super().__init__()
        self._quads = quads
        self._rows = []

    @cached_property
    @override
    def jelly_type(self) -> jelly.LogicalStreamType:
        return (
            jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS
            if self._quads
            else jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
        )


@dataclass
class FlatSizedFrameProducer(FlatFrameProducer):
    frame_size: int

    default_frame_size: ClassVar[int] = 250

    def __init__(
        self,
        *,
        quads: bool = False,
        frame_size: int | None = None,
    ) -> None:
        super().__init__(quads=quads)
        self.frame_size = frame_size or self.default_frame_size

    @property
    @override
    def stream_frame_ready(self) -> bool:
        return len(self._rows) >= self.frame_size
