from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import ClassVar
from typing_extensions import override

from pyjelly import jelly


class Producer:
    _rows: list[jelly.RdfStreamRow]

    def __init__(self) -> None:
        self._rows = []

    def add_rows(
        self,
        rows: Iterable[jelly.RdfStreamRow],
    ) -> jelly.RdfStreamFrame | None:
        if not rows:
            return None
        self._rows.extend(rows)
        if not self.new_frame_ready:
            return None
        return self.to_frame()

    @property
    def new_frame_ready(self) -> bool:
        return False

    @cached_property
    def jelly_type(self) -> jelly.LogicalStreamType:
        return jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED

    def to_frame(self) -> jelly.RdfStreamFrame | None:
        if not self._rows:
            return None
        frame = jelly.RdfStreamFrame(rows=self._rows)
        self._rows.clear()
        return frame


@dataclass
class FlatProducer(Producer):
    frame_size: int
    quads: bool

    default_frame_size: ClassVar[int] = 250

    def __init__(self, frame_size: int | None = None, *, quads: bool = False) -> None:
        super().__init__()
        self.frame_size = frame_size or self.default_frame_size
        self.quads = quads

    @cached_property
    @override
    def jelly_type(self) -> jelly.LogicalStreamType:
        return (
            jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS
            if self.quads
            else jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
        )

    @property
    @override
    def new_frame_ready(self) -> bool:
        return len(self._rows) >= self.frame_size
