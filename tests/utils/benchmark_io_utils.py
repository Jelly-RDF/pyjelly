from __future__ import annotations

import io


class NullCounter(io.RawIOBase):
    def __init__(self) -> None:
        self.n = 0

    def writable(self) -> bool:
        return True

    def write(self, b: bytes) -> int:  # type: ignore[override]
        ln = len(b)
        self.n += ln
        return ln
