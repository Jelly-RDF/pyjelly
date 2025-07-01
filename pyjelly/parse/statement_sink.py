from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class StatementSink(Protocol):
    _store: Any

    def add(self, statement: Any) -> None: ...

    def bind(
        self,
        prefix: str | None,
        namespace: Any,
    ) -> None: ...

    def __iter__(self) -> Iterator[Any]: ...
