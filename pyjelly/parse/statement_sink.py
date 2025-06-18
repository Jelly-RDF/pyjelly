from typing import Any, Protocol


class StatementSink(Protocol):
    def add(self, statement: Any) -> None: ...

