from typing import Any, Protocol


class StatementSink(Protocol):
    def add(self, statement: Any) -> None: ...

    def bind(self, prefix: str, namespace: Any) -> None: ...

    @property
    def store(self) -> Any: ...
