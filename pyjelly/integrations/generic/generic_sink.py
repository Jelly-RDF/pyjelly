from __future__ import annotations

import re
from collections import deque
from collections.abc import Generator, Iterable
from pathlib import Path
from typing import Union
from typing_extensions import Self

Node = Union[str, "Triple"]
check_if_iri = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")


class Triple(tuple[Node, Node, Node]):
    __slots__ = ()

    def __new__(cls, s: Node, p: Node, o: Node) -> Self:
        return tuple.__new__(cls, (s, p, o))

    @property
    def s(self) -> Node:
        return self[0]

    @property
    def p(self) -> Node:
        return self[1]

    @property
    def o(self) -> Node:
        return self[2]


class Quad(tuple[Node, Node, Node, Node]):
    __slots__ = ()

    def __new__(cls, s: Node, p: Node, o: Node, g: Node) -> Self:
        return tuple.__new__(cls, (s, p, o, g))

    @property
    def s(self) -> Node:
        return self[0]

    @property
    def p(self) -> Node:
        return self[1]

    @property
    def o(self) -> Node:
        return self[2]

    @property
    def g(self) -> Node:
        return self[3]


class GenericStatementSink:
    _store: deque[tuple[Node, ...]]

    def __init__(self) -> None:
        self._store: deque[tuple[Node, ...]] = deque()
        self._namespaces: dict[Node, Node] = {}

    def add(self, statement: Iterable[Node]) -> None:
        self._store.append(tuple(statement))

    def bind(self, prefix: str, namespace: str) -> None:
        self._namespaces.update({prefix: namespace})

    def __iter__(self) -> Generator[tuple[Node, ...]]:
        yield from self._store

    def __len__(self) -> int:
        return len(self._store)

    @property
    def is_triples_sink(self) -> bool:
        triples_arity = 3
        return len(self._store[0]) == triples_arity

    def _nt_token(self, node: Node) -> str:
        if isinstance(node, str) and node.startswith("_:"):
            return node
        if isinstance(node, Triple):
            quoted_triple = [self._nt_token(t) for t in node]
            return "<< " + " ".join(quoted_triple) + " >>"
        if isinstance(node, str) and bool(check_if_iri.match(node)):
            return f"<{node}>"
        return node

    def serialize(self, output_filename: Path, encoding: str) -> None:
        with output_filename.open("w", encoding=encoding) as output_file:
            for statement in self._store:
                output_file.write(
                    " ".join(self._nt_token(t) for t in statement) + " .\n"
                )
