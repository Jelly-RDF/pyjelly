from __future__ import annotations

import re
from collections import deque
from collections.abc import Generator, Iterable
from pathlib import Path
from typing import Union
from typing_extensions import Self

check_if_iri = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")


class BlankNode:
    def __init__(self, identifier: str) -> None:
        self._identifier: str = identifier

    def __repr__(self) -> str:
        return f"_:{self._identifier}"


class IRI:
    def __init__(self, iri: str) -> None:
        self._iri: str = iri

    def __repr__(self) -> str:
        return f"<{self._iri}>"


class Literal:
    def __init__(self, lex: str, langtag: str | None, datatype: str | None) -> None:
        self._lex: str = lex
        self._langtag: str | None = langtag
        self._datatype: str | None = datatype

    def __repr__(self) -> str:
        suffix = ""
        if self._langtag:
            suffix = f"@{self._langtag}"
        elif self._datatype:
            suffix = f"^^<{self._datatype}>"
        return f'"{self._lex}"{suffix}'


Node = Union[BlankNode, IRI, Literal, "Triple", str]


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
        self._namespaces: dict[str, IRI] = {}

    def add(self, statement: Iterable[Node]) -> None:
        self._store.append(tuple(statement))

    def bind(self, prefix: str, namespace: IRI) -> None:
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
        if isinstance(node, Triple):
            quoted_triple = [self._nt_token(t) for t in node]
            return "<< " + " ".join(quoted_triple) + " >>"
        return str(node)

    def serialize(self, output_filename: Path, encoding: str) -> None:
        with output_filename.open("w", encoding=encoding) as output_file:
            for statement in self._store:
                output_file.write(
                    " ".join(self._nt_token(t) for t in statement) + " .\n"
                )
