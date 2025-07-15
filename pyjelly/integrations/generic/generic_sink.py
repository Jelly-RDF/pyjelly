from __future__ import annotations

from collections import deque
from collections.abc import Generator, Iterable
from pathlib import Path
from typing import NamedTuple, Union


class BlankNode:
    """Class for blank nodes, storing BN's identifier as a string."""

    def __init__(self, identifier: str) -> None:
        self._identifier: str = identifier

    def __repr__(self) -> str:
        return f"_:{self._identifier}"


class IRI:
    """Class for IRIs, storing IRI as a string."""

    def __init__(self, iri: str) -> None:
        self._iri: str = iri

    def __repr__(self) -> str:
        return f"<{self._iri}>"


class Literal:
    """
    Class for literals.

    Notes:
        Consists of: lexical form, and optional language tag and datatype.
        All parts of literal are stored as strings.

    """

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


class Triple(NamedTuple):
    """Class for RDF triples."""

    s: Node
    p: Node
    o: Node


class Quad(NamedTuple):
    """Class for RDF quads."""

    s: Node
    p: Node
    o: Node
    g: Node


class Prefix(NamedTuple):
    """Class for generic namespace declaration."""

    prefix: str
    iri: IRI


class GenericStatementSink:
    _store: deque[tuple[Node, ...]]

    def __init__(self) -> None:
        """
        Initialize statements storage and namespaces dictionary.

        Notes:
            _store preserves the order of statements.

        """
        self._store: deque[tuple[Node, ...]] = deque()
        self._namespaces: dict[str, IRI] = {}

    def add(self, statement: Iterable[Node]) -> None:
        self._store.append(tuple(statement))

    def bind(self, prefix: str, namespace: IRI) -> None:
        self._namespaces.update({prefix: namespace})

    def __iter__(self) -> Generator[tuple[Node, ...]]:
        yield from self._store

    @property
    def namespaces(self) -> Generator[tuple[str, IRI]]:
        yield from self._namespaces.items()

    @property
    def is_triples_sink(self) -> bool:
        """
        Check if the sink contains triples or quads.

        Returns:
            bool: true, if length of statement is 3.

        """
        triples_arity = 3
        return len(self._store[0]) == triples_arity

    def _serialize_node(self, node: Node) -> str:
        """
        Serialize node to its string representation.

        Args:
            node (Node): Node to convert - RDF term, str, or Triple.

        Returns:
            str: string representation of Node.

        """
        if isinstance(node, Triple):
            quoted_triple = [self._serialize_node(t) for t in node]
            return "<< " + " ".join(quoted_triple) + " >>"
        return str(node)

    def serialize(self, output_filename: Path, encoding: str = "utf-8") -> None:
        """
        Serialize sink's store content to a simple N-triples/N-quads format.

        Args:
            output_filename (Path): path to the output file.
            encoding (str): encoding of output. Defaults to utf-8.

        """
        with output_filename.open("w", encoding=encoding) as output_file:
            for statement in self._store:
                output_file.write(
                    " ".join(self._serialize_node(t) for t in statement) + " .\n"
                )
