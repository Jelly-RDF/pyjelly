from __future__ import annotations

from collections import deque
from collections.abc import Generator
from typing import IO, NamedTuple, Union

from mypy_extensions import mypyc_attr


class _DefaultGraph:
    def __repr__(self) -> str:
        return ""


DefaultGraph = _DefaultGraph()


class BlankNode:
    """Class for blank nodes, storing BN's identifier as a string."""

    def __init__(self, identifier: str) -> None:
        # Not `_identifier`: mypyc would name its C field `__identifier`, which
        # is a reserved word for the MSVC compiler
        self._id: str = identifier

    @property
    def identifier(self) -> str:
        """The identifier of the blank node, without the `_:` prefix."""
        return self._id

    def __str__(self) -> str:
        return f"_:{self._id}"

    def __repr__(self) -> str:
        return f"BlankNode(identifier={self._id})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, BlankNode):
            return self._id == other._id
        return False

    def __hash__(self) -> int:
        return hash(self._id)


class IRI:
    """Class for IRIs, storing IRI as a string."""

    def __init__(self, iri: str) -> None:
        self._iri: str = iri

    @property
    def iri(self) -> str:
        """The IRI as a string, without angle brackets."""
        return self._iri

    def __str__(self) -> str:
        return f"<{self._iri}>"

    def __repr__(self) -> str:
        return f"IRI({self._iri})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, IRI):
            return self._iri == other._iri
        return False

    def __hash__(self) -> int:
        return hash(self._iri)


class Literal:
    """
    Class for literals.

    Notes:
        Consists of: lexical form, and optional language tag and datatype.
        A language-tagged string may also have a base direction, "ltr" or "rtl"
        (RDF 1.2). All parts of literal are stored as strings.

    """

    def __init__(
        self,
        lex: str,
        langtag: str | None = None,
        datatype: str | None = None,
        direction: str | None = None,
    ) -> None:
        self._lex: str = lex
        self._langtag: str | None = langtag
        self._datatype: str | None = datatype
        self._direction: str | None = direction

    @property
    def lex(self) -> str:
        """The lexical form, without quotes or escapes."""
        return self._lex

    @property
    def langtag(self) -> str | None:
        """The language tag, or None if there is none."""
        return self._langtag

    @property
    def datatype(self) -> str | None:
        """The datatype IRI as a string, or None if it is not given."""
        return self._datatype

    @property
    def direction(self) -> str | None:
        """The base direction, "ltr" or "rtl", or None if there is none."""
        return self._direction

    def __str__(self) -> str:
        lex = (
            self._lex.replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("\n", "\\n")
            .replace("\r", "\\r")
        )
        suffix = ""
        if self._langtag:
            suffix = f"@{self._langtag}"
            if self._direction:
                suffix += f"--{self._direction}"
        elif self._datatype:
            suffix = f"^^<{self._datatype}>"
        return f'"{lex}"{suffix}'

    def __repr__(self) -> str:
        direction = f", direction={self._direction!r}" if self._direction else ""
        return (
            f"Literal({self._lex!r}, langtag={self._langtag!r}, "
            f"datatype={self._datatype!r}{direction})"
        )

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Literal):
            return (
                self._lex == other._lex
                and self._langtag == other._langtag
                and self._datatype == other._datatype
                and self._direction == other._direction
            )
        return False

    def __hash__(self) -> int:
        return hash((self._lex, self._langtag, self._datatype, self._direction))


Node = Union[BlankNode, IRI, Literal, "Triple"]
GraphName = Node | _DefaultGraph


TRIPLE_ARITY = 3


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
    g: GraphName


class Prefix(NamedTuple):
    """Class for generic namespace declaration."""

    prefix: str
    iri: IRI


@mypyc_attr(allow_interpreted_subclasses=True)
class GenericStatementSink:
    _store: deque[Triple | Quad]

    def __init__(self, identifier: GraphName = DefaultGraph) -> None:
        """
        Initialize statements storage, namespaces dictionary, and parser.

        Notes:
            _store preserves the order of statements.

        Args:
            identifier (str, optional): Identifier for a sink.
                Defaults to DefaultGraph.

        """
        self._store: deque[Triple | Quad] = deque()
        self._namespaces: dict[str, IRI] = {}
        # Not `_identifier`, see BlankNode
        self._id = identifier

    def add(self, statement: Triple | Quad) -> None:
        self._store.append(statement)

    def bind(self, prefix: str, namespace: IRI) -> None:
        self._namespaces.update({prefix: namespace})

    def __iter__(self) -> Generator[Triple | Quad]:
        yield from self._store

    def __len__(self) -> int:
        return len(self._store)

    @property
    def namespaces(self) -> Generator[tuple[str, IRI]]:
        yield from self._namespaces.items()

    @property
    def identifier(self) -> GraphName:
        return self._id

    @property
    def store(self) -> Generator[Triple | Quad]:
        yield from self._store

    @property
    def is_triples_sink(self) -> bool:
        """
        Check if the sink contains triples or quads.

        Returns:
            bool: true, if length of statement is 3.

        """
        return bool(self._store) and len(self._store[0]) == TRIPLE_ARITY

    def parse(self, input_file: IO[bytes]) -> None:
        from pyjelly.integrations.generic.parse import (  # noqa: PLC0415
            parse_jelly_to_graph,
        )

        parsed_result = parse_jelly_to_graph(input_file)
        self._store = parsed_result._store
        self._namespaces = parsed_result._namespaces
        self._id = parsed_result._id

    def serialize(self, output_file: IO[bytes]) -> None:
        from pyjelly.integrations.generic.serialize import (  # noqa: PLC0415
            grouped_stream_to_file,
        )

        grouped_stream_to_file((sink for sink in [self]), output_file)
