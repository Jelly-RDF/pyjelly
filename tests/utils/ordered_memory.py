"""An oversimplified RDFLib memory store that stores triples in the insertion order."""

from __future__ import annotations

from collections.abc import Generator
from itertools import repeat
from typing import Any
from typing_extensions import TypeAlias

from rdflib import Graph, URIRef
from rdflib.store import Store
from rdflib.term import Identifier, Node

Triple: TypeAlias = tuple[Node, Node, Node]


class OrderedMemory(Store):
    """
    A simple in-memory RDFLib store that preserves the order of inserted triples.

    Does not support quoted graphs, contexts, or any other RDFLib features.
    Intended for testing serializers that depend on triple order.
    """

    def __init__(
        self,
        configuration: str | None = None,
        identifier: Identifier | None = None,
    ) -> None:
        super().__init__(configuration=configuration, identifier=identifier)
        self._triples: list[Triple] = []
        self._namespaces: dict[str, URIRef] = {}
        self._prefixes: dict[URIRef, str] = {}

    def bind(self, prefix: str, namespace: URIRef, override: bool = True) -> None:  # noqa: ARG002,FBT001,FBT002
        self._namespaces[prefix] = namespace
        self._prefixes[namespace] = prefix

    def namespace(self, prefix: str) -> URIRef | None:
        return self._namespaces.get(prefix, None)

    def prefix(self, namespace: URIRef) -> str | None:
        return self._prefixes.get(namespace, None)

    def namespaces(self) -> Generator[tuple[str, URIRef]]:
        yield from self._namespaces.items()

    def triples(
        self,
        triple_pattern: Any = (None, None, None),  # noqa: ARG002
        context: Any = None,
    ) -> Any:
        """
        Yield all triples in the order they were added.

        Ignores the pattern and context.
        """
        return zip(self._triples, repeat(context))

    def add(
        self,
        triple: Triple,
        context: Graph | None = None,  # noqa: ARG002
        quoted: bool = False,  # noqa: FBT001,FBT002,ARG002
    ) -> None:
        """Append a triple to the internal list, preserving order."""
        self._triples.append(triple)
