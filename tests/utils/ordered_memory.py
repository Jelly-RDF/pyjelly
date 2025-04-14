"""An oversimplified RDFLib memory store that stores triples in the order they were inserted."""

from __future__ import annotations

from itertools import repeat
from typing import Any

from rdflib import Graph
from rdflib.store import Store
from rdflib.term import Identifier, Node
from typing_extensions import TypeAlias

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

    def triples(
        self,
        triple_pattern: Any = (None, None, None),
        context: Any = None,
    ) -> Any:
        """Yield all triples in the order they were added, ignoring the pattern and context."""
        return zip(self._triples, repeat(context))

    def add(
        self,
        triple: Triple,
        context: Graph | None = None,
        quoted: bool = False,
    ) -> None:
        """Append a triple to the internal list, preserving order."""
        self._triples.append(triple)
