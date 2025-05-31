# ruff: noqa: ARG002,FBT001,FBT002

from __future__ import annotations

from collections.abc import Generator, Iterator
from typing import Any
from typing_extensions import TypeAlias

from rdflib.graph import Graph
from rdflib.store import Store
from rdflib.term import Identifier, Node, URIRef

Triple: TypeAlias = tuple[Node, Node, Node]


class OrderedMemory(Store):
    context_aware = True
    graph_aware = True

    def __init__(
        self,
        configuration: str | None = None,
        identifier: Identifier | None = None,
    ) -> None:
        super().__init__(configuration=configuration, identifier=identifier)
        self._quads: list[tuple[Triple, Graph | None]] = []
        self._seen_contexts: dict[str, Graph] = {}
        self._namespaces: dict[str, URIRef] = {}
        self._prefixes: dict[URIRef, str] = {}

    def add(
        self, triple: Triple, context: Graph | None = None, quoted: bool = False
    ) -> None:
        self._quads.append((triple, context))
        if context is not None:
            self._seen_contexts[str(context.identifier)] = context

    def triples(
        self, triple_pattern: Any = None, context: Graph | None = None
    ) -> Iterator[tuple[Triple, Iterator[Graph | None]]]:
        for triple, ctx in self._quads:
            if context is None or context == ctx:
                yield triple, iter((ctx,))

    def contexts(self, triple: Triple | None = None) -> Generator[Graph]:
        if triple is None:
            yield from self._seen_contexts.values()
        else:
            seen_ids = set()
            for t, ctx in self._quads:
                if t == triple and ctx is not None:
                    ctx_id = str(ctx.identifier)
                    if ctx_id not in seen_ids:
                        seen_ids.add(ctx_id)
                        yield ctx

    def add_graph(self, graph: Graph) -> None:
        self._seen_contexts[graph.identifier] = graph

    def remove_graph(self, graph: Graph) -> None:
        self._quads = [(t, ctx) for (t, ctx) in self._quads if ctx != graph]
        self._seen_contexts.pop(graph.identifier, None)

    def __len__(self, context: Graph | None = None) -> int:
        return sum(1 for _, ctx in self._quads if context is None or ctx == context)

    def bind(self, prefix: str, namespace: URIRef, override: bool = True) -> None:
        if override or prefix not in self._namespaces:
            self._namespaces[prefix] = namespace
        if override or namespace not in self._prefixes:
            self._prefixes[namespace] = prefix

    def prefix(self, namespace: URIRef) -> str | None:
        return self._prefixes.get(namespace)

    def namespace(self, prefix: str) -> URIRef | None:
        return self._namespaces.get(prefix)

    def namespaces(self) -> Iterator[tuple[str, URIRef]]:
        return iter(self._namespaces.items())
