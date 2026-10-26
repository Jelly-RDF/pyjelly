"""
Adapters that make generic terms and statements from decoded Jelly rows.

Compiled with mypyc, like the decoder that calls them.
"""

from __future__ import annotations

from collections.abc import Iterable
from itertools import chain
from typing import Any
from typing_extensions import override

from mypy_extensions import mypyc_attr

from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.generic.generic_sink import (
    IRI,
    BlankNode,
    DefaultGraph,
    GraphName,
    Literal,
    Prefix,
    Quad,
    Triple,
)
from pyjelly.parse.decode import Adapter, ParserOptions


@mypyc_attr(allow_interpreted_subclasses=True)
class GenericStatementSinkAdapter(Adapter):
    """
    Implement Adapter for generic statements.

    Notes:
        Returns custom RDF terms expected by GenericStatementSink,
        handles namespace declarations, and quoted triples.

    Args:
        Adapter (_type_): base Adapter class

    """

    @override
    def iri(self, iri: str) -> IRI:
        return IRI(iri)

    @override
    def bnode(self, bnode: str) -> BlankNode:
        return BlankNode(bnode)

    @override
    def default_graph(self) -> GraphName:
        return DefaultGraph

    @override
    def literal(
        self,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
    ) -> Literal:
        return Literal(lex, language, datatype)

    @override
    def namespace_declaration(self, name: str, iri: str) -> Prefix:
        return Prefix(name, self.iri(iri))

    @override
    def quoted_triple(self, terms: Iterable[Any]) -> Triple:
        # Faster than Triple(*terms), which runs Triple.__new__ in Python. The
        # decoder always gives the right number of terms
        return tuple.__new__(Triple, terms)


@mypyc_attr(allow_interpreted_subclasses=True)
class GenericTriplesAdapter(GenericStatementSinkAdapter):
    """
    Triples adapted implementation for GenericStatementSink.

    Args:
        GenericStatementSinkAdapter (_type_): base GenericStatementSink
            adapter implementation that handles terms and namespaces.

    """

    def __init__(
        self,
        options: ParserOptions,
    ) -> None:
        super().__init__(options=options)

    @override
    def triple(self, terms: Iterable[Any]) -> Triple:
        return tuple.__new__(Triple, terms)


@mypyc_attr(allow_interpreted_subclasses=True)
class GenericQuadsBaseAdapter(GenericStatementSinkAdapter):
    def __init__(self, options: ParserOptions) -> None:
        super().__init__(options=options)


@mypyc_attr(allow_interpreted_subclasses=True)
class GenericQuadsAdapter(GenericQuadsBaseAdapter):
    """
    Extends GenericQuadsBaseAdapter for QUADS physical type.

    Args:
        GenericQuadsBaseAdapter (_type_): quads adapter that handles
            base quads processing.

    """

    @override
    def quad(self, terms: Iterable[Any]) -> Quad:
        return tuple.__new__(Quad, terms)


@mypyc_attr(allow_interpreted_subclasses=True)
class GenericGraphsAdapter(GenericQuadsBaseAdapter):
    """
    Extends GenericQuadsBaseAdapter for GRAPHS physical type.

    Notes:
        introduces graph start/end, checks if graph exists.

    Args:
        GenericQuadsBaseAdapter (_type_): quads adapter that handles
            base quads processing.

    Raises:
        JellyConformanceError: raised if graph start message was not received.

    """

    _graph_id: GraphName | None

    def __init__(
        self,
        options: ParserOptions,
    ) -> None:
        super().__init__(options=options)
        self._graph_id = None

    @property
    def graph(self) -> None:
        if self._graph_id is None:
            msg = "new graph was not started"
            raise JellyConformanceError(msg)

    @override
    def graph_start(self, graph_id: GraphName) -> None:
        self._graph_id = graph_id

    @override
    def triple(self, terms: Iterable[Any]) -> Quad:
        return Quad(*chain(terms, [self._graph_id]))

    @override
    def graph_end(self) -> None:
        self._graph_id = None
