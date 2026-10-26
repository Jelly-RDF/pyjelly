"""
Adapters that make rdflib terms and statements from decoded Jelly rows.

Compiled with mypyc, like the decoder that calls them. rdflib is not installed
when mypyc builds pyjelly, so its classes are only `Any` to the type checker here.
"""

from __future__ import annotations

from collections.abc import Iterable
from itertools import chain
from typing import Any
from typing_extensions import override

import rdflib
from mypy_extensions import mypyc_attr
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID

from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.rdflib.statements import Prefix, Quad, Triple
from pyjelly.parse.decode import Adapter, ParserOptions


@mypyc_attr(allow_interpreted_subclasses=True)
class RDFLibAdapter(Adapter):
    """
    RDFLib adapter class, is extended by triples and quads implementations.

    Args:
        Adapter (): abstract adapter class

    """

    @override
    def iri(self, iri: str) -> rdflib.URIRef:
        return rdflib.URIRef(iri)

    @override
    def bnode(self, bnode: str) -> rdflib.BNode:
        return rdflib.BNode(bnode)

    @override
    def default_graph(self) -> rdflib.URIRef:
        return DATASET_DEFAULT_GRAPH_ID

    @override
    def literal(
        self,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
    ) -> rdflib.Literal:
        # normalize=False keeps the lexical form exactly as written. rdflib would
        # otherwise turn it into a Python value and back, which is slow and can
        # change it (e.g., "01" as an xsd:integer would become "1")
        return rdflib.Literal(lex, lang=language, datatype=datatype, normalize=False)

    @override
    def namespace_declaration(self, name: str, iri: str) -> Prefix:
        return Prefix(name, self.iri(iri))


@mypyc_attr(allow_interpreted_subclasses=True)
class RDFLibTriplesAdapter(RDFLibAdapter):
    """
    Triples adapter RDFLib implementation.

    Notes: returns triple/namespace declaration as soon as receives them.
    """

    def __init__(
        self,
        options: ParserOptions,
    ) -> None:
        super().__init__(options=options)

    @override
    def triple(self, terms: Iterable[Any]) -> Triple:
        # Faster than Triple(*terms), which runs Triple.__new__ in Python. The
        # decoder always gives the right number of terms
        return tuple.__new__(Triple, terms)


@mypyc_attr(allow_interpreted_subclasses=True)
class RDFLibQuadsBaseAdapter(RDFLibAdapter):
    def __init__(self, options: ParserOptions) -> None:
        super().__init__(options=options)


@mypyc_attr(allow_interpreted_subclasses=True)
class RDFLibQuadsAdapter(RDFLibQuadsBaseAdapter):
    """
    Extended RDFLib adapter for the QUADS physical type.

    Args:
        RDFLibQuadsBaseAdapter (RDFLibAdapter): base quads adapter
            (shared with graphs physical type)

    """

    @override
    def quad(self, terms: Iterable[Any]) -> Quad:
        return tuple.__new__(Quad, terms)


@mypyc_attr(allow_interpreted_subclasses=True)
class RDFLibGraphsAdapter(RDFLibQuadsBaseAdapter):
    """
    Extension of RDFLibQuadsBaseAdapter for the GRAPHS physical type.

    Notes: introduces graph start/end, checks if graph exists.

    Args:
        RDFLibQuadsBaseAdapter (RDFLibAdapter): base adapter for quads management.

    Raises:
        JellyConformanceError: if no graph_start was encountered

    """

    _graph_id: str | None

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
    def graph_start(self, graph_id: str) -> None:
        self._graph_id = graph_id

    @override
    def triple(self, terms: Iterable[Any]) -> Quad:
        return Quad(*chain(terms, [self._graph_id]))

    @override
    def graph_end(self) -> None:
        self._graph_id = None
