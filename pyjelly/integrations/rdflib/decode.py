"""
Adapters that make rdflib terms and statements from decoded Jelly rows.

Compiled with mypyc, like the decoder that calls them. rdflib is not installed
when mypyc builds pyjelly, so its classes are only `Any` to the type checker here.
"""

from __future__ import annotations

from collections.abc import Iterable
from itertools import chain
from typing import Any, Final
from typing_extensions import Never, override

import rdflib
from mypy_extensions import mypyc_attr
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from rdflib.term import Variable

from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.rdflib.statements import Prefix, Quad, Triple
from pyjelly.parse.decode import Adapter, ParserOptions

# Bound once: mypyc would look up __new__ on the type at every call
_new_tuple: Final = tuple.__new__
_new_str: Final = str.__new__


@mypyc_attr(allow_interpreted_subclasses=True)
class RDFLibAdapter(Adapter):
    """
    RDFLib adapter class, is extended by triples and quads implementations.

    Args:
        options (ParserOptions): options of the stream
        validate_iris (bool): whether rdflib checks the IRIs. An invalid IRI is
            still read, rdflib only logs a warning. Checking slows down parsing.

    """

    def __init__(self, options: ParserOptions, *, validate_iris: bool = False) -> None:
        super().__init__(options=options)
        self.validate_iris = validate_iris

    @override
    def iri(self, iri: str) -> rdflib.URIRef:
        if self.validate_iris:
            return rdflib.URIRef(iri)
        # The same term, without the check
        return _new_str(rdflib.URIRef, iri)

    @override
    def bnode(self, bnode: str) -> rdflib.BNode:
        # What BNode(bnode) makes, without running its __new__ in Python
        return _new_str(rdflib.BNode, bnode)

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
    def namespace_declaration(self, name: str, iri: rdflib.URIRef) -> Prefix:
        return Prefix(name, iri)

    @override
    def directional_literal(self, lex: str, language: str, direction: str) -> Never:
        msg = (
            "rdflib does not support literals with a base direction (RDF 1.2). "
            "Read the data with the generic API instead."
        )
        raise NotImplementedError(msg)

    @override
    def quoted_triple(self, terms: Iterable[Any]) -> Never:
        msg = (
            "rdflib does not support triple terms (RDF 1.2) and quoted triples "
            "(RDF-star). Read the data with the generic API instead."
        )
        raise NotImplementedError(msg)


@mypyc_attr(allow_interpreted_subclasses=True)
class RDFLibTriplesAdapter(RDFLibAdapter):
    """
    Triples adapter RDFLib implementation.

    Notes: returns triple/namespace declaration as soon as receives them.
    """

    def __init__(self, options: ParserOptions, *, validate_iris: bool = False) -> None:
        super().__init__(options=options, validate_iris=validate_iris)

    @override
    def triple(self, terms: Iterable[Any]) -> Triple:
        # Faster than Triple(*terms), which runs Triple.__new__ in Python. The
        # decoder always gives the right number of terms
        return _new_tuple(Triple, terms)


@mypyc_attr(allow_interpreted_subclasses=True)
class RDFLibQuadsBaseAdapter(RDFLibAdapter):
    def __init__(self, options: ParserOptions, *, validate_iris: bool = False) -> None:
        super().__init__(options=options, validate_iris=validate_iris)


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
        return _new_tuple(Quad, terms)


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

    def __init__(self, options: ParserOptions, *, validate_iris: bool = False) -> None:
        super().__init__(options=options, validate_iris=validate_iris)
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


class BindingRows:
    """
    Row factory that turns the cells of Jelly-SPARQL frames into rdflib bindings.

    A binding is a dict from each bound Variable to its value, as in rdflib's query
    results. Equal names share one Variable object (see variables()).
    """

    def __init__(self) -> None:
        self.names: list[str] = []
        self.vars: list[Variable] = []

    def variables(self, names: list[str]) -> list[Variable]:
        """Get the Variable objects for these names."""
        if names != self.names:
            self.names = names
            self.vars = [Variable(name) for name in names]
        return self.vars

    def __call__(
        self, names: list[str], cells: list[list[Any]], rows: int
    ) -> list[Any]:
        bindings: list[dict[Variable, Any]] = [{} for _ in range(rows)]
        variables = self.variables(names)
        for j in range(len(cells)):
            variable = variables[j]
            # Iterating a list is cheaper than indexing it
            for r, value in enumerate(cells[j]):
                if value is not None:
                    bindings[r][variable] = value
        return bindings
