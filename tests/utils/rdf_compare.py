"""
Compare lists of statements, as the Jelly conformance tests do.

Two lists are equivalent if they have the same statements in the same order, with
a one-to-one mapping between their blank nodes. A simple literal and an
xsd:string literal with the same lexical form are the same term.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import rdflib
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID

from pyjelly.integrations.generic.generic_sink import (
    IRI,
    BlankNode,
    DefaultGraph,
    GenericStatementSink,
    Literal,
    Triple,
)
from pyjelly.options import STRING_DATATYPE_IRI
from tests.utils.generic_sink_test_parser import GenericSinkParser

# A term is a tuple of its type ("iri", "bnode", "literal", "triple" or "default")
# and what it is made of
Term = tuple[Any, ...]
Statement = tuple[Term, ...]


def _literal(
    lex: str, language: str | None, datatype: str | None, direction: str | None
) -> Term:
    if language:
        return ("literal", lex, language.lower(), None, direction)
    if datatype == STRING_DATATYPE_IRI:
        datatype = None
    return ("literal", lex, None, datatype, None)


def generic_term(term: object) -> Term:
    if isinstance(term, IRI):
        return ("iri", term._iri)
    if isinstance(term, BlankNode):
        return ("bnode", term._id)
    if isinstance(term, Literal):
        return _literal(term._lex, term._langtag, term._datatype, term._direction)
    if isinstance(term, Triple):
        return ("triple", *map(generic_term, term))
    if term is DefaultGraph:
        return ("default",)
    msg = f"unknown term: {term!r}"
    raise TypeError(msg)


def rdflib_term(term: object) -> Term:
    if term == DATASET_DEFAULT_GRAPH_ID:
        return ("default",)
    if isinstance(term, rdflib.Literal):
        datatype = term.datatype and str(term.datatype)
        return _literal(str(term), term.language, datatype, None)
    if isinstance(term, rdflib.URIRef):
        return ("iri", str(term))
    if isinstance(term, rdflib.BNode):
        return ("bnode", str(term))
    if isinstance(term, rdflib.Graph):
        return rdflib_term(term.identifier)
    msg = f"unknown term: {term!r}"
    raise TypeError(msg)


def generic_statements(statements: Iterable[Iterable[object]]) -> list[Statement]:
    return [tuple(map(generic_term, statement)) for statement in statements]


def rdflib_statements(statements: Iterable[Iterable[object]]) -> list[Statement]:
    return [tuple(map(rdflib_term, statement)) for statement in statements]


def rdflib_graph_statements(graph: rdflib.Graph) -> list[Statement]:
    """Return the statements of a Graph or Dataset, in its store's order."""
    if isinstance(graph, rdflib.Dataset):
        return rdflib_statements(graph.quads((None, None, None, None)))
    return rdflib_statements(graph.triples((None, None, None)))


def read_statements(path: Path) -> list[Statement]:
    """Read an N-Triples or N-Quads file of the conformance tests."""
    sink = GenericStatementSink()
    GenericSinkParser(sink).parse(path)
    return generic_statements(sink)


class _BlankNodes:
    """A one-to-one mapping between the blank nodes of two lists."""

    def __init__(self) -> None:
        self.forward: dict[str, str] = {}
        self.backward: dict[str, str] = {}

    def same(self, expected: Term, actual: Term) -> bool:
        if expected[0] != actual[0] or len(expected) != len(actual):
            return False
        if expected[0] == "bnode":
            expected_label: str = expected[1]
            actual_label: str = actual[1]
            mapped = self.forward.setdefault(expected_label, actual_label)
            reverse = self.backward.setdefault(actual_label, expected_label)
            return mapped == actual_label and reverse == expected_label
        if expected[0] == "triple":
            return all(
                self.same(e, a) for e, a in zip(expected[1:], actual[1:], strict=True)
            )
        return bool(expected == actual)


def assert_same_statements(
    expected: list[Statement], actual: list[Statement], hint: str = ""
) -> None:
    """Check that two lists of statements are equivalent (see the module)."""
    assert len(actual) == len(expected), (
        f"{hint}: expected {len(expected)} statements, got {len(actual)}"
    )
    blank_nodes = _BlankNodes()
    for index, (e, a) in enumerate(zip(expected, actual, strict=True)):
        same = len(e) == len(a) and all(
            blank_nodes.same(x, y) for x, y in zip(e, a, strict=True)
        )
        assert same, f"{hint}: statement {index} differs: expected {e}, got {a}"
