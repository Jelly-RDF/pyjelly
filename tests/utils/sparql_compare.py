"""
Reading SPARQL JSON results and comparing results, for Jelly-SPARQL tests.

Two results are equivalent if both are boolean results with the same value, or if
they have the same variables and the same rows in the same order, with a
one-to-one mapping between their blank nodes. Links are not compared.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from rdflib.query import Result

from pyjelly.integrations.generic.generic_sink import (
    IRI,
    BlankNode,
    Literal,
    Node,
    Triple,
)
from pyjelly.parse.sparql import SparqlResultSet
from tests.utils.rdf_compare import (
    Term,
    _BlankNodes,
    _literal,
    generic_term,
    rdflib_term,
)

Row = tuple[Term | None, ...]


@dataclass
class Results:
    """Results, with their terms as in rdf_compare."""

    variables: list[str] = field(default_factory=list)
    rows: list[Row] = field(default_factory=list)
    boolean: bool | None = None


def _json_term(value: dict[str, Any]) -> Term:
    kind = value["type"]
    if kind == "uri":
        return ("iri", value["value"])
    if kind == "bnode":
        return ("bnode", value["value"])
    if kind in ("literal", "typed-literal"):
        return _literal(
            value["value"],
            value.get("xml:lang"),
            value.get("datatype"),
            value.get("its:dir"),
        )
    if kind == "triple":
        triple = value["value"]
        return (
            "triple",
            _json_term(triple["subject"]),
            _json_term(triple["predicate"]),
            _json_term(triple["object"]),
        )
    msg = f"unknown term type: {kind}"
    raise ValueError(msg)


def read_srj(path: Path) -> Results:
    """Read a file of SPARQL JSON results."""
    data = json.loads(path.read_text("utf-8"))
    if "boolean" in data:
        return Results(boolean=bool(data["boolean"]))
    variables: list[str] = data["head"]["vars"]
    rows = [
        tuple(_json_term(b[name]) if name in b else None for name in variables)
        for b in data["results"]["bindings"]
    ]
    return Results(variables, rows)


def generic_node(term: Term) -> Node:
    """Return the term of the generic API for a term."""
    kind = term[0]
    if kind == "iri":
        return IRI(term[1])
    if kind == "bnode":
        return BlankNode(term[1])
    if kind == "literal":
        return Literal(term[1], term[2], term[3], term[4])
    if kind == "triple":
        return Triple(*map(generic_node, term[1:]))
    msg = f"unknown term: {term!r}"
    raise ValueError(msg)


def generic_rows(results: Results) -> list[tuple[Node | None, ...]]:
    """Return the rows with terms of the generic API."""
    return [
        tuple(None if term is None else generic_node(term) for term in row)
        for row in results.rows
    ]


def generic_results(result_set: SparqlResultSet) -> Results:
    """Read a result set of the generic API whole."""
    rows = [
        tuple(None if value is None else generic_term(value) for value in row)
        for row in result_set
    ]
    return Results(result_set.variables, rows, result_set.boolean)


def rdflib_results(result: Result) -> Results:
    if result.type == "ASK":
        return Results(boolean=bool(result.askAnswer))
    variables = result.vars or []
    rows = [
        tuple(
            None if binding.get(v) is None else rdflib_term(binding[v])
            for v in variables
        )
        for binding in result.bindings
    ]
    return Results([str(v) for v in variables], rows)


def assert_same_results(expected: Results, actual: Results, hint: str = "") -> None:
    """Check that two results are equivalent (see the module)."""
    assert actual.boolean == expected.boolean, (
        f"{hint}: expected the boolean result {expected.boolean}, got {actual.boolean}"
    )
    assert actual.variables == expected.variables, (
        f"{hint}: expected the variables {expected.variables}, got {actual.variables}"
    )
    assert len(actual.rows) == len(expected.rows), (
        f"{hint}: expected {len(expected.rows)} rows, got {len(actual.rows)}"
    )
    blank_nodes = _BlankNodes()
    for index, (e, a) in enumerate(zip(expected.rows, actual.rows, strict=True)):
        same = len(e) == len(a) and all(
            x is y if x is None or y is None else blank_nodes.same(x, y)
            for x, y in zip(e, a, strict=True)
        )
        assert same, f"{hint}: row {index} differs: expected {e}, got {a}"


def assert_same_result_sets(
    expected: Iterable[Results], actual: Iterable[Results], hint: str = ""
) -> None:
    expected = list(expected)
    actual = list(actual)
    assert len(actual) == len(expected), (
        f"{hint}: expected {len(expected)} result sets, got {len(actual)}"
    )
    for index, (e, a) in enumerate(zip(expected, actual, strict=True)):
        assert_same_results(e, a, hint=f"{hint}: result set {index}")
