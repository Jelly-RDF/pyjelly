# ruff: noqa: D205,D400,D415
from __future__ import annotations

import rdflib


def fixup_term(term: rdflib.Node) -> None:
    """
    >>> l1 = rdflib.Literal("foo", datatype=rdflib.XSD.string)
    >>> l2 = rdflib.Literal("foo")
    >>> l1 == l2
    False
    >>> fixup_term(l1)
    >>> fixup_term(l2)
    >>> l1 == l2
    True
    """
    # workaround #99
    if isinstance(term, rdflib.Literal) and term.datatype == rdflib.XSD.string:
        term._datatype = None


def fixup_graph(graph: rdflib.Graph) -> None:
    for stmt in graph:
        for term in stmt:
            fixup_term(term)
