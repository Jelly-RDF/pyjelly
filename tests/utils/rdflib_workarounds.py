from __future__ import annotations

import rdflib


def fixup_term(term: rdflib.Node) -> None:
    # workaround #99
    if isinstance(term, rdflib.Literal) and term.datatype == rdflib.XSD.string:
        term._datatype = None


def fixup_graph(graph: rdflib.Graph) -> None:
    for stmt in graph:
        for term in stmt:
            fixup_term(term)
