"""
The statements and prefixes that the rdflib parser yields.

Not compiled with mypyc, which can't compile subclasses of tuple.
"""

from __future__ import annotations

from typing import TypeAlias
from typing_extensions import Self

import rdflib
from rdflib import BNode, Node, URIRef

GraphName: TypeAlias = URIRef | BNode | str


class Triple(tuple[Node, Node, Node]):
    """
    Describe RDFLib triple.

    Args:
        tuple (Node, Node, Node): s/p/o tuple of RDFLib Nodes.

    Returns:
        Triple: triple as tuple.

    """

    __slots__ = ()

    def __new__(cls, s: Node, p: Node, o: Node) -> Self:
        return tuple.__new__(cls, (s, p, o))

    @property
    def s(self) -> Node:
        return self[0]

    @property
    def p(self) -> Node:
        return self[1]

    @property
    def o(self) -> Node:
        return self[2]


class Quad(tuple[Node, Node, Node, GraphName]):
    """
    Describe RDFLib quad.

    Args:
        tuple (Node, Node, Node, GraphName):
            s/p/o/g as a tuple of RDFLib nodes and a GraphName,

    Returns:
        Quad: quad as tuple.

    """

    __slots__ = ()

    def __new__(cls, s: Node, p: Node, o: Node, g: GraphName) -> Self:
        return tuple.__new__(cls, (s, p, o, g))

    @property
    def s(self) -> Node:
        return self[0]

    @property
    def p(self) -> Node:
        return self[1]

    @property
    def o(self) -> Node:
        return self[2]

    @property
    def g(self) -> GraphName:
        return self[3]


Statement = Triple | Quad


class Prefix(tuple[str, rdflib.URIRef]):
    """
    Describe RDF Prefix(i.e, namespace declaration).

    Args:
        tuple (str, rdflib.URIRef): expects prefix as a string,
            and full namespace URI as Rdflib.URIRef.

    Returns:
        Prefix: prefix as tuple(prefix, iri).

    """

    __slots__ = ()

    def __new__(cls, prefix: str, iri: rdflib.URIRef) -> Self:
        return tuple.__new__(cls, (prefix, iri))

    @property
    def prefix(self) -> str:
        return self[0]

    @property
    def iri(self) -> rdflib.URIRef:
        return self[1]
