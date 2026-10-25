"""
Encoding of rdflib terms. Compiled with mypyc, like the other encoders.

rdflib is not installed when mypyc builds pyjelly, so its classes are only `Any`
to the type checker here.
"""

from __future__ import annotations

from typing import Any, cast

import rdflib
from mypy_extensions import mypyc_attr
from rdflib import Literal, URIRef
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID

from pyjelly.serialize.encode import HasGraph, Slot, Statement, TermEncoder


@mypyc_attr(allow_interpreted_subclasses=True)
class RDFLibTermEncoder(TermEncoder):
    def same_term(self, a: object, b: object) -> bool:
        """
        Return whether two terms are equal, for repeated terms.

        rdflib's __eq__ is slow Python code. For two terms of the same exact class
        URIRef or BNode it only compares their strings, so this does that directly.
        """
        if a is b:
            return True
        cls = type(a)
        if cls is type(b) and (cls is URIRef or cls is rdflib.BNode):
            # Typed as str, compiled to a plain string comparison
            a_str: str = cast("str", a)
            b_str: str = cast("str", b)
            return a_str == b_str
        return a == b

    def encode_spo(self, term: object, slot: Slot, statement: Statement) -> None:
        """
        Encode s/p/o term based on its RDFLib object.

        Args:
            term (object): term to encode
            slot (Slot): its place in statement.
            statement (Statement): Triple/Quad message to fill with s/p/o terms.

        """
        # Exact type checks first, as rdflib almost always makes terms of these
        # exact classes: they are cheaper than isinstance(), which is the fallback
        # for subclasses. `node` is Any, because the type checks don't narrow
        # the type when rdflib isn't installed (as during the mypyc compilation).
        node: Any = term
        if type(node) is URIRef or (
            type(node) is not Literal and isinstance(node, URIRef)
        ):
            iri = self.get_iri_field(statement, slot)
            # A plain str, so that the IRI cache never calls rdflib's __eq__
            self.encode_iri(str(node), iri)
            return

        if type(node) is Literal or isinstance(node, Literal):
            literal = self.get_literal_field(statement, slot)
            self.encode_literal(
                lex=str(node),
                language=node.language,
                # `datatype` is cast to `str` explicitly because
                # `URIRef.__eq__` overrides `str.__eq__` in an incompatible manner
                datatype=node.datatype and str(node.datatype),
                literal=literal,
            )
            return

        if isinstance(term, rdflib.BNode):
            self.set_bnode_field(statement, slot, str(term))
            return

        super().encode_spo(term, slot, statement)  # raises: not handled

    def encode_graph(self, term: object, statement: HasGraph) -> None:
        """
        Encode graph name term based on its RDFLib object.

        Args:
            term (object): term to encode
            statement (HasGraph): Quad/GraphStart message to fill g_{} in.

        """
        if term == DATASET_DEFAULT_GRAPH_ID:
            self.encode_default_graph(statement.g_default_graph)
            return

        if isinstance(term, rdflib.URIRef):
            self.encode_iri(str(term), statement.g_iri)
            return

        if isinstance(term, rdflib.BNode):
            statement.g_bnode = str(term)
            return
        super().encode_graph(term, statement)  # raises: not handled
