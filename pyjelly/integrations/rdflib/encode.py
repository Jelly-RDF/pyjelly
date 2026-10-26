"""
Encoding of rdflib terms. Compiled with mypyc, like the other encoders.

rdflib is not installed when mypyc builds pyjelly, so its classes are only `Any`
to the type checker here.
"""

from __future__ import annotations

from typing import Any, cast

from mypy_extensions import mypyc_attr
from rdflib import BNode, Literal, URIRef
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID

from pyjelly.serialize.encode import EncodedTerm, TermEncoder


def _same_str(a: object, b: object) -> bool:
    """Return whether two optional strings (e.g., URIRefs) are the same strings."""
    if a is b:
        return True
    if a is None or b is None:
        return False
    a_str: str = cast("str", a)
    b_str: str = cast("str", b)
    return a_str == b_str


@mypyc_attr(allow_interpreted_subclasses=True)
class RDFLibTermEncoder(TermEncoder):
    def same_term(self, a: object, b: object) -> bool:
        """
        Return whether two terms are equal, for repeated terms.

        rdflib's __eq__ is slow Python code, so the common cases are answered here
        by comparing the terms as strings:

        - Two URIRefs or two BNodes are equal if their strings are.
        - Two Literals are never equal if their lexical forms differ. If they are
          the same, and so are the language tags and datatypes, they are equal.
          Otherwise rdflib decides (e.g., language tags differ in case only).
        - Terms of different classes among these three are never equal.
        """
        if a is b:
            return True
        cls = type(a)
        other_cls = type(b)
        if cls is other_cls:
            if cls is URIRef or cls is BNode:
                a_str: str = cast("str", a)
                b_str: str = cast("str", b)
                return a_str == b_str
            if cls is Literal:
                a_lex: str = cast("str", a)
                b_lex: str = cast("str", b)
                if a_lex != b_lex:
                    return False
                # The slots behind the language and datatype properties
                a_node: Any = a
                b_node: Any = b
                if _same_str(a_node._language, b_node._language) and _same_str(
                    a_node._datatype, b_node._datatype
                ):
                    return True
        elif (cls is URIRef or cls is BNode or cls is Literal) and (
            other_cls is URIRef or other_cls is BNode or other_cls is Literal
        ):
            return False
        return a == b

    def encode_spo(self, term: object, out: EncodedTerm) -> None:
        """
        Encode s/p/o term based on its RDFLib object.

        Args:
            term (object): term to encode
            out (EncodedTerm): where to put the encoded term.

        """
        # Exact type checks first, as rdflib almost always makes terms of these
        # exact classes: they are cheaper than isinstance(), which is the fallback
        # for subclasses, and runs Python code for rdflib's abstract classes.
        # `node` is Any, because the type checks don't narrow the type when
        # rdflib isn't installed (as during the mypyc compilation).
        node: Any = term
        cls = type(node)
        if cls is URIRef or (
            cls is not Literal and cls is not BNode and isinstance(node, URIRef)
        ):
            # A plain str, so that the IRI cache never calls rdflib's __eq__
            self.encode_iri(str(node), out)
            return

        if cls is Literal or (cls is not BNode and isinstance(node, Literal)):
            # The slots behind the language and datatype properties, which are
            # Python functions
            datatype = node._datatype
            self.encode_literal(
                lex=str(node),
                language=node._language,
                # `datatype` is cast to `str` explicitly because
                # `URIRef.__eq__` overrides `str.__eq__` in an incompatible manner
                datatype=datatype and str(datatype),
                out=out,
            )
            return

        if isinstance(node, BNode):
            self.encode_bnode(str(node), out)
            return

        super().encode_spo(term, out)  # raises NotImplementedError

    def encode_graph(self, term: object, out: EncodedTerm) -> None:
        """
        Encode graph name term based on its RDFLib object.

        Args:
            term (object): term to encode
            out (EncodedTerm): where to put the encoded graph name.

        """
        if self.same_term(term, DATASET_DEFAULT_GRAPH_ID):
            self.encode_default_graph(out)
            return

        # Exact type checks first, see encode_spo()
        cls = type(term)
        if cls is URIRef or (cls is not BNode and isinstance(term, URIRef)):
            self.encode_iri(str(term), out)
            return

        if isinstance(term, BNode):
            self.encode_bnode(str(term), out)
            return
        super().encode_graph(term, out)  # raises NotImplementedError
