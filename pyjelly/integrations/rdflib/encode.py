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

from pyjelly.serialize.encode import EncodedTerm, TermEncoder


@mypyc_attr(allow_interpreted_subclasses=True)
class RDFLibTermEncoder(TermEncoder):
    def same_term(self, a: object, b: object) -> bool:
        """
        Return whether two terms are equal, for repeated terms.

        rdflib's __eq__ is slow Python code, so the common cases are answered here
        with plain string comparisons (typed as str, so mypyc compiles them to C):

        - Two URIRefs or two BNodes are equal if their strings are.
        - Two Literals are never equal if their lexical forms differ. If they are
          the same, the language tag and datatype matter too: rdflib decides.
        - Terms of different classes among these three are never equal.
        """
        if a is b:
            return True
        cls = type(a)
        other_cls = type(b)
        if cls is other_cls:
            if cls is URIRef or cls is rdflib.BNode:
                a_str: str = cast("str", a)
                b_str: str = cast("str", b)
                return a_str == b_str
            if cls is Literal:
                a_lex: str = cast("str", a)
                b_lex: str = cast("str", b)
                if a_lex != b_lex:
                    return False
        elif (cls is URIRef or cls is rdflib.BNode or cls is Literal) and (
            other_cls is URIRef or other_cls is rdflib.BNode or other_cls is Literal
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
        # for subclasses. `node` is Any, because the type checks don't narrow
        # the type when rdflib isn't installed (as during the mypyc compilation).
        node: Any = term
        if type(node) is URIRef or (
            type(node) is not Literal and isinstance(node, URIRef)
        ):
            # A plain str, so that the IRI cache never calls rdflib's __eq__
            self.encode_iri(str(node), out)
            return

        if type(node) is Literal or isinstance(node, Literal):
            self.encode_literal(
                lex=str(node),
                language=node.language,
                # `datatype` is cast to `str` explicitly because
                # `URIRef.__eq__` overrides `str.__eq__` in an incompatible manner
                datatype=node.datatype and str(node.datatype),
                out=out,
            )
            return

        if isinstance(term, rdflib.BNode):
            self.encode_bnode(str(term), out)
            return

        super().encode_spo(term, out)  # raises: not handled

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

        if isinstance(term, rdflib.URIRef):
            self.encode_iri(str(term), out)
            return

        if isinstance(term, rdflib.BNode):
            self.encode_bnode(str(term), out)
            return
        super().encode_graph(term, out)  # raises: not handled
