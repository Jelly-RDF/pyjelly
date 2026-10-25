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
