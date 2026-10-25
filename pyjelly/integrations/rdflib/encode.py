"""
Encoding of rdflib terms. Compiled with mypyc, like the other encoders.

rdflib is not installed when mypyc builds pyjelly, so its classes are only `Any`
to the type checker here.
"""

from __future__ import annotations

from typing import Any

import rdflib
from mypy_extensions import mypyc_attr
from rdflib import Literal, URIRef
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID

from pyjelly.serialize.encode import HasGraph, Rows, Slot, Statement, TermEncoder


@mypyc_attr(allow_interpreted_subclasses=True)
class RDFLibTermEncoder(TermEncoder):
    def encode_spo(self, term: object, slot: Slot, statement: Statement) -> Rows:
        """
        Encode s/p/o term based on its RDFLib object.

        Args:
            term (object): term to encode
            slot (Slot): its place in statement.
            statement (Statement): Triple/Quad message to fill with s/p/o terms.

        Returns:
            Rows: encoded extra rows

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
            return self.encode_iri(node, iri)

        if type(node) is Literal or isinstance(node, Literal):
            literal = self.get_literal_field(statement, slot)
            return self.encode_literal(
                lex=str(node),
                language=node.language,
                # `datatype` is cast to `str` explicitly because
                # `URIRef.__eq__` overrides `str.__eq__` in an incompatible manner
                datatype=node.datatype and str(node.datatype),
                literal=literal,
            )

        if isinstance(term, rdflib.BNode):
            self.set_bnode_field(statement, slot, str(term))
            return ()

        return super().encode_spo(term, slot, statement)  # error if not handled

    def encode_graph(self, term: object, statement: HasGraph) -> Rows:
        """
        Encode graph name term based on its RDFLib object.

        Args:
            term (object): term to encode
            statement (HasGraph): Quad/GraphStart message to fill g_{} in.

        Returns:
            Rows: encoded extra rows

        """
        if term == DATASET_DEFAULT_GRAPH_ID:
            return self.encode_default_graph(statement.g_default_graph)

        if isinstance(term, rdflib.URIRef):
            return self.encode_iri(term, statement.g_iri)

        if isinstance(term, rdflib.BNode):
            statement.g_bnode = str(term)
            return ()
        return super().encode_graph(term, statement)  # error if not handled
