from __future__ import annotations

from itertools import chain
from typing import IO
from typing_extensions import override

import google.protobuf.proto as protolib  # type: ignore[import-not-found]
import rdflib
from rdflib.graph import Dataset, Graph, QuotedGraph
from rdflib.serializer import Serializer as RDFLibSerializer

from pyjelly._serializing import encoders, lookups, streams


class RDFLibJellySerializer(RDFLibSerializer):
    """
    RDFLib serializer for writing graphs in Jelly RDF stream format.

    Handles streaming RDF terms into Jelly frames using internal encoders.
    Supports only graphs and datasets (not quoted graphs).

    Parameters
    ----------
    store
        RDFLib Graph or Dataset. QuotedGraph is not supported.

    Raises
    ------
    NotImplementedError
        If the store is a QuotedGraph.

    """

    def __init__(self, store: Graph) -> None:
        if isinstance(store, QuotedGraph):
            msg = "N3 format is not supported"
            raise NotImplementedError(msg)
        super().__init__(store)

    @override
    def serialize(  # type: ignore[override]
        self,
        stream: IO[bytes],
        base: str | None = None,
        encoding: str | None = None,
        *,
        quads: bool | None = None,
        options: lookups.Options | None = None,
    ) -> None:
        """
        Serialize RDFLib graph to Jelly format.

        Parameters
        ----------
        stream
            Output byte stream.
        base
            Base IRI (unused).
        encoding
            Character encoding (unused).
        quads
            Whether to serialize as RDF quads. Required for datasets.
        options
            Jelly serialization options.

        Raises
        ------
        ValueError
            If the graph is a dataset and `quads` is not specified.
        NotImplementedError
            If quad serialization is requested (not yet implemented).
        TypeError
            If an RDF term has unsupported type.

        """
        if isinstance(self.store, Dataset) and quads is None:
            msg = (
                "serialized store has multiple graphs"
                "but quads was not set to True or False"
            )
            raise ValueError(msg)

        if quads is not None:
            msg = "multiple graph serialization is not implemented"
            raise NotImplementedError(msg)

        logic = streams.FlatStream()
        encoder = encoders.TripleEncoder(options)
        encoder.encode_options(frame_logic=logic)

        for term in chain.from_iterable(self.store):
            if not encoder.is_repeated(term):
                if isinstance(term, rdflib.URIRef):
                    encoder.encode_iri(term)
                elif isinstance(term, rdflib.BNode):
                    encoder.encode_bnode(term)
                elif isinstance(term, rdflib.Literal):
                    encoder.encode_literal(
                        lex=term,
                        language=term.language,
                        datatype=term.datatype,
                    )
                else:
                    msg = f"unexpected term type {term!r}"
                    raise TypeError(msg)

            if frame := encoder.cycle(logic):
                protolib.serialize_length_prefixed(frame, output=stream)

        protolib.serialize_length_prefixed(logic.to_frame(), output=stream)
