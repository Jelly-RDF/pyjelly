from collections.abc import Callable
from itertools import chain
from typing import IO
from typing_extensions import override

import rdflib
from rdflib.graph import Dataset, Graph, QuotedGraph
from rdflib.parser import InputSource
from rdflib.parser import Parser as RDFLibParser
from rdflib.serializer import Serializer as RDFLibSerializer

from pyjelly.pb2 import rdf_pb2 as pb
from pyjelly.serialization import encoders, lookups, queues


class RDFLibJellySerializer(RDFLibSerializer):
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
        as_quads: bool | None = None,
        writer: Callable[[pb.RdfStreamFrame], None],
    ) -> None:
        if encoding == "utf-8":
            msg = "jelly is a binary protocol, call with encoding='jelly'"
            raise TypeError(msg)

        if isinstance(self.store, Dataset) and as_quads is None:
            msg = (
                "serialized store has multiple graphs"
                "but as_quads was not set to True or False"
            )
            raise ValueError(msg)

        if as_quads is not None:
            msg = "multiple graph serialization is not implemented"
            raise NotImplementedError(msg)

        options = lookups.Options.big()
        queue = queues.FlatTripleQueue(on_full=writer)
        encoder = encoders.TripleEncoder(options)

        encoder.encode_options(queue)

        for term in chain.from_iterable(self.store):
            if encoder.should_skip(term):
                encoder.cycle(queue)
                continue

            match term:
                case rdflib.URIRef():
                    encoder.encode_iri(term, queue)

                case rdflib.BNode():
                    encoder.encode_bnode(term)

                case rdflib.Literal(language=language, datatype=datatype):
                    encoder.encode_literal(
                        lex=str(term),
                        language=language,
                        datatype=None if datatype is None else str(datatype),
                        queue=queue,
                    )

                case _:
                    msg = (
                        f"Got {term!r} which didn't match any known RDF object pattern"
                    )
                    raise AssertionError(msg)

            encoder.cycle(queue)
        queue.flush()


class RDFLibJellyParser(RDFLibParser):
    @override
    def parse(self, source: InputSource, sink: rdflib.Graph) -> None:
        return super().parse(source, sink)
