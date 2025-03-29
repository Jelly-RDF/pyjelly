from functools import partial
from itertools import chain, cycle
from typing import IO
from typing_extensions import Never, override

import rdflib
from google.protobuf.proto import serialize_length_prefixed
from rdflib.graph import Dataset, Graph, QuotedGraph
from rdflib.parser import InputSource
from rdflib.parser import Parser as RDFLibParser
from rdflib.serializer import Serializer as RDFLibSerializer

from pyjelly import contexts, producers, stax, writers
from pyjelly.pb2 import rdf_pb2 as pb


def flat_triples(out: IO[bytes], graph: rdflib.Graph) -> None:
    options = contexts.Options(
        name_lookup_size=128,
        prefix_lookup_size=16,
        datatype_lookup_size=16,
    )
    context = contexts.TripleContext(options)
    stream = stax.FlatTripleStreamLogic(
        context,
        flush_callback=partial(
            serialize_length_prefixed,
            output=out,  # type: ignore[arg-type]
        ),
    )
    writers.begin(stream)

    roles = cycle(("subject", "predicate", "object"))
    fields = {}

    for i, (role, term) in enumerate(zip(roles, chain.from_iterable(graph))):
        if context.repeated_terms[role] == term:
            continue

        print(role, term)

        context.repeated_terms[role] = term

        match term:
            case rdflib.URIRef():
                kind = "iri"
                (pb_prefix, pb_name, pb_iri) = producers.produce_iri(context, term)
                if pb_prefix:
                    stream.add_row(pb.RdfStreamRow(prefix=pb_prefix))
                if pb_name:
                    stream.add_row(pb.RdfStreamRow(name=pb_name))
                field_value = pb_iri

            case rdflib.BNode():
                kind = "bnode"
                field_value = term

            case rdflib.Literal(
                value=lex,
                language=language_tag,
                datatype=datatype,
            ) if not (language_tag and datatype):
                kind = "literal"
                (pb_datatype, pb_literal) = producers.produce_literal(
                    context,
                    lex,
                    language_tag=language_tag,
                    datatype=datatype,
                )
                if pb_datatype:
                    stream.add_row(pb.RdfStreamRow(datatype=pb_datatype))

                field_value = pb_literal
            case _:
                msg = f"unsupported term type {type(term)}"
                raise AssertionError(msg)

        fields[f"{role[0]}_{kind}"] = field_value

        if role == "object":
            row = pb.RdfStreamRow(triple=pb.RdfTriple(**fields))
            stream.add_row(row)

    stream.flush()


class RDFLibJellySerializer(RDFLibSerializer):
    def __init__(self, store: Graph) -> None:
        if isinstance(store, QuotedGraph):
            msg = "N3 format is not supported"
            raise NotImplementedError(msg)
        super().__init__(store)

    @override
    def serialize(
        self,
        stream: IO[bytes],
        base: str | None = None,
        encoding: str | None = None,
        *,
        as_quads: bool | None = None,
        **args: Never,
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

        flat_triples(stream, self.store)


class RDFLibJellyParser(RDFLibParser):
    @override
    def parse(self, source: InputSource, sink: rdflib.Graph) -> None:
        return super().parse(source, sink)


if __name__ == "__main__":
    g = Graph()
    g.parse("test.nt")
    g.serialize(destination="z/jelly-python.jelly", format="jelly")
