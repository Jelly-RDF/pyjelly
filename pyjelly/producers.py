from __future__ import annotations

from typing import TypeVar

from pyjelly import contexts, stax
from pyjelly.pb2 import rdf_pb2 as pb

PbEntryT = TypeVar("PbEntryT", pb.RdfNameEntry, pb.RdfPrefixEntry, pb.RdfDatatypeEntry)


def produce_options(logic: stax.StreamLogic) -> pb.RdfStreamOptions:
    return pb.RdfStreamOptions(
        stream_name=logic.context.options.name,
        physical_type=logic.context._pb_type,
        generalized_statements=True,
        rdf_star=True,
        max_name_table_size=logic.context.options.name_lookup_size,
        max_prefix_table_size=logic.context.options.prefix_lookup_size,
        max_datatype_table_size=logic.context.options.datatype_lookup_size,
        logical_type=logic._pb_type,
        version=1,
    )


def produce_entry(
    context: contexts.StreamContext,
    value: str,
    entry_class: type[PbEntryT],
) -> tuple[int, PbEntryT | None]:
    ident, is_new = context.lookups[entry_class].find(value, compress=True)
    return ident, entry_class(id=ident, value=value) if is_new else None


def produce_iri(
    context: contexts.StreamContext,
    iri: str,
) -> tuple[pb.RdfPrefixEntry | None, pb.RdfNameEntry | None, pb.RdfIri]:
    name = iri
    prefix = ""
    for sep in ("#", "/"):
        prefix, was_split, name = iri.rpartition(sep)
        if was_split:
            break
    prefix_id, prefix_entry = produce_entry(context, prefix, pb.RdfPrefixEntry)
    name_id, name_entry = produce_entry(context, name, pb.RdfNameEntry)
    compressed_iri = pb.RdfIri(prefix_id=prefix_id, name_id=name_id)
    return prefix_entry, name_entry, compressed_iri


def produce_literal(
    context: contexts.StreamContext,
    lex: str,
    *,
    language_tag: str | None = None,
    datatype: str | None = None,
) -> tuple[pb.RdfDatatypeEntry | None, pb.RdfLiteral]:
    if datatype is not None:
        datatype_ident, datatype_entry = produce_entry(
            context,
            datatype,
            pb.RdfDatatypeEntry,
        )
        return datatype_entry, pb.RdfLiteral(lex=lex, datatype=datatype_ident)
    return None, pb.RdfLiteral(lex=lex, langtag=language_tag)
