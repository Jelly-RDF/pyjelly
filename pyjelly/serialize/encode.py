from __future__ import annotations

from collections.abc import Iterable, Sequence
from enum import Enum
from typing import Any, ClassVar, TypeVar
from typing_extensions import TypeAlias

from pyjelly import jelly, options
from pyjelly.errors import JellyConformanceError
from pyjelly.serialize.lookup import LookupEncoder


def split_iri(iri_string: str) -> tuple[str, str]:
    name = iri_string
    prefix = ""
    for sep in "#", "/":
        prefix, char, name = iri_string.rpartition(sep)
        if char:
            return prefix + char, name
    return prefix, name


T = TypeVar("T")
RowsAnd: TypeAlias = tuple[Sequence[jelly.RdfStreamRow], T]
RowsAndTerm: TypeAlias = (
    "RowsAnd[jelly.RdfIri | jelly.RdfLiteral | str | jelly.RdfDefaultGraph]"
)


class TermEncoder:
    TERM_ONEOF_NAMES: ClassVar = {
        jelly.RdfIri: "iri",
        jelly.RdfLiteral: "literal",
        str: "bnode",
        jelly.RdfDefaultGraph: "default_graph",
    }

    def __init__(
        self,
        lookup_preset: options.LookupPreset | None = None,
    ) -> None:
        if lookup_preset is None:
            lookup_preset = options.LookupPreset()
        self.lookup_preset = lookup_preset
        self.names = LookupEncoder(lookup_size=lookup_preset.max_names)
        self.prefixes = LookupEncoder(lookup_size=lookup_preset.max_prefixes)
        self.datatypes = LookupEncoder(lookup_size=lookup_preset.max_datatypes)

    def encode_iri(self, iri_string: str) -> RowsAnd[jelly.RdfIri]:
        prefix, name = split_iri(iri_string)
        if self.prefixes.lookup.max_size:
            prefix_entry_index = self.prefixes.encode_entry_index(prefix)
        else:
            name = iri_string
            prefix_entry_index = None

        name_entry_index = self.names.encode_entry_index(name)
        term_rows = []

        if prefix_entry_index is not None:
            prefix_entry = jelly.RdfPrefixEntry(id=prefix_entry_index, value=prefix)
            term_rows.append(jelly.RdfStreamRow(prefix=prefix_entry))

        if name_entry_index is not None:
            name_entry = jelly.RdfNameEntry(id=name_entry_index, value=name)
            term_rows.append(jelly.RdfStreamRow(name=name_entry))

        prefix_index = self.prefixes.encode_prefix_term_index(prefix)
        name_index = self.names.encode_name_term_index(name)
        return term_rows, jelly.RdfIri(prefix_id=prefix_index, name_id=name_index)

    def encode_default_graph(self) -> RowsAnd[jelly.RdfDefaultGraph]:
        return (), jelly.RdfDefaultGraph()

    def encode_bnode(self, bnode: str) -> RowsAnd[str]:
        return (), bnode

    def encode_literal(
        self,
        *,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
    ) -> RowsAnd[jelly.RdfLiteral]:
        datatype_id = None
        term_rows: tuple[()] | tuple[jelly.RdfStreamRow] = ()

        if datatype and datatype != options.STRING_DATATYPE_IRI:
            if self.datatypes.lookup.max_size == 0:
                msg = (
                    f"can't encode literal with type {datatype}: "
                    "datatype lookup cannot be used if disabled "
                    "(its size was set to 0)"
                )
                raise JellyConformanceError(msg)
            datatype_entry_id = self.datatypes.encode_entry_index(datatype)

            if datatype_entry_id is not None:
                entry = jelly.RdfDatatypeEntry(id=datatype_entry_id, value=datatype)
                term_rows = (jelly.RdfStreamRow(datatype=entry),)

            datatype_id = self.datatypes.encode_datatype_term_index(datatype)

        return term_rows, jelly.RdfLiteral(
            lex=lex,
            langtag=language,
            datatype=datatype_id,
        )

    def encode_any(self, term: object, slot: Slot) -> RowsAndTerm:
        msg = f"unsupported term type: {type(term)}"
        raise NotImplementedError(msg)


class Slot(str, Enum):
    """Slots for encoding RDF terms."""

    subject = "s"
    predicate = "p"
    object = "o"
    graph = "g"

    def __str__(self) -> str:
        return self.value


def encode_statement(
    terms: Iterable[object],
    term_encoder: TermEncoder,
    repeated_terms: dict[Slot, object],
) -> tuple[list[jelly.RdfStreamRow], dict[str, Any]]:
    statement: dict[str, object] = {}
    rows: list[jelly.RdfStreamRow] = []
    for slot, term in zip(Slot, terms):
        if repeated_terms[slot] != term:
            extra_rows, value = term_encoder.encode_any(term, slot)
            oneof = term_encoder.TERM_ONEOF_NAMES[type(value)]
            rows.extend(extra_rows)
            field = f"{slot}_{oneof}"
            statement[field] = value
            repeated_terms[slot] = term
    return rows, statement


def encode_triple(
    terms: Iterable[object],
    term_encoder: TermEncoder,
    repeated_terms: dict[Slot, object],
) -> list[jelly.RdfStreamRow]:
    rows, statement = encode_statement(terms, term_encoder, repeated_terms)
    row = jelly.RdfStreamRow(triple=jelly.RdfTriple(**statement))
    rows.append(row)
    return rows


def encode_quad(
    terms: Iterable[object],
    term_encoder: TermEncoder,
    repeated_terms: dict[Slot, object],
) -> list[jelly.RdfStreamRow]:
    rows, statement = encode_statement(terms, term_encoder, repeated_terms)
    row = jelly.RdfStreamRow(quad=jelly.RdfQuad(**statement))
    rows.append(row)
    return rows


def encode_namespace_declaration(
    name: str,
    value: str,
    term_encoder: TermEncoder,
) -> list[jelly.RdfStreamRow]:
    [*rows], iri = term_encoder.encode_iri(value)
    declaration = jelly.RdfNamespaceDeclaration(name=name, value=iri)
    row = jelly.RdfStreamRow(namespace=declaration)
    rows.append(row)
    return rows


def encode_options(
    lookup_preset: options.LookupPreset,
    stream_types: options.StreamTypes,
    params: options.StreamParameters,
) -> jelly.RdfStreamRow:
    return jelly.RdfStreamRow(
        options=jelly.RdfStreamOptions(
            stream_name=params.stream_name,
            physical_type=stream_types.physical_type,
            generalized_statements=params.generalized_statements,
            rdf_star=params.rdf_star,
            max_name_table_size=lookup_preset.max_names,
            max_prefix_table_size=lookup_preset.max_prefixes,
            max_datatype_table_size=lookup_preset.max_datatypes,
            logical_type=stream_types.logical_type,
            version=params.version,
        )
    )
