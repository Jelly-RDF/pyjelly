from __future__ import annotations

from collections.abc import Iterable, Sequence
from enum import Enum
from typing import Any, ClassVar
from typing_extensions import TypeAlias

from pyjelly import jelly, options
from pyjelly.producing.lookups import LookupEncoder


def split_iri(iri_string: str) -> tuple[str, str]:
    name = iri_string
    prefix = ""
    for sep in "#", "/":
        prefix, char, name = iri_string.rpartition(sep)
        if char:
            return prefix + char, name
    return prefix, name


RowsAndTerm: TypeAlias = tuple[
    Sequence[jelly.RdfStreamRow],
    "jelly.RdfIri | jelly.RdfLiteral | str | jelly.RdfDefaultGraph",
]


class TermEncoder:
    TERM_ONEOF_NAMES: ClassVar = {
        jelly.RdfIri: "iri",
        jelly.RdfLiteral: "literal",
        str: "bnode",
        jelly.RdfDefaultGraph: "default_graph",
    }

    def __init__(
        self,
        name_lookup_size: int = options.DEFAULT_NAME_LOOKUP_SIZE,
        prefix_lookup_size: int = options.DEFAULT_PREFIX_LOOKUP_SIZE,
        datatype_lookup_size: int = options.DEFAULT_DATATYPE_LOOKUP_SIZE,
    ) -> None:
        self.names = LookupEncoder(lookup_size=name_lookup_size)
        self.prefixes = LookupEncoder(lookup_size=prefix_lookup_size)
        self.datatypes = LookupEncoder(lookup_size=datatype_lookup_size)

    def encode_iri(self, iri_string: str) -> RowsAndTerm:
        prefix, name = split_iri(iri_string)
        prefix_id = self.prefixes.encode_entry_index(prefix)
        name_id = self.names.encode_entry_index(name)
        term_rows = []

        if prefix_id is not None:
            prefix_entry = jelly.RdfPrefixEntry(id=prefix_id, value=prefix)
            term_rows.append(jelly.RdfStreamRow(prefix=prefix_entry))

        if name_id is not None:
            name_entry = jelly.RdfNameEntry(id=name_id, value=name)
            term_rows.append(jelly.RdfStreamRow(name=name_entry))

        prefix_id = self.prefixes.encode_prefix_term_index(prefix)
        name_id = self.names.encode_name_term_index(name)
        return term_rows, jelly.RdfIri(prefix_id=prefix_id, name_id=name_id)

    def encode_default_graph(self) -> RowsAndTerm:
        return (), jelly.RdfDefaultGraph()

    def encode_bnode(self, bnode: str) -> RowsAndTerm:
        return (), bnode

    def encode_literal(
        self,
        *,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
    ) -> RowsAndTerm:
        datatype_id = None
        term_rows: tuple[()] | tuple[jelly.RdfStreamRow] = ()

        if datatype and datatype != options.STRING_DATATYPE_IRI:
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


def new_repeated_terms() -> dict[Slot, object]:
    """Create a new dictionary for repeated terms."""
    return dict.fromkeys(Slot)


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


def encode_quad(
    terms: Iterable[object],
    term_encoder: TermEncoder,
    repeated_terms: dict[Slot, object],
) -> list[jelly.RdfStreamRow]:
    rows, statement = encode_statement(terms, term_encoder, repeated_terms)
    row = jelly.RdfStreamRow(quad=jelly.RdfQuad(**statement))
    rows.append(row)
    return rows


def encode_triple(
    terms: Iterable[object],
    term_encoder: TermEncoder,
    repeated_terms: dict[Slot, object],
) -> list[jelly.RdfStreamRow]:
    rows, statement = encode_statement(terms, term_encoder, repeated_terms)
    row = jelly.RdfStreamRow(triple=jelly.RdfTriple(**statement))
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
