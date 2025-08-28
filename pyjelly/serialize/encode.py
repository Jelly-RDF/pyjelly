from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from enum import IntEnum
from typing import TypeVar, Union
from typing_extensions import TypeAlias

from pyjelly import jelly, options
from pyjelly.errors import JellyConformanceError
from pyjelly.serialize.lookup import LookupEncoder


def split_iri(iri_string: str) -> tuple[str, str]:
    """
    Split iri into prefix and name.

    Args:
        iri_string (str): full iri string.

    Returns:
        tuple[str, str]: iri's prefix and name.

    """
    name = iri_string
    prefix = ""
    for sep in "#", "/":
        prefix, char, name = iri_string.rpartition(sep)
        if char:
            return prefix + char, name
    return prefix, name


T = TypeVar("T")
Rows: TypeAlias = Sequence[jelly.RdfStreamRow]
Statement: TypeAlias = Union[jelly.RdfQuad, jelly.RdfTriple]
HasGraph: TypeAlias = Union[jelly.RdfQuad, jelly.RdfGraphStart]
Terms: TypeAlias = Union[
    jelly.RdfIri, jelly.RdfLiteral, str, jelly.RdfDefaultGraph, jelly.RdfTriple
]


class TermEncoder:
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

    def encode_iri_indices(self, iri_string: str) -> tuple[Rows, int, int]:
        """
        Encode lookup indices for IRI.

        Args:
            iri_string (str): full iri in string format.

        Returns:
            tuple[Rows, int, int]: additional rows (if any) and
                indices in prefix and name tables.

        """
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
        return term_rows, prefix_index, name_index

    def encode_iri(self, iri_string: str, iri: jelly.RdfIri) -> Rows:
        """
        Encode iri.

        Args:
            iri_string (str): full iri in string format.
            iri (jelly.RdfIri): iri to fill

        Returns:
            Rows: extra rows for prefix and name tables, if any.

        """
        term_rows, prefix_index, name_index = self.encode_iri_indices(iri_string)
        iri.prefix_id = prefix_index
        iri.name_id = name_index
        return term_rows

    def encode_default_graph(self, g_default_graph: jelly.RdfDefaultGraph) -> Rows:
        """
        Encode default graph.

        Returns:
            Rows: empty extra rows (for API consistency)

        """
        g_default_graph.CopyFrom(jelly.RdfDefaultGraph())
        return ()

    def encode_literal(
        self,
        *,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
        literal: jelly.RdfLiteral,
    ) -> Rows:
        """
        Encode literal.

        Args:
            lex (str): lexical form/literal value
            language (str | None, optional): langtag. Defaults to None.
            datatype (str | None, optional): data type if
            it is a typed literal. Defaults to None.
            literal (jelly.RdfLiteral): literal to fill.

        Raises:
            JellyConformanceError: if datatype specified while
                datatable is not used.

        Returns:
            Rows: extra rows (i.e., datatype entries).

        """
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

        literal.lex = lex
        if language:
            literal.langtag = language
        if datatype_id:
            literal.datatype = datatype_id
        return term_rows

    def encode_quoted_triple(
        self, terms: Iterable[object], quoted_statement: jelly.RdfTriple
    ) -> Rows:
        """
        Encode a quoted triple.

        Notes:
            Although a triple, it is treated as a part of a statement.
            Repeated terms are not used when encoding quoted triples.

        Args:
            terms (Iterable[object]): triple terms to encode.
            quoted_statement (jelly.RdfTriple): quoted triple to fill.

        Returns:
            Rows: additional stream rows with preceeding
                information (prefixes, names, datatypes rows, if any).

        """
        rows: list[jelly.RdfStreamRow] = []
        terms = iter(terms)
        extra_rows = self.encode_spo(next(terms), Slot.subject, quoted_statement)
        rows.extend(extra_rows)
        extra_rows = self.encode_spo(next(terms), Slot.predicate, quoted_statement)
        rows.extend(extra_rows)
        extra_rows = self.encode_spo(next(terms), Slot.object, quoted_statement)
        rows.extend(extra_rows)
        return rows

    def encode_spo(self, term: object, slot: Slot, statement: Statement) -> Rows:
        msg = f"unsupported term type: {type(term)}"
        raise NotImplementedError(msg)

    def encode_graph(self, term: object, statement: HasGraph) -> Rows:
        msg = f"unsupported term type: {type(term)}"
        raise NotImplementedError(msg)

    def get_iri_field(self, statement: Statement, slot: Slot) -> jelly.RdfIri:
        """Get IRI field directly based on slot."""
        if slot == Slot.subject:
            return statement.s_iri
        if slot == Slot.predicate:
            return statement.p_iri
        return statement.o_iri

    def get_literal_field(self, statement: Statement, slot: Slot) -> jelly.RdfLiteral:
        """Get literal field directly based on slot."""
        if slot == Slot.subject:
            return statement.s_literal
        if slot == Slot.predicate:
            return statement.p_literal
        return statement.o_literal

    def set_bnode_field(
        self, statement: Statement, slot: Slot, identifier: str
    ) -> None:
        """Set bnode field directly based on slot."""
        if slot == Slot.subject:
            statement.s_bnode = identifier
        elif slot == Slot.predicate:
            statement.p_bnode = identifier
        else:
            statement.o_bnode = identifier

    def get_triple_field(self, statement: Statement, slot: Slot) -> jelly.RdfTriple:
        """Get triple term field directly based on slot."""
        if slot == Slot.subject:
            return statement.s_triple_term
        if slot == Slot.predicate:
            return statement.p_triple_term
        return statement.o_triple_term


class Slot(IntEnum):
    subject = 0
    predicate = 1
    object = 2
    graph = 3


def encode_spo(
    terms: Iterator[object],
    term_encoder: TermEncoder,
    repeated_terms: list[object | None],
    statement: Statement,
) -> list[jelly.RdfStreamRow]:
    """
    Encode the s/p/o of a statement.

    Args:
        terms (Iterator[object]): iterator for original terms to encode
        term_encoder (TermEncoder): encoder with lookup tables
        repeated_terms (list[object | None): list of repeated terms.
        statement (Statement): Triple/Quad to fill.

    Returns:
        list[jelly.RdfStreamRow] extra rows to append.

    """
    rows: list[jelly.RdfStreamRow] = []
    s = next(terms)
    if repeated_terms[Slot.subject] != s:
        extra_rows = term_encoder.encode_spo(s, Slot.subject, statement)
        rows.extend(extra_rows)
        repeated_terms[Slot.subject] = s
    p = next(terms)
    if repeated_terms[Slot.predicate] != p:
        extra_rows = term_encoder.encode_spo(p, Slot.predicate, statement)
        rows.extend(extra_rows)
        repeated_terms[Slot.predicate] = p
    o = next(terms)
    if repeated_terms[Slot.object] != o:
        extra_rows = term_encoder.encode_spo(o, Slot.object, statement)
        rows.extend(extra_rows)
        repeated_terms[Slot.object] = o
    return rows


def encode_triple(
    terms: Iterable[object],
    term_encoder: TermEncoder,
    repeated_terms: list[object | None],
) -> list[jelly.RdfStreamRow]:
    """
    Encode one triple.

    Args:
        terms (Iterable[object]): original terms to encode
        term_encoder (TermEncoder): current encoder with lookup tables
        repeated_terms (list[object | None]): list of repeated terms.

    Returns:
        list[jelly.RdfStreamRow]: list of rows to add to the current flow.

    """
    triple = jelly.RdfTriple()
    terms = iter(terms)
    rows = encode_spo(terms, term_encoder, repeated_terms, triple)
    row = jelly.RdfStreamRow(triple=triple)
    rows.append(row)
    return rows


def encode_quad(
    terms: Iterable[object],
    term_encoder: TermEncoder,
    repeated_terms: list[object | None],
) -> list[jelly.RdfStreamRow]:
    """
    Encode one quad.

    Args:
        terms (Iterable[object]): original terms to encode
        term_encoder (TermEncoder): current encoder with lookup tables
        repeated_terms (list[object | None]): list of repeated terms.

    Returns:
        list[jelly.RdfStreamRow]: list of messages to append to current flow.

    """
    terms = iter(terms)
    quad = jelly.RdfQuad()
    rows = encode_spo(terms, term_encoder, repeated_terms, quad)
    g = next(terms)
    if repeated_terms[Slot.graph] != g:
        extra_rows = term_encoder.encode_graph(g, quad)
        rows.extend(extra_rows)
        repeated_terms[Slot.graph] = g
    row = jelly.RdfStreamRow(quad=quad)
    rows.append(row)
    return rows


def encode_namespace_declaration(
    name: str,
    value: str,
    term_encoder: TermEncoder,
) -> list[jelly.RdfStreamRow]:
    """
    Encode namespace declaration.

    Args:
        name (str): namespace prefix label
        value (str): namespace iri
        term_encoder (TermEncoder): current encoder

    Returns:
        list[jelly.RdfStreamRow]: list of messages to append to current flow.

    """
    iri = jelly.RdfIri()
    [*rows] = term_encoder.encode_iri(value, iri=iri)
    declaration = jelly.RdfNamespaceDeclaration(name=name, value=iri)
    row = jelly.RdfStreamRow(namespace=declaration)
    rows.append(row)
    return rows


def encode_options(
    lookup_preset: options.LookupPreset,
    stream_types: options.StreamTypes,
    params: options.StreamParameters,
) -> jelly.RdfStreamRow:
    """
    Encode stream options to ProtoBuf message.

    Args:
        lookup_preset (options.LookupPreset): lookup tables options
        stream_types (options.StreamTypes): physical and logical types
        params (options.StreamParameters): other params.

    Returns:
        jelly.RdfStreamRow: encoded stream options row

    """
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
