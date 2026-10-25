from __future__ import annotations

from collections.abc import Iterable, Iterator
from enum import IntEnum
from typing import TypeAlias, TypeVar

from mypy_extensions import mypyc_attr

from pyjelly import jelly, options
from pyjelly.errors import JellyConformanceError
from pyjelly.serialize.flows import FrameFlow, ManualFrameFlow
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
Statement: TypeAlias = jelly.RdfQuad | jelly.RdfTriple
HasGraph: TypeAlias = jelly.RdfQuad | jelly.RdfGraphStart
Terms: TypeAlias = (
    jelly.RdfIri | jelly.RdfLiteral | str | jelly.RdfDefaultGraph | jelly.RdfTriple
)


class _CachedIri:
    """
    How an IRI was encoded last time: its prefix and name, and their indices.

    An index is still valid if the lookup holds the very same string object at it
    (the strings are the lookup's own, see TermEncoder.encode_iri_indices()). If
    the entry was evicted since, the string there is another one. 0 means none.
    """

    def __init__(self, prefix: str, name: str) -> None:
        self.prefix = prefix
        self.name = name
        self.prefix_index = 0
        self.name_index = 0


@mypyc_attr(allow_interpreted_subclasses=True)
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
        # Where rows for new lookup entries go. A stream replaces it with its own.
        self.flow: FrameFlow = ManualFrameFlow()
        self.iri_cache: dict[str, _CachedIri] = {}
        # Cleared when full, so that a long stream doesn't grow it without bound
        self.iri_cache_size = max(4 * lookup_preset.max_names, 1024)

    def same_term(self, a: object, b: object) -> bool:
        """
        Return whether two terms are equal, for repeated terms.

        Integrations can make it faster for their term classes.
        """
        return a is b or a == b

    def new_epoch(self) -> None:
        """
        Start encoding a new statement (or other row with terms).

        Until the next epoch, the lookup entries it uses are not evicted: the row
        still refers to them.
        """
        self.names.lookup.new_epoch()
        self.prefixes.lookup.new_epoch()
        self.datatypes.lookup.new_epoch()

    def encode_iri_indices(self, iri_string: str) -> tuple[int, int]:
        """
        Encode lookup indices for IRI, adding rows for new lookup entries.

        Most IRIs repeat, so how an IRI splits into prefix and name, and the
        indices of their entries, are cached (see _CachedIri).

        Args:
            iri_string (str): full iri in string format.

        Returns:
            tuple[int, int]: indices in prefix and name tables.

        """
        cached = self.iri_cache.get(iri_string)
        if cached is None:
            if len(self.iri_cache) >= self.iri_cache_size:
                self.iri_cache.clear()
            if self.prefixes.lookup.max_size:
                prefix, name = split_iri(iri_string)
            else:
                prefix, name = "", iri_string
            cached = _CachedIri(prefix, name)
            self.iri_cache[iri_string] = cached

        prefix_index = 0
        prefixes = self.prefixes
        if prefixes.lookup.max_size:
            index = cached.prefix_index
            if index and prefixes.lookup.keys[index] is cached.prefix:
                prefixes.lookup.touch(index)
            else:
                index, set_index = prefixes.encode_entry(cached.prefix)
                if set_index >= 0:
                    prefix_entry = self.flow.new_row().prefix
                    prefix_entry.id = set_index
                    prefix_entry.value = cached.prefix
                cached.prefix_index = index
                cached.prefix = prefixes.lookup.keys[index]
            prefix_index = prefixes.prefix_index_to_write(
                index, empty=not cached.prefix
            )

        names = self.names
        index = cached.name_index
        if index and names.lookup.keys[index] is cached.name:
            names.lookup.touch(index)
        else:
            index, set_index = names.encode_entry(cached.name)
            if set_index >= 0:
                name_entry = self.flow.new_row().name
                name_entry.id = set_index
                name_entry.value = cached.name
            cached.name_index = index
            cached.name = names.lookup.keys[index]
        return prefix_index, names.name_index_to_write(index)

    def encode_iri(self, iri_string: str, iri: jelly.RdfIri) -> None:
        """
        Encode iri, adding rows for new lookup entries.

        Args:
            iri_string (str): full iri in string format.
            iri (jelly.RdfIri): iri to fill

        """
        prefix_index, name_index = self.encode_iri_indices(iri_string)
        iri.prefix_id = prefix_index
        iri.name_id = name_index

    def encode_default_graph(self, g_default_graph: jelly.RdfDefaultGraph) -> None:
        """Encode default graph."""
        g_default_graph.CopyFrom(jelly.RdfDefaultGraph())

    def encode_literal(
        self,
        *,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
        literal: jelly.RdfLiteral,
    ) -> None:
        """
        Encode literal, adding a row for a new datatype entry.

        Args:
            lex (str): lexical form/literal value
            language (str | None, optional): langtag. Defaults to None.
            datatype (str | None, optional): data type if
            it is a typed literal. Defaults to None.
            literal (jelly.RdfLiteral): literal to fill.

        Raises:
            JellyConformanceError: if datatype specified while
                datatable is not used.

        """
        datatype_id = None

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
                entry = self.flow.new_row().datatype
                entry.id = datatype_entry_id
                entry.value = datatype

            datatype_id = self.datatypes.encode_datatype_term_index(datatype)

        literal.lex = lex
        if language:
            literal.langtag = language
        if datatype_id:
            literal.datatype = datatype_id

    def encode_quoted_triple(
        self, terms: Iterable[object], quoted_statement: jelly.RdfTriple
    ) -> None:
        """
        Encode a quoted triple, adding rows for new lookup entries.

        Notes:
            Although a triple, it is treated as a part of a statement.
            Repeated terms are not used when encoding quoted triples.

        Args:
            terms (Iterable[object]): triple terms to encode.
            quoted_statement (jelly.RdfTriple): quoted triple to fill.

        """
        terms = iter(terms)
        self.encode_spo(next(terms), Slot.subject, quoted_statement)
        self.encode_spo(next(terms), Slot.predicate, quoted_statement)
        self.encode_spo(next(terms), Slot.object, quoted_statement)

    def encode_spo(self, term: object, slot: Slot, statement: Statement) -> None:
        msg = f"unsupported term type: {type(term)}"
        raise NotImplementedError(msg)

    def encode_graph(self, term: object, statement: HasGraph) -> None:
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
) -> None:
    """
    Encode the s/p/o of a statement, adding rows for new lookup entries.

    Args:
        terms (Iterator[object]): iterator for original terms to encode
        term_encoder (TermEncoder): encoder with lookup tables
        repeated_terms (list[object | None): list of repeated terms.
        statement (Statement): Triple/Quad to fill.

    """
    s = next(terms)
    if not term_encoder.same_term(repeated_terms[Slot.subject], s):
        term_encoder.encode_spo(s, Slot.subject, statement)
        repeated_terms[Slot.subject] = s
    p = next(terms)
    if not term_encoder.same_term(repeated_terms[Slot.predicate], p):
        term_encoder.encode_spo(p, Slot.predicate, statement)
        repeated_terms[Slot.predicate] = p
    o = next(terms)
    if not term_encoder.same_term(repeated_terms[Slot.object], o):
        term_encoder.encode_spo(o, Slot.object, statement)
        repeated_terms[Slot.object] = o


def encode_triple(
    terms: Iterable[object],
    term_encoder: TermEncoder,
    repeated_terms: list[object | None],
    buffer: jelly.RdfTriple,
) -> None:
    """
    Encode one triple, adding its rows to the encoder's flow.

    Args:
        terms (Iterable[object]): original terms to encode
        term_encoder (TermEncoder): current encoder with lookup tables
        repeated_terms (list[object | None]): list of repeated terms.
        buffer (jelly.RdfTriple): empty triple to encode into, empty again after.
            The triple can only be added to the flow after the rows for its new
            lookup entries, which encoding it adds.

    """
    term_encoder.new_epoch()
    encode_spo(iter(terms), term_encoder, repeated_terms, buffer)
    term_encoder.flow.new_row().triple.CopyFrom(buffer)
    buffer.Clear()


def encode_quad(
    terms: Iterable[object],
    term_encoder: TermEncoder,
    repeated_terms: list[object | None],
    buffer: jelly.RdfQuad,
) -> None:
    """
    Encode one quad, adding its rows to the encoder's flow.

    Args:
        terms (Iterable[object]): original terms to encode
        term_encoder (TermEncoder): current encoder with lookup tables
        repeated_terms (list[object | None]): list of repeated terms.
        buffer (jelly.RdfQuad): empty quad to encode into, see encode_triple().

    """
    term_encoder.new_epoch()
    terms = iter(terms)
    encode_spo(terms, term_encoder, repeated_terms, buffer)
    g = next(terms)
    if not term_encoder.same_term(repeated_terms[Slot.graph], g):
        term_encoder.encode_graph(g, buffer)
        repeated_terms[Slot.graph] = g
    term_encoder.flow.new_row().quad.CopyFrom(buffer)
    buffer.Clear()


def encode_namespace_declaration(
    name: str,
    value: str,
    term_encoder: TermEncoder,
) -> None:
    """
    Encode namespace declaration, adding its rows to the encoder's flow.

    Args:
        name (str): namespace prefix label
        value (str): namespace iri
        term_encoder (TermEncoder): current encoder

    """
    term_encoder.new_epoch()
    # The rows for new lookup entries go first
    prefix_index, name_index = term_encoder.encode_iri_indices(value)
    declaration = term_encoder.flow.new_row().namespace
    declaration.name = name
    declaration.value.prefix_id = prefix_index
    declaration.value.name_id = name_index


def encode_options(
    lookup_preset: options.LookupPreset,
    stream_types: options.StreamTypes,
    params: options.StreamParameters,
) -> jelly.RdfStreamOptions:
    """
    Encode stream options to ProtoBuf message.

    Args:
        lookup_preset (options.LookupPreset): lookup tables options
        stream_types (options.StreamTypes): physical and logical types
        params (options.StreamParameters): other params.

    Returns:
        jelly.RdfStreamOptions: encoded stream options

    """
    return jelly.RdfStreamOptions(
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
