from __future__ import annotations

from collections.abc import Iterable
from enum import IntEnum
from typing import Final, TypeAlias

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


Statement: TypeAlias = jelly.RdfQuad | jelly.RdfTriple


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


# Kinds of EncodedTerm
TERM_IRI: Final = 1
TERM_BNODE: Final = 2
TERM_LITERAL: Final = 3
TERM_TRIPLE: Final = 4
TERM_DEFAULT_GRAPH: Final = 5


class EncodedTerm:
    """
    A term encoded for a statement, before it is written into the statement.

    A statement is written in two steps: first its terms are encoded (adding rows
    for new lookup entries to the frame), then the statement's row is added
    after them and the terms are written into it. This way the row is filled in
    place, and never copied.
    """

    def __init__(self) -> None:
        #: One of the TERM_* constants
        self.kind = 0
        #: IRI: prefix index; literal: datatype index
        self.first = 0
        #: IRI: name index
        self.second = 0
        #: Literal: lexical form; blank node: label
        self.text = ""
        #: Literal: language tag, empty if none
        self.lang = ""
        #: Quoted triple
        self.triple: jelly.RdfTriple | None = None


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
        # The encoded subject, predicate, object and graph of the statement
        self.encoded = [EncodedTerm() for _ in range(4)]

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

    def encode_iri(self, iri_string: str, out: EncodedTerm) -> None:
        """Encode iri into `out`, adding rows for new lookup entries."""
        prefix_index, name_index = self.encode_iri_indices(iri_string)
        out.kind = TERM_IRI
        out.first = prefix_index
        out.second = name_index

    def encode_bnode(self, label: str, out: EncodedTerm) -> None:
        """Encode a blank node into `out`."""
        out.kind = TERM_BNODE
        out.text = label

    def encode_default_graph(self, out: EncodedTerm) -> None:
        """Encode default graph into `out`."""
        out.kind = TERM_DEFAULT_GRAPH

    def encode_literal(
        self,
        *,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
        out: EncodedTerm,
    ) -> None:
        """
        Encode literal into `out`, adding a row for a new datatype entry.

        Args:
            lex (str): lexical form/literal value
            language (str | None, optional): langtag. Defaults to None.
            datatype (str | None, optional): data type if
            it is a typed literal. Defaults to None.
            out (EncodedTerm): where to put the encoded literal.

        Raises:
            JellyConformanceError: if datatype specified while
                datatable is not used.

        """
        datatype_id = 0

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

        out.kind = TERM_LITERAL
        out.text = lex
        out.lang = language or ""
        out.first = datatype_id

    def encode_quoted_triple(self, terms: Iterable[object], out: EncodedTerm) -> None:
        """
        Encode a quoted triple into `out`, adding rows for new lookup entries.

        Notes:
            Although a triple, it is treated as a part of a statement.
            Repeated terms are not used when encoding quoted triples.

        Args:
            terms (Iterable[object]): triple terms to encode.
            out (EncodedTerm): where to put the encoded triple.

        """
        terms = iter(terms)
        triple = jelly.RdfTriple()
        for slot in range(3):
            term = EncodedTerm()
            self.encode_spo(next(terms), term)
            write_term(term, triple, slot)
        out.kind = TERM_TRIPLE
        out.triple = triple

    def encode_spo(self, term: object, out: EncodedTerm) -> None:
        """Encode a subject, predicate or object into `out`."""
        msg = f"unsupported term type: {type(term)}"
        raise NotImplementedError(msg)

    def encode_graph(self, term: object, out: EncodedTerm) -> None:
        """Encode a graph name into `out`."""
        msg = f"unsupported term type: {type(term)}"
        raise NotImplementedError(msg)


class Slot(IntEnum):
    subject = 0
    predicate = 1
    object = 2
    graph = 3


def _iri_field(statement: Statement, slot: int) -> jelly.RdfIri:
    if slot == 0:
        return statement.s_iri
    if slot == 1:
        return statement.p_iri
    return statement.o_iri


def _literal_field(statement: Statement, slot: int) -> jelly.RdfLiteral:
    if slot == 0:
        return statement.s_literal
    if slot == 1:
        return statement.p_literal
    return statement.o_literal


def _triple_field(statement: Statement, slot: int) -> jelly.RdfTriple:
    if slot == 0:
        return statement.s_triple_term
    if slot == 1:
        return statement.p_triple_term
    return statement.o_triple_term


def _set_bnode(statement: Statement, slot: int, label: str) -> None:
    if slot == 0:
        statement.s_bnode = label
    elif slot == 1:
        statement.p_bnode = label
    else:
        statement.o_bnode = label


def _write_literal(term: EncodedTerm, literal: jelly.RdfLiteral) -> None:
    literal.lex = term.text
    if term.lang:
        literal.langtag = term.lang
    if term.first:
        literal.datatype = term.first


def write_term(term: EncodedTerm, statement: Statement, slot: int) -> None:
    """Write an encoded subject (slot 0), predicate (1) or object (2)."""
    kind = term.kind
    if kind == TERM_IRI:
        iri = _iri_field(statement, slot)
        iri.prefix_id = term.first
        iri.name_id = term.second
    elif kind == TERM_LITERAL:
        _write_literal(term, _literal_field(statement, slot))
    elif kind == TERM_BNODE:
        _set_bnode(statement, slot, term.text)
    elif kind == TERM_TRIPLE:
        assert term.triple is not None
        _triple_field(statement, slot).CopyFrom(term.triple)


def write_graph(term: EncodedTerm, quad: jelly.RdfQuad) -> None:
    """Write an encoded graph name."""
    kind = term.kind
    if kind == TERM_IRI:
        iri = quad.g_iri
        iri.prefix_id = term.first
        iri.name_id = term.second
    elif kind == TERM_DEFAULT_GRAPH:
        quad.g_default_graph.SetInParent()
    elif kind == TERM_BNODE:
        quad.g_bnode = term.text
    elif kind == TERM_LITERAL:
        _write_literal(term, quad.g_literal)


def encode_triple(
    terms: Iterable[object],
    term_encoder: TermEncoder,
    repeated_terms: list[object | None],
) -> None:
    """
    Encode one triple, adding its rows to the encoder's flow.

    Args:
        terms (Iterable[object]): original terms to encode
        term_encoder (TermEncoder): current encoder with lookup tables
        repeated_terms (list[object | None]): list of repeated terms.

    """
    term_encoder.new_epoch()
    encoded = term_encoder.encoded
    terms = iter(terms)
    # First encode the terms that changed, which adds rows for new lookup
    # entries, then add the triple's row after them and write the terms
    changed = 0
    for slot in range(3):
        term = next(terms)
        if not term_encoder.same_term(repeated_terms[slot], term):
            term_encoder.encode_spo(term, encoded[slot])
            repeated_terms[slot] = term
            changed |= 1 << slot
    triple = term_encoder.flow.new_row().triple
    if not changed:
        # Every term is repeated: the row is an empty triple
        triple.SetInParent()
    for slot in range(3):
        if changed & (1 << slot):
            write_term(encoded[slot], triple, slot)


def encode_quad(
    terms: Iterable[object],
    term_encoder: TermEncoder,
    repeated_terms: list[object | None],
) -> None:
    """
    Encode one quad, adding its rows to the encoder's flow.

    Args:
        terms (Iterable[object]): original terms to encode
        term_encoder (TermEncoder): current encoder with lookup tables
        repeated_terms (list[object | None]): list of repeated terms.

    """
    term_encoder.new_epoch()
    encoded = term_encoder.encoded
    terms = iter(terms)
    # See encode_triple()
    changed = 0
    for slot in range(3):
        term = next(terms)
        if not term_encoder.same_term(repeated_terms[slot], term):
            term_encoder.encode_spo(term, encoded[slot])
            repeated_terms[slot] = term
            changed |= 1 << slot
    g = next(terms)
    if not term_encoder.same_term(repeated_terms[3], g):
        term_encoder.encode_graph(g, encoded[3])
        repeated_terms[3] = g
        changed |= 1 << 3
    quad = term_encoder.flow.new_row().quad
    if not changed:
        quad.SetInParent()
    for slot in range(3):
        if changed & (1 << slot):
            write_term(encoded[slot], quad, slot)
    if changed & (1 << 3):
        write_graph(encoded[3], quad)


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
