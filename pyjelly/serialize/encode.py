from __future__ import annotations

from collections.abc import Iterable
from enum import IntEnum
from typing import Final, TypeAlias

from mypy_extensions import mypyc_attr

from pyjelly import jelly, options
from pyjelly.errors import JellyConformanceError
from pyjelly.serialize.flows import FrameFlow, ManualFrameFlow
from pyjelly.serialize.lookup import NO_BUDGET, LookupEncoder


def split_iri(iri_string: str) -> tuple[str, str]:
    """
    Split iri into prefix and name.

    Args:
        iri_string (str): full iri string.

    Returns:
        tuple[str, str]: iri's prefix and name.

    """
    # After the last "#", or else the last "/". Without either, all is the name.
    i = iri_string.rfind("#")
    if i < 0:
        i = iri_string.rfind("/")
    return iri_string[: i + 1], iri_string[i + 1 :]


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

# Base directions of language-tagged strings, as RdfBaseDirection values
_DIRECTIONS: Final = {
    "ltr": jelly.RDF_BASE_DIRECTION_LTR,
    "rtl": jelly.RDF_BASE_DIRECTION_RTL,
}

#: How deep triple terms can be nested in Jelly-RDF 1.2
MAX_TRIPLE_TERM_DEPTH: Final = 32


class EncodedTerm:
    """
    A term encoded for a statement, before it is written into the statement.

    A statement is written in two steps: first its terms are encoded (adding new
    lookup entries to the frame), then the terms are written into the statement's
    row (row layout) or columns (column layout). This way the row is filled in
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
        #: Literal: base direction, as an RdfBaseDirection value, 0 if none
        self.direction = 0
        #: Quoted triple, in the row layout
        self.triple: jelly.RdfTriple | None = None
        #: Triple term, in the column layout
        self.triple_term: jelly.RdfTripleTerm | None = None


def _budget(lookup: LookupEncoder, per_statement: int) -> int:
    size = lookup.lookup.max_size
    if not size:
        return NO_BUDGET
    return max(size - per_statement, 0)


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
        # Where rows for new lookup entries go in the row layout. A stream replaces
        # it with its own.
        self.flow: FrameFlow = ManualFrameFlow()
        self.iri_cache: dict[str, _CachedIri] = {}
        # Cleared when full, so that a long stream doesn't grow it without bound
        self.iri_cache_size = max(4 * lookup_preset.max_names, 1024)
        # The encoded subject, predicate, object and graph of the statement
        self.encoded = [EncodedTerm() for _ in range(4)]
        # Set by streams of Jelly-RDF 1.2. Then IRIs are encoded with the indices of
        # their entries, new entries are packed (see PackedEntries), and quoted
        # triples are encoded as triple terms.
        self.column_layout = False
        # The RDF version that a Jelly-RDF 1.2 stream declares, which limits its terms
        self.rdf_version: int = jelly.RDF_VERSION_UNSPECIFIED
        # The IRI sequence of the triple terms of a frame (only the object column can
        # have them): the indices of the last IRI. -1 makes the first IRI state its
        # prefix.
        self.triple_prefix_index = -1
        self.triple_name_index = 0
        # How deep the triple term being encoded is nested, and how many IRIs the
        # outermost one has so far
        self.triple_depth = 0
        self.triple_iris = 0

    def same_term(self, a: object, b: object) -> bool:
        """
        Return whether two terms are equal, for repeated terms.

        Integrations can make it faster for their term classes.
        """
        return a is b or a == b

    def new_epoch(self) -> None:
        """
        Start encoding a new statement (or other row with terms) in the row layout.

        Until the next epoch, the lookup entries it uses are not evicted: the row
        still refers to them.
        """
        self.names.lookup.new_epoch()
        self.prefixes.lookup.new_epoch()
        self.datatypes.lookup.new_epoch()

    def new_frame(self, iris: int, literals: int = 1) -> None:
        """
        Start encoding a new frame in the column layout.

        The frame is one epoch of the lookups: all its entries must fit in them,
        because they are all applied before its statements are read. So each lookup
        gets a budget, which leaves room for one more statement (see has_room()).

        Args:
            iris (int): the most IRIs that one statement can have.
            literals (int): the most literals that one statement can have. In a
                statement, only the object can be a literal.

        """
        self.names.lookup.new_epoch(_budget(self.names, iris))
        self.prefixes.lookup.new_epoch(_budget(self.prefixes, iris))
        self.datatypes.lookup.new_epoch(_budget(self.datatypes, literals))
        self.triple_prefix_index = -1
        self.triple_name_index = 0

    def has_room(self) -> bool:
        """Return whether one more statement fits in the frame, see new_frame()."""
        return (
            self.names.lookup.remaining >= 0
            and self.prefixes.lookup.remaining >= 0
            and self.datatypes.lookup.remaining >= 0
        )

    def reserve_iris(self, count: int) -> None:
        """Take IRIs that later statements may need out of the frame's budget."""
        self.names.lookup.remaining -= count
        self.prefixes.lookup.remaining -= count

    def iri_indices(self, iri_string: str) -> tuple[int, int]:
        """
        Return the indices of an IRI's prefix and name entries, adding new entries.

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
            prefix_index = cached.prefix_index
            if prefix_index and prefixes.lookup.keys[prefix_index] is cached.prefix:
                prefixes.lookup.touch(prefix_index)
            else:
                prefix_index = self._new_prefix_index(cached)

        names = self.names
        name_index = cached.name_index
        if name_index and names.lookup.keys[name_index] is cached.name:
            names.lookup.touch(name_index)
        else:
            name_index = self._new_name_index(cached)
        return prefix_index, name_index

    def _new_prefix_index(self, cached: _CachedIri) -> int:
        """Find or add the entry of an IRI's prefix, which is not cached."""
        prefixes = self.prefixes
        index, set_index = prefixes.encode_entry(cached.prefix)
        if set_index >= 0:
            if self.column_layout:
                prefixes.packed.add(index, cached.prefix)
            else:
                entry = self.flow.new_row().prefix
                entry.id = set_index
                entry.value = cached.prefix
        cached.prefix_index = index
        cached.prefix = prefixes.lookup.keys[index]
        return index

    def _new_name_index(self, cached: _CachedIri) -> int:
        """Find or add the entry of an IRI's name, which is not cached."""
        names = self.names
        index, set_index = names.encode_entry(cached.name)
        if set_index >= 0:
            if self.column_layout:
                names.packed.add(index, cached.name)
            else:
                entry = self.flow.new_row().name
                entry.id = set_index
                entry.value = cached.name
        cached.name_index = index
        cached.name = names.lookup.keys[index]
        return index

    def encode_iri_indices(self, iri_string: str) -> tuple[int, int]:
        """
        Encode lookup indices for IRI, as written in the row layout.

        The indices are relative to the IRI before (see RdfIri), so IRIs must be
        encoded in the order they are written.

        Args:
            iri_string (str): full iri in string format.

        Returns:
            tuple[int, int]: indices in prefix and name tables.

        """
        prefix_index, name_index = self.iri_indices(iri_string)
        prefixes = self.prefixes
        if prefixes.lookup.max_size:
            prefix_index = prefixes.prefix_index_to_write(
                prefix_index, empty=not prefixes.lookup.keys[prefix_index]
            )
        return prefix_index, self.names.name_index_to_write(name_index)

    def encode_iri(self, iri_string: str, out: EncodedTerm) -> None:
        """Encode iri into `out`, adding new lookup entries."""
        if self.column_layout:
            prefix_index, name_index = self.iri_indices(iri_string)
        else:
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
        direction: str | None = None,
        out: EncodedTerm,
    ) -> None:
        """
        Encode literal into `out`, adding a new datatype entry.

        Args:
            lex (str): lexical form/literal value
            language (str | None, optional): langtag. Defaults to None.
            datatype (str | None, optional): data type if
            it is a typed literal. Defaults to None.
            direction (str | None, optional): base direction of a language-tagged
                string, "ltr" or "rtl". Defaults to None.
            out (EncodedTerm): where to put the encoded literal.

        Raises:
            JellyConformanceError: if datatype specified while
                datatable is not used, or if the stream can't have the base
                direction.

        """
        datatype_index = 0
        direction_value = 0
        if language:
            if direction:
                direction_value = self.encode_direction(direction)
        elif datatype and datatype != options.STRING_DATATYPE_IRI:
            datatype_index = self.encode_datatype(datatype)
        out.kind = TERM_LITERAL
        out.text = lex
        out.lang = language or ""
        out.first = datatype_index
        out.direction = direction_value

    def encode_direction(self, direction: str) -> int:
        """Return the RdfBaseDirection value of a base direction."""
        value = _DIRECTIONS.get(direction, 0)
        if not value:
            msg = f"unknown base direction: {direction!r}, expected 'ltr' or 'rtl'"
            raise JellyConformanceError(msg)
        if not self.column_layout:
            msg = (
                "literals with a base direction can only be written in Jelly-RDF 1.2 "
                "(protocol version 3)"
            )
            raise JellyConformanceError(msg)
        if self.rdf_version == jelly.RDF_VERSION_1_1:
            msg = "the stream declares RDF 1.1, which has no base directions"
            raise JellyConformanceError(msg)
        return value

    def encode_datatype(self, datatype: str) -> int:
        """Return the index of a datatype, adding an entry if it's new."""
        datatypes = self.datatypes
        if datatypes.lookup.max_size == 0:
            msg = (
                f"can't encode literal with type {datatype}: "
                "datatype lookup cannot be used if disabled "
                "(its size was set to 0)"
            )
            raise JellyConformanceError(msg)
        index, set_index = datatypes.encode_entry(datatype)
        if set_index >= 0:
            if not self.column_layout:
                entry = self.flow.new_row().datatype
                entry.id = set_index
                entry.value = datatype
            elif datatype in (
                options.LANG_STRING_DATATYPE_IRI,
                options.DIR_LANG_STRING_DATATYPE_IRI,
            ):
                msg = f"a literal with the datatype {datatype} must have a language tag"
                raise JellyConformanceError(msg)
            else:
                datatypes.packed.add(index, datatype)
        return index

    def encode_quoted_triple(self, terms: Iterable[object], out: EncodedTerm) -> None:
        """
        Encode a quoted triple into `out`, adding new lookup entries.

        In the column layout, it is a triple term, see encode_triple_term().

        Notes:
            Although a triple, it is treated as a part of a statement.
            Repeated terms are not used when encoding quoted triples.

        Args:
            terms (Iterable[object]): triple terms to encode.
            out (EncodedTerm): where to put the encoded triple.

        """
        if self.column_layout:
            out.triple_term = self.encode_triple_term(terms)
            out.kind = TERM_TRIPLE
            return
        terms = iter(terms)
        triple = jelly.RdfTriple()
        for slot in range(3):
            term = EncodedTerm()
            self.encode_spo(next(terms), term)
            write_term(term, triple, slot)
        out.kind = TERM_TRIPLE
        out.triple = triple

    def encode_triple_term(self, terms: Iterable[object]) -> jelly.RdfTripleTerm:
        """
        Encode a triple term of the column layout, adding new lookup entries.

        Its IRIs are in the IRI sequence of the frame's triple terms, in the order
        subject, predicate, object. `triple_iris` counts the IRIs of the outermost
        triple term, nested ones included.

        Args:
            terms (Iterable[object]): the subject, predicate and object.

        Returns:
            jelly.RdfTripleTerm: the triple term.

        """
        depth = self.triple_depth
        if not depth:
            rdf_version = self.rdf_version
            if rdf_version in (jelly.RDF_VERSION_1_1, jelly.RDF_VERSION_1_2_BASIC):
                name = (
                    "RDF 1.1"
                    if rdf_version == jelly.RDF_VERSION_1_1
                    else "RDF 1.2 Basic"
                )
                msg = f"the stream declares {name}, which has no triple terms"
                raise JellyConformanceError(msg)
            self.triple_iris = 0
        elif depth >= MAX_TRIPLE_TERM_DEPTH:
            msg = (
                f"triple terms nested deeper than {MAX_TRIPLE_TERM_DEPTH} levels "
                "are not supported"
            )
            raise JellyConformanceError(msg)
        self.triple_depth = depth + 1
        try:
            triple = jelly.RdfTripleTerm()
            terms = iter(terms)
            term = EncodedTerm()
            self.encode_spo(next(terms), term)
            if term.kind == TERM_IRI:
                self._triple_term_iri(term, triple.s_iri)
            elif term.kind == TERM_BNODE:
                triple.s_bnode = term.text
            else:
                msg = "the subject of a triple term must be an IRI or a blank node"
                raise JellyConformanceError(msg)
            self.encode_spo(next(terms), term)
            if term.kind != TERM_IRI:
                msg = "the predicate of a triple term must be an IRI"
                raise JellyConformanceError(msg)
            self._triple_term_iri(term, triple.p_iri)
            self.encode_spo(next(terms), term)
            if term.kind == TERM_IRI:
                self._triple_term_iri(term, triple.o_iri)
            elif term.kind == TERM_BNODE:
                triple.o_bnode = term.text
            elif term.kind == TERM_LITERAL:
                _write_literal(term, triple.o_literal)
            else:
                assert term.triple_term is not None
                triple.o_triple_term.CopyFrom(term.triple_term)
        finally:
            self.triple_depth = depth
        return triple

    def _triple_term_iri(self, term: EncodedTerm, iri: jelly.RdfIri) -> None:
        prefix_index = term.first
        name_index = term.second
        # Assigning even 0 marks the IRI as set
        iri.prefix_id = 0 if prefix_index == self.triple_prefix_index else prefix_index
        iri.name_id = 0 if name_index == self.triple_name_index + 1 else name_index
        self.triple_prefix_index = prefix_index
        self.triple_name_index = name_index
        self.triple_iris += 1

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
        if term.direction:
            literal.direction = term.direction  # type: ignore[assignment]
    elif term.first:
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
    if options.is_row_layout(params.version):
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
    return jelly.RdfStreamOptions(
        stream_name=params.stream_name,
        physical_type=stream_types.physical_type,
        stream_type=options.stream_type_for(params, stream_types.logical_type),
        rdf_version=params.rdf_version,
        max_name_table_size=lookup_preset.max_names,
        max_prefix_table_size=lookup_preset.max_prefixes,
        max_datatype_table_size=lookup_preset.max_datatypes,
        version=params.version,
    )
