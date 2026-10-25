from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from enum import Enum, auto
from typing import Any, ClassVar, Final, NamedTuple, cast
from typing_extensions import Never

from mypy_extensions import mypyc_attr

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.options import (
    MAX_DATATYPE_LOOKUP_SIZE,
    MAX_NAME_LOOKUP_SIZE,
    MAX_PREFIX_LOOKUP_SIZE,
    MAX_VERSION,
    MIN_COLUMN_NAME_LOOKUP_SIZE,
    MIN_NAME_LOOKUP_SIZE,
    MIN_VERSION,
    VERSION_1_1,
    LookupPreset,
    StreamParameters,
    StreamTypes,
    is_row_layout,
)
from pyjelly.parse.columns import MESSAGE_END, ColumnDecoder
from pyjelly.parse.lookup import LookupDecoder

TermHandler = Callable[[Any], Any | None]
RdfStreamOptions = jelly.RdfStreamOptions

# Methods called through the message classes rather than on messages: upb looks
# up any attribute of a message as a field name first, even a method's name
_ROW_WHICH_ONEOF: Final = jelly.RdfStreamRow.WhichOneof
_LITERAL_WHICH_ONEOF: Final = jelly.RdfLiteral.WhichOneof
_TRIPLE_LIST_FIELDS: Final = jelly.RdfTriple.ListFields
_QUAD_LIST_FIELDS: Final = jelly.RdfQuad.ListFields

# The term positions of a statement, for error messages
_POSITIONS: Final = ("subject", "predicate", "object", "graph")
_GRAPH: Final = 3
# The kinds of term fields, in the order of their numbers in each position
_IRI: Final = 0
_BNODE: Final = 1
_LITERAL_OR_DEFAULT_GRAPH: Final = 2
# Marks a repeated term that was never set
_MISSING: Final = object()

# How many literal terms the decoder keeps for reuse. When the cache is full, it
# is emptied: simpler and cheaper than evicting the least recently used ones
LITERAL_CACHE_SIZE = 16384


class ParsingMode(Enum):
    """
    Specifies how jelly frames should be treated.

    Modes:
    FLAT
        Yield all frames as one Graph or Dataset.
    GROUPED
        Yield one Graph/Dataset per frame (grouped parsing).
    """

    FLAT = auto()
    GROUPED = auto()


@mypyc_attr(allow_interpreted_subclasses=True)
class ParserOptions(NamedTuple):
    stream_types: StreamTypes
    lookup_preset: LookupPreset
    params: StreamParameters


_PHYSICAL_TYPES: Final = (
    jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
    jelly.PHYSICAL_STREAM_TYPE_QUADS,
    jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
)
_LOGICAL_TYPES: Final = frozenset(jelly.LogicalStreamType.values())
_RDF_VERSIONS: Final = frozenset(jelly.RdfVersion.values())


def check_stream_options(options: jelly.RdfStreamOptions) -> None:
    """
    Check that the parser can read a stream with these options.

    As in Jelly-JVM, the fields of one layout must not be set in a stream of the
    other, and the lookups must not be larger than the parser allows (see the
    MAX_*_LOOKUP_SIZE constants).

    Raises:
        JellyConformanceError: if it can't.

    """
    version = options.version
    if not MIN_VERSION <= version <= MAX_VERSION:
        msg = (
            f"unsupported protocol version {version}, "
            f"expected {MIN_VERSION} to {MAX_VERSION}"
        )
        raise JellyConformanceError(msg)
    physical_type = options.physical_type
    if physical_type not in _PHYSICAL_TYPES:
        msg = f"unknown physical stream type: {physical_type}"
        raise JellyConformanceError(msg)
    if is_row_layout(version):
        if options.stream_type or options.rdf_version:
            msg = (
                "the stream type and RDF version are only valid in Jelly-RDF 1.2 "
                "streams (protocol version 3)"
            )
            raise JellyConformanceError(msg)
        if options.logical_type not in _LOGICAL_TYPES:
            msg = f"unknown logical stream type: {options.logical_type}"
            raise JellyConformanceError(msg)
        min_names = MIN_NAME_LOOKUP_SIZE
    else:
        if options.generalized_statements or options.rdf_star or options.logical_type:
            msg = (
                "generalized statements, RDF-star and logical stream types are only "
                "valid in Jelly-RDF 1.0 and 1.1 streams (protocol versions 1 and 2)"
            )
            raise JellyConformanceError(msg)
        if physical_type == jelly.PHYSICAL_STREAM_TYPE_GRAPHS:
            msg = "Jelly-RDF 1.2 streams cannot be of the GRAPHS physical type"
            raise JellyConformanceError(msg)
        if options.stream_type not in (
            jelly.RDF_STREAM_TYPE_FLAT,
            jelly.RDF_STREAM_TYPE_MESSAGES,
        ):
            msg = f"unknown stream type: {options.stream_type}"
            raise JellyConformanceError(msg)
        if options.rdf_version not in _RDF_VERSIONS:
            msg = f"unknown RDF version: {options.rdf_version}"
            raise JellyConformanceError(msg)
        min_names = MIN_COLUMN_NAME_LOOKUP_SIZE
    _check_size("name", options.max_name_table_size, min_names, MAX_NAME_LOOKUP_SIZE)
    _check_size("prefix", options.max_prefix_table_size, 0, MAX_PREFIX_LOOKUP_SIZE)
    _check_size(
        "datatype", options.max_datatype_table_size, 0, MAX_DATATYPE_LOOKUP_SIZE
    )


def _check_size(lookup: str, size: int, smallest: int, largest: int) -> None:
    if not smallest <= size <= largest:
        msg = (
            f"the {lookup} lookup size is {size}, "
            f"but it must be from {smallest} to {largest}"
        )
        raise JellyConformanceError(msg)


def parser_options(
    options: jelly.RdfStreamOptions, *, delimited: bool
) -> ParserOptions:
    """
    Return the parser options of a stream, after checking them.

    Args:
        options (jelly.RdfStreamOptions): the stream options
        delimited (bool): whether the stream is delimited

    Returns:
        ParserOptions: types, lookup sizes and other parameters of the stream

    Raises:
        JellyConformanceError: if the parser can't read the stream.

    """
    check_stream_options(options)
    return ParserOptions(
        stream_types=StreamTypes(
            physical_type=options.physical_type,
            logical_type=_logical_type(options),
        ),
        lookup_preset=LookupPreset(
            max_names=options.max_name_table_size,
            max_prefixes=options.max_prefix_table_size,
            max_datatypes=options.max_datatype_table_size,
        ),
        params=StreamParameters(
            stream_name=options.stream_name,
            generalized_statements=options.generalized_statements,
            rdf_star=options.rdf_star,
            version=options.version,
            delimited=delimited,
            namespace_declarations=options.version >= VERSION_1_1,
            stream_type=options.stream_type,
            rdf_version=options.rdf_version,
        ),
    )


def _logical_type(options: jelly.RdfStreamOptions) -> jelly.LogicalStreamType:
    """
    Return the logical type of a stream.

    Jelly-RDF 1.2 streams have none, but the specification says which they
    correspond to: a FLAT stream to a flat one, a stream of RDF Messages to a
    stream of graphs or datasets.
    """
    if is_row_layout(options.version):
        return options.logical_type
    quads = options.physical_type == jelly.PHYSICAL_STREAM_TYPE_QUADS
    if options.stream_type == jelly.RDF_STREAM_TYPE_MESSAGES:
        if quads:
            return jelly.LOGICAL_STREAM_TYPE_DATASETS
        return jelly.LOGICAL_STREAM_TYPE_GRAPHS
    if quads:
        return jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS
    return jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES


def options_from_frame(
    frame: jelly.RdfStreamFrame,
    *,
    delimited: bool,
) -> ParserOptions:
    """
    Fill stream options based on the options row.

    Args:
        frame (jelly.RdfStreamFrame): first non-empty frame from the stream
        delimited (bool): derived delimited flag

    Returns:
        ParserOptions: filled options with types/lookups/stream parameters information

    Raises:
        JellyConformanceError: if the parser can't read the stream.

    """
    row = frame.rows[0]
    if _ROW_WHICH_ONEOF(row, "row") != "options":
        msg = "the stream does not start with the stream options"
        raise JellyConformanceError(msg)
    return parser_options(row.options, delimited=delimited)


def messages(rows: Iterable[Iterable[Any]]) -> Iterator[list[Any]]:
    """
    Group the rows of a stream of RDF Messages by message.

    Args:
        rows (Iterable[Iterable[Any]]): the rows that Decoder.iter_rows() yields
            for each frame, and Decoder.end_of_stream()

    Yields:
        list[Any]: the namespace declarations and statements of each message

    """
    message: list[Any] = []
    for frame_rows in rows:
        for row in frame_rows:
            if row is MESSAGE_END:
                yield message
                message = []
            else:
                message.append(row)


def grouped_rows(
    rows: Iterable[Iterable[Any]], options: ParserOptions
) -> Iterable[Iterable[Any]]:
    """
    Return the rows of each frame, or of each message in a stream of RDF Messages.

    Args:
        rows (Iterable[Iterable[Any]]): the rows that Decoder.iter_rows() yields
            for each frame, and Decoder.end_of_stream()
        options (ParserOptions): the stream's options

    """
    if options.params.stream_type == jelly.RDF_STREAM_TYPE_MESSAGES:
        return messages(rows)
    return rows


def flat_rows(rows: Iterable[Iterable[Any]], options: ParserOptions) -> Iterator[Any]:
    """
    Return the rows of all frames, without the ends of RDF Messages.

    Args:
        rows (Iterable[Iterable[Any]]): the rows that Decoder.iter_rows() yields
            for each frame, and Decoder.end_of_stream()
        options (ParserOptions): the stream's options

    """
    if options.params.stream_type == jelly.RDF_STREAM_TYPE_MESSAGES:
        for frame_rows in rows:
            for row in frame_rows:
                if row is not MESSAGE_END:
                    yield row
    else:
        for frame_rows in rows:
            yield from frame_rows


def _adapter_missing(feature: str, *, stream_types: StreamTypes) -> Never:
    physical_type_name = jelly.PhysicalStreamType.Name(stream_types.physical_type)
    logical_type_name = jelly.LogicalStreamType.Name(stream_types.logical_type)
    msg = (
        f"adapter with {physical_type_name} and {logical_type_name} "
        f"does not implement {feature}"
    )
    raise NotImplementedError(msg)


@mypyc_attr(allow_interpreted_subclasses=True)
class Adapter(metaclass=ABCMeta):
    def __init__(
        self, options: ParserOptions, parsing_mode: ParsingMode = ParsingMode.FLAT
    ) -> None:
        self.options = options
        self.parsing_mode = parsing_mode

    # Obligatory abstract methods--all adapters must implement these
    @abstractmethod
    def iri(self, iri: str) -> Any:
        """Return an IRI term. The decoder reuses it, so it must be immutable."""
        raise NotImplementedError

    @abstractmethod
    def default_graph(self) -> Any:
        raise NotImplementedError

    @abstractmethod
    def bnode(self, bnode: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    def literal(
        self,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
    ) -> Any:
        """Return a literal term. The decoder reuses it, so it must be immutable."""
        raise NotImplementedError

    def directional_literal(self, lex: str, language: str, direction: str) -> Any:  # noqa: ARG002
        """Return a language-tagged string with a base direction, "ltr" or "rtl"."""
        _adapter_missing(
            "decoding literals with a base direction",
            stream_types=self.options.stream_types,
        )

    # Optional abstract methods--not required to be implemented by all adapters
    def triple(self, terms: Iterable[Any]) -> Any:  # noqa: ARG002
        _adapter_missing("decoding triples", stream_types=self.options.stream_types)

    def quad(self, terms: Iterable[Any]) -> Any:  # noqa: ARG002
        _adapter_missing("decoding quads", stream_types=self.options.stream_types)

    def graph_start(self, graph_id: Any) -> Any:  # noqa: ARG002
        _adapter_missing(
            "decoding graph start markers", stream_types=self.options.stream_types
        )

    def graph_end(self) -> Any:
        _adapter_missing(
            "decoding graph end markers", stream_types=self.options.stream_types
        )

    def namespace_declaration(self, name: str, iri: Any) -> Any:  # noqa: ARG002
        """Return a namespace declaration. `iri` is a term made by iri()."""
        _adapter_missing(
            "decoding namespace declarations",
            stream_types=self.options.stream_types,
        )

    def quoted_triple(self, terms: Iterable[Any]) -> Any:  # noqa: ARG002
        _adapter_missing(
            "decoding quoted triple", stream_types=self.options.stream_types
        )

    def frame(self) -> Any:
        return None


class _CachedIri:
    """The IRI term last decoded for a name table entry, and what it was made of."""

    def __init__(self, name: str, prefix: str, term: Any) -> None:
        self.name = name
        self.prefix = prefix
        self.term = term


@mypyc_attr(allow_interpreted_subclasses=True)
class Decoder:
    _TERM_HANDLER_NAMES: ClassVar[Mapping[type[Any], str]] = {
        jelly.RdfIri: "decode_iri",
        str: "decode_bnode",
        jelly.RdfLiteral: "decode_literal",
        jelly.RdfDefaultGraph: "decode_default_graph",
        jelly.RdfTriple: "decode_quoted_triple",
    }

    def __init__(self, adapter: Adapter) -> None:
        """
        Initialize decoder.

        Initializes decoder with lookup tables of preset sizes and an
        integration-dependent adapter.

        Args:
            adapter (Adapter): integration-dependent adapter that specifies terms
            conversion to specific objects, framing,
            namespace declarations, and graphs/datasets forming.

        """
        self.adapter = adapter
        self.new_lookups()
        # The last s, p, o and g terms, for repeated terms
        self.repeated_terms: list[object] = [_MISSING] * 4
        # Literals often repeat (e.g., the same values in many entities), and
        # making one is often slow (rdflib parses the value)
        self.literal_cache: dict[tuple[str, str | None, str | None], Any] = {}

        self.term_handlers: dict[type[Any], TermHandler] = {
            t: getattr(self, name) for t, name in self._TERM_HANDLER_NAMES.items()
        }
        #: Decodes the frames of Jelly-RDF 1.2 streams, None for older ones
        self.column_decoder: ColumnDecoder | None = None
        if not is_row_layout(self.options.params.version):
            self.column_decoder = ColumnDecoder(self)

    def new_lookups(self) -> None:
        """Make empty lookups, of the sizes in the options."""
        lookup_preset = self.options.lookup_preset
        self.names = LookupDecoder(lookup_size=lookup_preset.max_names)
        self.prefixes = LookupDecoder(lookup_size=lookup_preset.max_prefixes)
        self.datatypes = LookupDecoder(lookup_size=lookup_preset.max_datatypes)
        # One IRI term per name table entry, as in Jelly-JVM: an IRI is mostly
        # used with the same prefix again, so its term can be reused. Index 0 is
        # unused, as table indices start from 1
        self.iri_cache: list[_CachedIri | None] = [None] * (lookup_preset.max_names + 1)

    @property
    def options(self) -> ParserOptions:
        return self.adapter.options

    def restate_options(self, options: jelly.RdfStreamOptions) -> None:
        """Use restated options of a Jelly-RDF 1.2 stream, which empty the lookups."""
        self.adapter.options = parser_options(
            options, delimited=self.options.params.delimited
        )
        self.new_lookups()

    def end_of_stream(self) -> Iterator[Any]:
        """Yield what ends with the stream: the last RDF Message, if any."""
        column_decoder = self.column_decoder
        if column_decoder is not None:
            yield from column_decoder.end_of_stream()

    def iter_rows(self, frame: jelly.RdfStreamFrame) -> Iterator[Any]:
        """
        Iterate through rows in the frame.

        Args:
            frame (jelly.RdfStreamFrame): jelly frame
        Yields:
            Iterator[Any]: decoded rows, and in streams of RDF Messages, MESSAGE_END
                at the end of each message

        """
        column_decoder = self.column_decoder
        if column_decoder is not None:
            yield from column_decoder.iter_frame(frame)
            return
        # Rows are told apart by the name of their field, most common ones first
        for row_owner in frame.rows:
            kind: str | None = _ROW_WHICH_ONEOF(row_owner, "row")
            if kind == "triple":
                yield self.decode_triple(row_owner.triple)
            elif kind == "quad":
                yield self.decode_quad(row_owner.quad)
            elif kind == "name":
                self.ingest_name_entry(row_owner.name)
            elif kind == "prefix":
                self.ingest_prefix_entry(row_owner.prefix)
            elif kind == "datatype":
                self.ingest_datatype_entry(row_owner.datatype)
            elif kind == "namespace":
                yield self.decode_namespace_declaration(row_owner.namespace)
            else:
                self.decode_other_row(row_owner, kind)

    def decode_other_row(self, row_owner: jelly.RdfStreamRow, kind: str | None) -> None:
        """Decode a row that gives nothing to yield: graph start or end, options."""
        if kind == "graph_start":
            self.decode_graph_start(row_owner.graph_start)
        elif kind == "graph_end":
            self.decode_graph_end(row_owner.graph_end)
        elif kind == "options":
            self.validate_stream_options(row_owner.options)
        else:
            msg = f"decoder not implemented for row {kind}"
            raise TypeError(msg)

    def validate_stream_options(self, options: jelly.RdfStreamOptions) -> None:
        stream_types, lookup_preset, params = self.options
        assert stream_types.physical_type == options.physical_type
        assert stream_types.logical_type == options.logical_type
        assert params.stream_name == options.stream_name
        assert params.version >= options.version
        assert lookup_preset.max_prefixes == options.max_prefix_table_size
        assert lookup_preset.max_datatypes == options.max_datatype_table_size
        assert lookup_preset.max_names == options.max_name_table_size

    def ingest_prefix_entry(self, entry: jelly.RdfPrefixEntry) -> None:
        """
        Update prefix lookup table based on the table entry.

        Args:
            entry (jelly.RdfPrefixEntry): prefix message, containing id and value

        """
        self.prefixes.assign_entry(index=entry.id, value=entry.value)

    def ingest_name_entry(self, entry: jelly.RdfNameEntry) -> None:
        """
        Update name lookup table based on the table entry.

        Args:
            entry (jelly.RdfNameEntry): name message, containing id and value

        """
        self.names.assign_entry(index=entry.id, value=entry.value)

    def ingest_datatype_entry(self, entry: jelly.RdfDatatypeEntry) -> None:
        """
        Update datatype lookup table based on the table entry.

        Args:
            entry (jelly.RdfDatatypeEntry): name message, containing id and value

        """
        self.datatypes.assign_entry(index=entry.id, value=entry.value)

    def decode_term(self, term: Any) -> Any:
        """
        Decode a term based on its type: IRI/literal/BN/default graph.

        Notes: requires a custom adapter with implemented methods for terms decoding.

        Args:
            term (Any): IRI/literal/BN(string)/Default graph message

        Raises:
            TypeError: raises error if no handler for the term is found

        Returns:
            Any: decoded term (currently, rdflib objects, e.g., rdflib.term.URIRef)

        """
        decode_term = self.term_handlers.get(type(term))
        if decode_term is None:
            msg = f"decoder not implemented for {type(term)}"
            raise TypeError(msg) from None
        return decode_term(term)

    def decode_iri(self, iri: jelly.RdfIri) -> Any:
        """
        Decode RdfIri message to IRI using a custom adapter.

        The term is reused if the same name and prefix entries were decoded
        before, so the adapter's IRI terms must be immutable.

        Args:
            iri (jelly.RdfIri): RdfIri message

        Returns:
            Any: IRI, based on adapter implementation, e.g., rdflib.term.URIRef

        """
        names = self.names
        name_index = iri.name_id or names.last_reused_index + 1
        name = names.decode_name_term_index(name_index)
        prefix = self.prefixes.decode_prefix_term_index(iri.prefix_id)
        # The lookup tables keep the strings they were given, so the same objects
        # mean the same entries (or at least equal strings, which is just as good)
        cached = self.iri_cache[name_index]
        if cached is not None and cached.name is name and cached.prefix is prefix:
            return cached.term
        return self.new_iri(name_index, name, prefix)

    def iri_term(self, prefix_index: int, name_index: int) -> Any:
        """Return the IRI made of the entries at the indices (prefix 0 is none)."""
        name = self.names.at(name_index)
        prefix = self.prefixes.at(prefix_index) if prefix_index else ""
        cached = self.iri_cache[name_index]
        if cached is not None and cached.name is name and cached.prefix is prefix:
            return cached.term
        return self.new_iri(name_index, name, prefix)

    def new_iri(self, name_index: int, name: str, prefix: str) -> Any:
        """Make an IRI term, and cache it for its name entry."""
        term = self.adapter.iri(iri=prefix + name)
        self.iri_cache[name_index] = _CachedIri(name, prefix, term)
        return term

    def decode_default_graph(self, _: jelly.RdfDefaultGraph) -> Any:
        return self.adapter.default_graph()

    def decode_bnode(self, bnode: str) -> Any:
        """
        Decode string message to blank node (BN) using a custom adapter.

        Args:
            bnode (str): blank node id

        Returns:
            Any: blank node object from the custom adapter

        """
        return self.adapter.bnode(bnode)

    def decode_literal(self, literal: jelly.RdfLiteral) -> Any:
        """
        Decode RdfLiteral to literal based on custom adapter implementation.

        Recently decoded literals are reused, so the adapter's literal terms must
        be immutable.

        Args:
            literal (jelly.RdfLiteral): RdfLiteral message

        Returns:
            Any: literal returned by the custom adapter

        """
        language = datatype = None
        kind = _LITERAL_WHICH_ONEOF(literal, "literalKind")
        if kind == "langtag":
            language = literal.langtag or None
        elif kind == "datatype" and self.datatypes.lookup_size:
            datatype = self.datatypes.decode_datatype_term_index(literal.datatype)
        lex = literal.lex
        term = self.literal_cache.get((lex, language, datatype))
        if term is None:
            term = self._new_literal(lex, language, datatype)
        return term

    def literal_term(
        self,
        lex: str,
        language: str | None,
        datatype: str | None,
        direction: str | None,
    ) -> Any:
        """Return a literal, reusing a recently made one if it's the same."""
        if direction:
            assert language
            return self.adapter.directional_literal(lex, language, direction)
        term = self.literal_cache.get((lex, language, datatype))
        if term is None:
            term = self._new_literal(lex, language, datatype)
        return term

    def _new_literal(self, lex: str, language: str | None, datatype: str | None) -> Any:
        """Make a literal term, and cache it."""
        term = self.adapter.literal(lex=lex, language=language, datatype=datatype)
        cache = self.literal_cache
        if len(cache) >= LITERAL_CACHE_SIZE:
            cache.clear()
        cache[(lex, language, datatype)] = term
        return term

    def decode_namespace_declaration(
        self,
        declaration: jelly.RdfNamespaceDeclaration,
    ) -> Any:
        iri = self.decode_iri(declaration.value)
        return self.adapter.namespace_declaration(declaration.name, iri)

    def decode_graph_start(self, graph_start: jelly.RdfGraphStart) -> Any:
        term = getattr(graph_start, graph_start.WhichOneof("graph"))
        return self.adapter.graph_start(self.decode_term(term))

    def decode_graph_end(self, _: jelly.RdfGraphEnd) -> Any:
        return self.adapter.graph_end()

    def decode_statement(self, fields: list[Any], size: int) -> list[Any]:
        """
        Decode the terms of a triple or quad message.

        Notes: also updates the repeated terms

        Args:
            fields (list[Any]): the message's fields, from ListFields()
            size (int): 3 for a triple, 4 for a quad

        Raises:
            ValueError: if a missing repeated term is encountered

        Returns:
            Any: a list of decoded terms

        """
        repeated = self.repeated_terms
        terms = repeated[:size]
        # The fields come in the order of their numbers, so the terms are decoded
        # in s, p, o, g order. Each term has four fields, numbered from 1:
        # IRI, blank node, literal and quoted triple (s_iri = 1, ..., o_iri = 9),
        # but the graph's are IRI, blank node, default graph and literal
        for field, value in fields:
            number: int = field.number - 1
            position = number >> 2
            kind = number & 3
            if kind == _IRI:
                term = self.decode_iri(value)
            elif kind == _BNODE:
                term = self.adapter.bnode(value)
            elif kind == _LITERAL_OR_DEFAULT_GRAPH:
                if position == _GRAPH:
                    term = self.adapter.default_graph()
                else:
                    term = self.decode_literal(value)
            elif position == _GRAPH:
                term = self.decode_literal(value)
            else:
                term = self.decode_quoted_triple(value)
            terms[position] = term
            repeated[position] = term
        for i in range(size):
            if terms[i] is _MISSING:
                msg = f"missing repeated term {_POSITIONS[i]}"
                raise ValueError(msg)
        return terms

    def decode_triple(self, triple: jelly.RdfTriple) -> Any:
        terms = self.decode_statement(cast("list[Any]", _TRIPLE_LIST_FIELDS(triple)), 3)
        return self.adapter.triple(terms)

    def decode_quoted_triple(self, triple: jelly.RdfTriple) -> Any:
        oneofs: Sequence[str] = ("subject", "predicate", "object")
        terms = []
        for oneof in oneofs:
            field = triple.WhichOneof(oneof)
            if field:
                jelly_term = getattr(triple, field)
                decoded_term = self.decode_term(jelly_term)
            else:
                msg = "repeated terms are not allowed in quoted triples"
                raise ValueError(msg)
            terms.append(decoded_term)
        return self.adapter.quoted_triple(terms)

    def decode_quad(self, quad: jelly.RdfQuad) -> Any:
        terms = self.decode_statement(cast("list[Any]", _QUAD_LIST_FIELDS(quad)), 4)
        return self.adapter.quad(terms)
