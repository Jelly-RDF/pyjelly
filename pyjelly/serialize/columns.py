"""
The column layout of Jelly-RDF 1.2: the statements of a frame, stored column by column.

Each statement position (subject, predicate, object, graph) is one column. A run of
equal terms in consecutive statements is stored once. The terms are split by type
into lists, and the layout tokens tell where a term takes more than one cell, and
where cells are unbound (the default graph).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Final

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.serialize.encode import (
    TERM_BNODE,
    TERM_DEFAULT_GRAPH,
    TERM_IRI,
    TERM_LITERAL,
    EncodedTerm,
    TermEncoder,
)

# Types of the run values of a column, as stored in its kinds
KIND_IRI: Final = 0
KIND_LITERAL: Final = 1
KIND_BNODE: Final = 2
KIND_TRIPLE: Final = 3

# Which types of terms a column allows, as bits 1 << KIND_*
_IRIS: Final = 1 << KIND_IRI
_RESOURCES: Final = (1 << KIND_IRI) | (1 << KIND_BNODE)
_ANY: Final = 0b1111

# A layout token is (skip << 5) | (unbound << 4) | len_code. A len_code of 15 means
# that the rest of the length follows in the next value.
TOKEN_SKIP_SHIFT: Final = 5
TOKEN_UNBOUND: Final = 1 << 4
MAX_INLINE_LENGTH: Final = 15

#: The most statements a frame can have
MAX_ROWS: Final = (1 << 27) - 1

_KIND_NAMES: Final = ("an IRI", "a literal", "a blank node", "a triple term")


def _kind_of(term: EncodedTerm) -> int:
    kind = term.kind
    if kind == TERM_IRI:
        return KIND_IRI
    if kind == TERM_LITERAL:
        return KIND_LITERAL
    if kind == TERM_BNODE:
        return KIND_BNODE
    return KIND_TRIPLE


class Column:
    """The cells of one statement position in the frame being built."""

    def __init__(self, allowed: int, position: str) -> None:
        # Which types of terms the column allows (bits 1 << KIND_*), and its name
        # in error messages
        self.allowed = allowed
        self.position = position
        # The current run: its term, and how many cells it has. A run of the
        # default graph is a run of unbound cells
        self.run_term: object = None
        self.run_length = 0
        self.run_unbound = False
        # Run values that took one cell each since the last layout token
        self.skip = 0
        self.layouts: list[int] = []
        self.value_count = 0
        # The types of the run values (bits 1 << KIND_*), and once there is more
        # than one, the type of each value, packed as written: 2 bits each, four
        # in a byte, from the lowest bits
        self.kinds_used = 0
        self.kinds: list[int] | None = None
        # IRIs: name indices (0 if the previous one + 1) and prefix indices (0 if
        # the same as the previous one). If all prefix indices are the same, the
        # first is written alone.
        self.name_ids: list[int] = []
        self.prefix_ids: list[int] = []
        self.last_name_id = 0
        self.last_prefix_id = -1
        self.prefix_ids_differ = False
        # Literals: lexical forms and literal kinds, and the language tags they
        # refer to, with their base directions. If all literal kinds are the same,
        # the first is written alone.
        self.lex_values: list[str] = []
        self.literal_kinds: list[int] = []
        self.last_literal_kind = -1
        self.literal_kinds_differ = False
        self.langtags: list[str] = []
        self.langtag_directions: list[int] = []
        # Index of each language tag in langtags, one dict per base direction
        self.langtag_indices: list[dict[str, int]] = [{}, {}, {}]
        self.bnodes: list[str] = []
        self.triple_terms: list[jelly.RdfTripleTerm] = []

    def add_value(self, term: EncodedTerm) -> None:
        """Add a run value: a term that is not the same as the one before."""
        kind = _kind_of(term)
        bit = 1 << kind
        used = self.kinds_used
        if used != bit:
            if not used & bit:
                self._new_kind(kind, used)
            kinds = self.kinds
            if kinds is not None:
                shift = (self.value_count & 3) << 1
                if shift:
                    kinds[-1] |= kind << shift
                else:
                    kinds.append(kind)
        self.value_count += 1
        if kind == KIND_IRI:
            self._add_iri(term.first, term.second)
        elif kind == KIND_LITERAL:
            self._add_literal(term)
        elif kind == KIND_BNODE:
            self.bnodes.append(term.text)
        else:
            assert term.triple_term is not None
            self.triple_terms.append(term.triple_term)

    def _add_iri(self, prefix_id: int, name_id: int) -> None:
        self.name_ids.append(0 if name_id == self.last_name_id + 1 else name_id)
        self.last_name_id = name_id
        if prefix_id == self.last_prefix_id:
            self.prefix_ids.append(0)
        else:
            if self.last_prefix_id >= 0:
                self.prefix_ids_differ = True
            self.prefix_ids.append(prefix_id)
            self.last_prefix_id = prefix_id

    def _add_literal(self, term: EncodedTerm) -> None:
        self.lex_values.append(term.text)
        if term.lang:
            literal_kind = 2 * self._langtag_index(term.lang, term.direction) + 2
        elif term.first:
            literal_kind = 2 * term.first - 1
        else:
            literal_kind = 0
        if literal_kind != self.last_literal_kind:
            if self.last_literal_kind >= 0:
                self.literal_kinds_differ = True
            self.last_literal_kind = literal_kind
        self.literal_kinds.append(literal_kind)

    def _new_kind(self, kind: int, used: int) -> None:
        """Note the first value of a type, after values of other types."""
        if not self.allowed & (1 << kind):
            msg = (
                f"the {self.position} of a statement cannot be {_KIND_NAMES[kind]} "
                "in Jelly-RDF 1.2. Generalized statements, and quoted triples "
                "anywhere but in the object, can only be written in Jelly-RDF 1.1 "
                "(protocol version 2)"
            )
            raise JellyConformanceError(msg)
        if used and self.kinds is None:
            # Every value so far had the one type
            count = self.value_count
            full_byte = ((used & -used).bit_length() - 1) * 0x55
            kinds = [full_byte] * (count >> 2)
            if count & 3:
                kinds.append(full_byte & ((1 << ((count & 3) << 1)) - 1))
            self.kinds = kinds
        self.kinds_used = used | (1 << kind)

    def _langtag_index(self, langtag: str, direction: int) -> int:
        indices = self.langtag_indices[direction]
        index = indices.get(langtag)
        if index is None:
            index = indices[langtag] = len(self.langtags)
            self.langtags.append(langtag)
            self.langtag_directions.append(direction)
        return index

    def end_run(self) -> None:
        """End the current run, adding a layout token if it isn't one cell long."""
        length = self.run_length
        if not length:
            return
        if self.run_unbound:
            self._token(TOKEN_UNBOUND, length - 1)
            self.run_unbound = False
        elif length > 1:
            self._token(0, length - 2)
        else:
            self.skip += 1
        self.run_length = 0

    def _token(self, unbound: int, length: int) -> None:
        code = min(length, MAX_INLINE_LENGTH)
        self.layouts.append((self.skip << TOKEN_SKIP_SHIFT) | unbound | code)
        if code == MAX_INLINE_LENGTH:
            self.layouts.append(length - MAX_INLINE_LENGTH)
        self.skip = 0

    def write(self, column: jelly.RdfColumn) -> None:
        """Write the column into its message, and empty it for the next frame."""
        if self.run_unbound:
            # Unbound cells at the end are left out: the parser pads the column
            self.run_length = 0
            self.run_unbound = False
        else:
            self.end_run()
        if self.layouts:
            column.layouts.extend(self.layouts)
        if self.name_ids:
            self._write_iris(column)
        if self.lex_values:
            self._write_literals(column)
        if self.bnodes:
            column.bnodes.extend(self.bnodes)
        if self.triple_terms:
            column.triple_terms.extend(self.triple_terms)
        if self.kinds is not None:
            column.kinds = bytes(self.kinds)
        self.clear()

    def _write_iris(self, column: jelly.RdfColumn) -> None:
        column.name_ids.extend(self.name_ids)
        if self.prefix_ids_differ:
            column.prefix_ids.extend(self.prefix_ids)
        elif self.last_prefix_id:
            column.prefix_ids.append(self.last_prefix_id)

    def _write_literals(self, column: jelly.RdfColumn) -> None:
        column.lex_values.extend(self.lex_values)
        if self.literal_kinds_differ:
            column.literal_kinds.extend(self.literal_kinds)
        elif self.last_literal_kind:
            column.literal_kinds.append(self.last_literal_kind)
        if self.langtags:
            column.langtags.extend(self.langtags)
            if any(self.langtag_directions):
                column.langtag_directions.extend(self.langtag_directions)

    def clear(self) -> None:
        self.run_term = None
        self.run_length = 0
        self.run_unbound = False
        self.skip = 0
        self.layouts = []
        self.value_count = 0
        self.kinds_used = 0
        self.kinds = None
        self.name_ids = []
        self.prefix_ids = []
        self.last_name_id = 0
        self.last_prefix_id = -1
        self.prefix_ids_differ = False
        self.lex_values = []
        self.literal_kinds = []
        self.last_literal_kind = -1
        self.literal_kinds_differ = False
        if self.langtags:
            self.langtags = []
            self.langtag_directions = []
            self.langtag_indices = [{}, {}, {}]
        self.bnodes = []
        self.triple_terms = []


class ColumnBatch:
    """
    The column batch of the frame being built, in a stream of Jelly-RDF 1.2.

    The terms are encoded by the stream's TermEncoder. The frame flow decides when
    the frame ends (see FrameFlow.to_stream_frame()).

    Args:
        encoder (TermEncoder): encoder of the stream's terms.
        quads (bool): whether the statements are quads.
        messages (bool): whether the stream is a stream of RDF Messages.

    """

    def __init__(self, encoder: TermEncoder, *, quads: bool, messages: bool) -> None:
        self.encoder = encoder
        self.messages = messages
        self.subjects = Column(_RESOURCES, "subject")
        self.predicates = Column(_IRIS, "predicate")
        self.objects = Column(_ANY, "object")
        self.graphs: Column | None = Column(_RESOURCES, "graph") if quads else None
        self.encoded = EncodedTerm()
        self.row_count = 0
        # The statements of the frame that are in messages that ended
        self.rows_in_messages = 0
        self.message_lengths: list[int] = []
        # Namespace declarations: name, and the indices of the IRI as written
        self.namespaces: list[tuple[str, int, int]] = []
        self.namespace_prefix_index = 0
        self.namespace_name_index = 0
        # The most IRIs a triple term had, so far in the stream: a statement can
        # need that many entries in the lookups for its object
        self.max_triple_term_iris = 1
        self.new_frame()

    def new_frame(self) -> None:
        columns = 3 if self.graphs is None else 4
        self.encoder.new_frame(columns + self.max_triple_term_iris - 1)
        self.namespace_prefix_index = 0
        self.namespace_name_index = 0

    def has_room(self) -> bool:
        """Return whether one more statement fits in the frame."""
        return self.row_count < MAX_ROWS and self.encoder.has_room()

    def has_content(self) -> bool:
        return bool(self.row_count or self.namespaces or self.message_lengths)

    def add(self, column: Column, term: object) -> None:
        """Add a subject, predicate or object to its column."""
        if column.run_length and self.encoder.same_term(column.run_term, term):
            column.run_length += 1
            return
        column.end_run()
        encoded = self.encoded
        self.encoder.encode_spo(term, encoded)
        column.add_value(encoded)
        column.run_term = term
        column.run_length = 1
        if encoded.triple_term is not None:
            self._triple_term_added()

    def add_changed(self, column: Column, term: object, *, changed: bool) -> None:
        """Add a term of the cursor API, which tells if it changed."""
        if not changed and column.run_length:
            column.run_length += 1
        else:
            self.add(column, term)

    def _triple_term_added(self) -> None:
        self.encoded.triple_term = None
        iris = self.encoder.triple_iris
        if iris > self.max_triple_term_iris:
            # From now on, each statement may need this many IRIs for its object
            self.encoder.reserve_iris(iris - self.max_triple_term_iris)
            self.max_triple_term_iris = iris

    def add_graph(self, term: object) -> None:
        """Add a graph name, an unbound cell for the default graph."""
        column = self.graphs
        assert column is not None
        if column.run_length and self.encoder.same_term(column.run_term, term):
            column.run_length += 1
            return
        encoded = self.encoded
        self.encoder.encode_graph(term, encoded)
        if encoded.kind == TERM_DEFAULT_GRAPH:
            if not column.run_unbound:
                column.end_run()
                column.run_unbound = True
        else:
            column.end_run()
            column.add_value(encoded)
        column.run_term = term
        column.run_length += 1

    def add_graph_changed(self, term: object, *, changed: bool) -> None:
        """Add a graph name of the cursor API, which tells if it changed."""
        column = self.graphs
        assert column is not None
        if not changed and column.run_length:
            column.run_length += 1
        else:
            self.add_graph(term)

    def add_triple(
        self, terms: Iterable[object], repeated_terms: list[object | None]
    ) -> None:
        """Add a triple, and keep its terms in `repeated_terms`, as the row layout."""
        s, p, o = terms
        self.add(self.subjects, s)
        self.add(self.predicates, p)
        self.add(self.objects, o)
        repeated_terms[0] = s
        repeated_terms[1] = p
        repeated_terms[2] = o
        self.row_count += 1

    def add_quad(
        self, terms: Iterable[object], repeated_terms: list[object | None]
    ) -> None:
        """Add a quad, and keep its terms in `repeated_terms`, as the row layout."""
        s, p, o, g = terms
        self.add(self.subjects, s)
        self.add(self.predicates, p)
        self.add(self.objects, o)
        self.add_graph(g)
        repeated_terms[0] = s
        repeated_terms[1] = p
        repeated_terms[2] = o
        repeated_terms[3] = g
        self.row_count += 1

    def add_namespace(self, name: str, iri: str) -> None:
        """Add a namespace declaration, which comes before the frame's statements."""
        prefix_index, name_index = self.encoder.iri_indices(iri)
        # The IRIs of the declarations are a sequence of their own
        written_prefix = (
            0 if prefix_index == self.namespace_prefix_index else prefix_index
        )
        written_name = 0 if name_index == self.namespace_name_index + 1 else name_index
        self.namespace_prefix_index = prefix_index
        self.namespace_name_index = name_index
        self.namespaces.append((name, written_prefix, written_name))

    def end_message(self) -> None:
        """End the current RDF Message, which may have started in an earlier frame."""
        self.message_lengths.append(self.row_count - self.rows_in_messages)
        self.rows_in_messages = self.row_count

    def write(self, batch: jelly.RdfColumnBatch) -> None:
        """Write the frame's statements into its batch, and start the next frame."""
        encoder = self.encoder
        batch.row_count = self.row_count
        encoder.names.packed.write(batch.names)
        encoder.prefixes.packed.write(batch.prefixes)
        encoder.datatypes.packed.write(batch.datatypes)
        for name, prefix_index, name_index in self.namespaces:
            declaration = batch.namespaces.add()
            declaration.name = name
            declaration.value.prefix_id = prefix_index
            declaration.value.name_id = name_index
        if self.row_count:
            self.subjects.write(batch.subjects)
            self.predicates.write(batch.predicates)
            self.objects.write(batch.objects)
            graphs = self.graphs
            if graphs is not None:
                if graphs.value_count:
                    graphs.write(batch.graphs)
                else:
                    # All statements are in the default graph: no column at all
                    graphs.clear()
        if self.message_lengths:
            batch.message_lengths.extend(self.message_lengths)
        self.row_count = 0
        self.rows_in_messages = 0
        self.message_lengths = []
        self.namespaces = []
        self.new_frame()
