"""
Decoding of the column layout of Jelly-RDF 1.2.

The statements of a frame are stored column by column: each run of equal terms in
consecutive statements is stored once, and the layout tokens of a column tell how
many cells each term takes, and where the cells are unbound. The terms are decoded
once per run, and the runs are only expanded into statements at the end, so the
memory needed does not depend on the row count that the frame declares.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Final

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.options import (
    DIR_LANG_STRING_DATATYPE_IRI,
    LANG_STRING_DATATYPE_IRI,
)

if TYPE_CHECKING:
    from pyjelly.parse.decode import Decoder
    from pyjelly.parse.lookup import LookupDecoder


class MessageEnd:
    """The end of an RDF Message, among the rows that Decoder.iter_rows() yields."""

    def __repr__(self) -> str:
        return "MESSAGE_END"


#: Yielded by Decoder.iter_rows() at the end of each RDF Message
MESSAGE_END: Final = MessageEnd()

#: The most statements a frame can have
MAX_ROWS: Final = (1 << 27) - 1

# Layout tokens, see RdfColumn.layouts
_SKIP_SHIFT: Final = 5
_UNBOUND: Final = 1 << 4
_LENGTH_MASK: Final = 15

# Types of run values, as stored in RdfColumn.kinds
_KIND_IRI: Final = 0
_KIND_LITERAL: Final = 1
_KIND_BNODE: Final = 2
_KIND_TRIPLE: Final = 3
_KIND_NAMES: Final = ("IRIs", "literals", "blank nodes", "triple terms")

_DIRECTIONS: Final[dict[int, str]] = {
    jelly.RDF_BASE_DIRECTION_LTR: "ltr",
    jelly.RDF_BASE_DIRECTION_RTL: "rtl",
}

# Methods called through the message classes rather than on messages: upb looks
# up any attribute of a message as a field name first
_FRAME_HAS_FIELD: Final = jelly.RdfStreamFrame.HasField
_BATCH_HAS_FIELD: Final = jelly.RdfColumnBatch.HasField
_ROW_WHICH_ONEOF: Final = jelly.RdfStreamRow.WhichOneof
_TRIPLE_TERM_WHICH_ONEOF: Final = jelly.RdfTripleTerm.WhichOneof
_TRIPLE_TERM_HAS_FIELD: Final = jelly.RdfTripleTerm.HasField
_LITERAL_WHICH_ONEOF: Final = jelly.RdfLiteral.WhichOneof
_DECLARATION_HAS_FIELD: Final = jelly.RdfNamespaceDeclaration.HasField

# Which types of terms each column allows, as bits 1 << _KIND_*
_RESOURCES: Final = (1 << _KIND_IRI) | (1 << _KIND_BNODE)
_IRIS: Final = 1 << _KIND_IRI
_ANY: Final = 0b1111


def _apply_entries(entries: Any, lookup: LookupDecoder) -> None:
    """Apply the packed entries of one lookup."""
    for entry in entries:
        index = entry.id
        for value in entry.values:
            lookup.assign_entry(index, value)
            index = 0


def _unbound(position: str) -> JellyConformanceError:
    msg = f"the {position} column must have a term in every row"
    return JellyConformanceError(msg)


class _Runs:
    """
    The runs of a column: the term of each run, and how many cells it takes.

    First the runs are added, then the cells are read with next(), in order.
    """

    def __init__(self, rows: int, position: str) -> None:
        self.rows = rows
        self.position = position
        self.terms: list[Any] = []
        self.counts: list[int] = []
        self.cells = 0
        # Reading: the next run, and the cells left of the current one
        self.index = 0
        self.left = 0
        self.term: Any = None

    def add(self, term: Any, count: int) -> None:
        """Add a run. Unbound cells have the term None."""
        self.cells += count
        if self.cells > self.rows:
            msg = f"the {self.position} column has more cells than the frame has rows"
            raise JellyConformanceError(msg)
        self.terms.append(term)
        self.counts.append(count)

    def add_cells(self, terms: list[Any]) -> None:
        """Add terms of one cell each."""
        for term in terms:
            self.add(term, 1)

    def set_unbound(self, term: Any) -> None:
        """Use a term for the unbound cells."""
        self.terms = [term if t is None else t for t in self.terms]

    def next(self) -> Any:
        if not self.left:
            self.term = self.terms[self.index]
            self.left = self.counts[self.index]
            self.index += 1
        self.left -= 1
        return self.term


class IriSequence:
    """
    IRIs written as RdfIri messages, in the order they are read.

    A prefix index of 0 means the same prefix as the IRI before (none in the first
    IRI), and a name index of 0 means the name after the one of the IRI before.
    """

    def __init__(self, decoder: Decoder) -> None:
        self.decoder = decoder
        self.prefix_index = 0
        self.name_index = 0

    def term(self, iri: jelly.RdfIri) -> Any:
        prefix_index = iri.prefix_id
        if prefix_index:
            self.prefix_index = prefix_index
        else:
            prefix_index = self.prefix_index
        name_index = iri.name_id or self.name_index + 1
        self.name_index = name_index
        return self.decoder.iri_term(prefix_index, name_index)


class ColumnDecoder:
    """Decodes the frames of a Jelly-RDF 1.2 stream for a Decoder."""

    def __init__(self, decoder: Decoder) -> None:
        self.decoder = decoder
        options = decoder.options
        self.quads = options.stream_types.physical_type == (
            jelly.PHYSICAL_STREAM_TYPE_QUADS
        )
        self.messages = options.params.stream_type == jelly.RDF_STREAM_TYPE_MESSAGES
        # Whether the stream options were read: those the decoder was made with
        self.options_read = False
        # Statements of the current RDF Message so far, in this and earlier frames
        self.rows_in_message = 0

    def iter_frame(self, frame: jelly.RdfStreamFrame) -> Iterator[Any]:
        """Yield the namespace declarations and statements of a frame."""
        rows = frame.rows
        if rows:
            if len(rows) > 1 or _ROW_WHICH_ONEOF(rows[0], "row") != "options":
                msg = (
                    "a frame of a Jelly-RDF 1.2 stream can only have the stream "
                    "options in its rows, in one row"
                )
                raise JellyConformanceError(msg)
            if self.options_read:
                if self.restate_options(rows[0].options):
                    yield MESSAGE_END
            else:
                self.options_read = True
        if _FRAME_HAS_FIELD(frame, "columns"):
            if not self.options_read:
                msg = "the stream has statements before the stream options"
                raise JellyConformanceError(msg)
            yield from self.iter_batch(frame.columns)

    def restate_options(self, options: jelly.RdfStreamOptions) -> bool:
        """
        Read restated stream options, which empty the lookups.

        Returns:
            bool: whether an RDF Message ended, because it had statements.

        """
        current = self.decoder.options
        if (
            options.version != current.params.version
            or options.physical_type != current.stream_types.physical_type
            or options.stream_type != current.params.stream_type
        ):
            msg = (
                "the protocol version, physical type and stream type must be the same "
                "in all stream options of a stream"
            )
            raise JellyConformanceError(msg)
        self.decoder.restate_options(options)
        if self.messages and self.rows_in_message:
            self.rows_in_message = 0
            return True
        return False

    def end_of_stream(self) -> Iterator[Any]:
        """Yield the end of the last RDF Message, if it has statements."""
        if self.messages and self.rows_in_message:
            self.rows_in_message = 0
            yield MESSAGE_END

    def iter_batch(self, batch: jelly.RdfColumnBatch) -> Iterator[Any]:
        rows = batch.row_count
        if rows > MAX_ROWS:
            msg = f"a frame can have at most {MAX_ROWS} statements, not {rows}"
            raise JellyConformanceError(msg)
        lengths = self.message_lengths(batch, rows)
        # All lookup entries come first, so that the columns can refer to them
        self.ingest_entries(batch)
        if batch.namespaces:
            yield from self.namespaces(batch)
        has_graphs = _BATCH_HAS_FIELD(batch, "graphs")
        if has_graphs and not self.quads:
            msg = "a stream of triples cannot have a graphs column"
            raise JellyConformanceError(msg)
        subjects = self.runs(batch.subjects, rows, _RESOURCES, "subject")
        predicates = self.runs(batch.predicates, rows, _IRIS, "predicate")
        objects = self.runs(batch.objects, rows, _ANY, "object")
        graphs = None
        if self.quads:
            if has_graphs:
                graphs = self.runs(batch.graphs, rows, _RESOURCES, "graph")
            else:
                graphs = _Runs(rows, "graph")
                graphs.add(None, rows)
            graphs.set_unbound(self.decoder.adapter.default_graph())

        # The statements, with an RDF Message ending at each boundary
        start = 0
        for length in lengths:
            yield from self.statements(length, subjects, predicates, objects, graphs)
            yield MESSAGE_END
            start += length
        yield from self.statements(rows - start, subjects, predicates, objects, graphs)
        if lengths:
            self.rows_in_message = rows - start
        else:
            self.rows_in_message += rows

    def message_lengths(self, batch: jelly.RdfColumnBatch, rows: int) -> list[int]:
        """Return the lengths of the RDF Messages that end in a batch."""
        lengths = list(batch.message_lengths)
        if lengths:
            if not self.messages:
                msg = "message boundaries are only valid in streams of RDF Messages"
                raise JellyConformanceError(msg)
            if sum(lengths) > rows:
                msg = (
                    f"the message lengths add up to {sum(lengths)}, more than the "
                    f"{rows} statements of the frame"
                )
                raise JellyConformanceError(msg)
        return lengths

    def ingest_entries(self, batch: jelly.RdfColumnBatch) -> None:
        decoder = self.decoder
        _apply_entries(batch.names, decoder.names)
        _apply_entries(batch.prefixes, decoder.prefixes)
        for entry in batch.datatypes:
            for value in entry.values:
                if value in (LANG_STRING_DATATYPE_IRI, DIR_LANG_STRING_DATATYPE_IRI):
                    msg = f"the datatype lookup cannot have {value}"
                    raise JellyConformanceError(msg)
        _apply_entries(batch.datatypes, decoder.datatypes)

    def namespaces(self, batch: jelly.RdfColumnBatch) -> Iterator[Any]:
        adapter = self.decoder.adapter
        # The IRIs of the declarations are a sequence of their own
        iris = IriSequence(self.decoder)
        for declaration in batch.namespaces:
            if not _DECLARATION_HAS_FIELD(declaration, "value"):
                msg = f"the namespace declaration {declaration.name!r} has no IRI"
                raise JellyConformanceError(msg)
            yield adapter.namespace_declaration(
                declaration.name, iris.term(declaration.value)
            )

    def statements(
        self,
        count: int,
        subjects: _Runs,
        predicates: _Runs,
        objects: _Runs,
        graphs: _Runs | None,
    ) -> Iterator[Any]:
        """Yield the next statements, from the next cells of the columns."""
        adapter = self.decoder.adapter
        if graphs is None:
            for _ in range(count):
                yield adapter.triple(
                    (subjects.next(), predicates.next(), objects.next())
                )
        else:
            for _ in range(count):
                yield adapter.quad(
                    (subjects.next(), predicates.next(), objects.next(), graphs.next())
                )

    def runs(
        self, column: jelly.RdfColumn, rows: int, allowed: int, position: str
    ) -> _Runs:
        """Decode a column into its runs, whose cells add up to the row count."""
        values = self.run_values(column, allowed, position)
        unbound_allowed = position == "graph"
        layouts = column.layouts
        runs = _Runs(rows, position)
        index = 0
        k = 0
        while k < len(layouts):
            token = layouts[k]
            k += 1
            length = token & _LENGTH_MASK
            if length == _LENGTH_MASK:
                if k == len(layouts):
                    msg = (
                        f"corrupt layout of the {position} column: a length is cut off"
                    )
                    raise JellyConformanceError(msg)
                length += layouts[k]
                k += 1
            skip = token >> _SKIP_SHIFT
            if index + skip > len(values):
                msg = (
                    f"corrupt layout of the {position} column: it skips past its values"
                )
                raise JellyConformanceError(msg)
            runs.add_cells(values[index : index + skip])
            index += skip
            if token & _UNBOUND:
                if not unbound_allowed:
                    raise _unbound(position)
                runs.add(None, length + 1)
            elif index < len(values):
                runs.add(values[index], length + 2)
                index += 1
            else:
                msg = (
                    f"corrupt layout of the {position} column: a repeated value is "
                    "past its values"
                )
                raise JellyConformanceError(msg)
        runs.add_cells(values[index:])
        if runs.cells < rows:
            if not unbound_allowed:
                raise _unbound(position)
            runs.add(None, rows - runs.cells)
        return runs

    def run_values(
        self, column: jelly.RdfColumn, allowed: int, position: str
    ) -> list[Any]:
        """Decode the run values of a column, in order."""
        counts = (
            len(column.name_ids),
            len(column.lex_values),
            len(column.bnodes),
            len(column.triple_terms),
        )
        for kind in range(4):
            if counts[kind] and not allowed & (1 << kind):
                msg = f"the {position} column cannot have {_KIND_NAMES[kind]}"
                raise JellyConformanceError(msg)
        if column.kinds:
            return self.mixed_values(column, counts, position)
        if sum(1 for count in counts if count) > 1:
            msg = f"the {position} column has terms of more than one type, but no kinds"
            raise JellyConformanceError(msg)
        if counts[_KIND_LITERAL]:
            return self.literals(column)
        if counts[_KIND_BNODE]:
            return self.bnodes(column)
        if counts[_KIND_TRIPLE]:
            return self.triple_terms(column)
        return self.iris(column)

    def mixed_values(
        self, column: jelly.RdfColumn, counts: tuple[int, ...], position: str
    ) -> list[Any]:
        """Decode the run values of a column whose kinds say which type each is."""
        kinds = column.kinds
        value_count = sum(counts)
        if len(kinds) != (value_count + 3) >> 2:
            msg = (
                f"the {position} column has {len(kinds)} bytes of kinds "
                f"for {value_count} values"
            )
            raise JellyConformanceError(msg)
        used = value_count & 3
        if used and kinds[-1] >> (used << 1):
            msg = f"the unused bits of the kinds of the {position} column are not 0"
            raise JellyConformanceError(msg)
        kind_list = [(kinds[i >> 2] >> ((i & 3) << 1)) & 3 for i in range(value_count)]
        for kind in range(4):
            if kind_list.count(kind) != counts[kind]:
                msg = (
                    f"the kinds of the {position} column do not match its "
                    f"number of {_KIND_NAMES[kind]}"
                )
                raise JellyConformanceError(msg)
        by_kind = (
            iter(self.iris(column)),
            iter(self.literals(column)),
            iter(self.bnodes(column)),
            iter(self.triple_terms(column)),
        )
        return [next(by_kind[kind]) for kind in kind_list]

    def iris(self, column: jelly.RdfColumn) -> list[Any]:
        name_ids = column.name_ids
        prefix_ids = column.prefix_ids
        count = len(name_ids)
        prefix_count = len(prefix_ids)
        if prefix_count > 1 and prefix_count != count:
            msg = f"a column has {prefix_count} prefix ids for {count} IRIs"
            raise JellyConformanceError(msg)
        decoder = self.decoder
        prefix_index = prefix_ids[0] if prefix_count == 1 else 0
        name_index = 0
        terms = []
        for i in range(count):
            if prefix_count > 1 and prefix_ids[i]:
                prefix_index = prefix_ids[i]
            name_index = name_ids[i] or name_index + 1
            terms.append(decoder.iri_term(prefix_index, name_index))
        return terms

    def bnodes(self, column: jelly.RdfColumn) -> list[Any]:
        adapter = self.decoder.adapter
        return [adapter.bnode(label) for label in column.bnodes]

    def literals(self, column: jelly.RdfColumn) -> list[Any]:
        lex_values = column.lex_values
        literal_kinds = column.literal_kinds
        count = len(lex_values)
        kind_count = len(literal_kinds)
        if kind_count > 1 and kind_count != count:
            msg = f"a column has {kind_count} literal kinds for {count} literals"
            raise JellyConformanceError(msg)
        langtags = column.langtags
        directions = column.langtag_directions
        if directions and len(directions) != len(langtags):
            msg = (
                f"a column has {len(directions)} base directions for "
                f"{len(langtags)} language tags"
            )
            raise JellyConformanceError(msg)
        if not count:
            return []
        decoder = self.decoder
        terms = []
        if kind_count <= 1:
            language, datatype, direction = self.literal_kind(
                literal_kinds[0] if kind_count else 0, langtags, directions
            )
            return [
                decoder.literal_term(lex, language, datatype, direction)
                for lex in lex_values
            ]
        for lex, kind in zip(lex_values, literal_kinds, strict=True):
            language, datatype, direction = self.literal_kind(
                kind, langtags, directions
            )
            terms.append(decoder.literal_term(lex, language, datatype, direction))
        return terms

    def literal_kind(
        self, kind: int, langtags: Any, directions: Any
    ) -> tuple[str | None, str | None, str | None]:
        """Return the language tag, datatype and base direction of a literal kind."""
        if not kind:
            return None, None, None
        if kind & 1:
            return None, self.decoder.datatypes.at((kind + 1) >> 1), None
        index = (kind >> 1) - 1
        if index >= len(langtags):
            msg = f"a literal kind refers to language tag {index}, which is not there"
            raise JellyConformanceError(msg)
        direction = None
        if directions and directions[index]:
            direction = self.direction(directions[index])
        return langtags[index] or None, None, direction

    def direction(self, value: int) -> str:
        """Return a base direction read from the stream."""
        direction = _DIRECTIONS.get(value)
        if direction is None:
            msg = f"unknown base direction: {value}"
            raise JellyConformanceError(msg)
        if self.decoder.options.params.rdf_version == jelly.RDF_VERSION_1_1:
            msg = "the stream declares RDF 1.1, but has a literal with a base direction"
            raise JellyConformanceError(msg)
        return direction

    def triple_terms(self, column: jelly.RdfColumn) -> list[Any]:
        if not column.triple_terms:
            return []
        rdf_version = self.decoder.options.params.rdf_version
        if rdf_version in (jelly.RDF_VERSION_1_1, jelly.RDF_VERSION_1_2_BASIC):
            msg = (
                "the stream declares an RDF version without triple terms, but has some"
            )
            raise JellyConformanceError(msg)
        # The IRIs of the column's triple terms are a sequence of their own
        iris = IriSequence(self.decoder)
        return [self.triple_term(term, iris) for term in column.triple_terms]

    def triple_term(self, triple: jelly.RdfTripleTerm, iris: IriSequence) -> Any:
        adapter = self.decoder.adapter
        kind = _TRIPLE_TERM_WHICH_ONEOF(triple, "subject")
        if kind == "s_iri":
            s = iris.term(triple.s_iri)
        elif kind == "s_bnode":
            s = adapter.bnode(triple.s_bnode)
        else:
            msg = "a triple term has no subject"
            raise JellyConformanceError(msg)
        if not _TRIPLE_TERM_HAS_FIELD(triple, "p_iri"):
            msg = "a triple term has no predicate"
            raise JellyConformanceError(msg)
        p = iris.term(triple.p_iri)
        kind = _TRIPLE_TERM_WHICH_ONEOF(triple, "object")
        if kind == "o_iri":
            o = iris.term(triple.o_iri)
        elif kind == "o_bnode":
            o = adapter.bnode(triple.o_bnode)
        elif kind == "o_literal":
            o = self.triple_term_literal(triple.o_literal)
        elif kind == "o_triple_term":
            o = self.triple_term(triple.o_triple_term, iris)
        else:
            msg = "a triple term has no object"
            raise JellyConformanceError(msg)
        return adapter.quoted_triple((s, p, o))

    def triple_term_literal(self, literal: jelly.RdfLiteral) -> Any:
        decoder = self.decoder
        direction = literal.direction
        kind = _LITERAL_WHICH_ONEOF(literal, "literalKind")
        if kind == "langtag":
            return decoder.literal_term(
                literal.lex,
                literal.langtag or None,
                None,
                self.direction(direction) if direction else None,
            )
        if direction:
            msg = "a literal has a base direction, but no language tag"
            raise JellyConformanceError(msg)
        if kind == "datatype":
            index = literal.datatype
            if not index:
                msg = "0 is not a valid datatype index"
                raise JellyConformanceError(msg)
            return decoder.literal_term(
                literal.lex, None, decoder.datatypes.at(index), None
            )
        return decoder.literal_term(literal.lex, None, None, None)
