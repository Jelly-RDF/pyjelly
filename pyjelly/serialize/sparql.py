"""
Writing of Jelly-SPARQL: SPARQL query results in frames, one column per variable.

The columns are those of Jelly-RDF 1.2 (see pyjelly.serialize.columns), in which
any cell may be unbound.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from contextlib import suppress
from typing import IO, Final

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.options import SPARQL_VERSION_1_0, SparqlOptions
from pyjelly.serialize.columns import MAX_ROWS, Column
from pyjelly.serialize.encode import EncodedTerm, TermEncoder
from pyjelly.serialize.ioutils import write_delimited, write_single

#: The metadata key of the links of a result set (head.link in SPARQL JSON results)
LINK_KEY: Final = "link"

# A column of results can have terms of any type
_ANY: Final = 0b1111


def links_value(links: Sequence[str]) -> bytes:
    r"""
    Return the value of the link metadata key: the IRIs, separated by LF.

    >>> links_value(["http://example.org/a", "http://example.org/b"])
    b'http://example.org/a\nhttp://example.org/b'
    """
    for link in links:
        if "\n" in link:
            msg = f"a link cannot contain the LF character, as no IRI can: {link!r}"
            raise JellyConformanceError(msg)
    return "\n".join(links).encode()


def variable_names(variables: Iterable[str]) -> list[str]:
    """
    Return the names of variables as written: without a leading "?" or "$".

    >>> variable_names(["?s", "$p", "o"])
    ['s', 'p', 'o']
    """
    names = []
    for variable in variables:
        name = str(variable)
        if name[:1] in ("?", "$"):
            name = name[1:]
        if not name:
            msg = "variable names cannot be empty"
            raise JellyConformanceError(msg)
        names.append(name)
    return names


def _failed() -> JellyConformanceError:
    msg = (
        "a row failed to encode, so the stream can only be ended, with an error: "
        "end(error)"
    )
    return JellyConformanceError(msg)


class VariableColumn(Column):
    """The cells of one variable in the frame being built."""

    def __init__(self) -> None:
        super().__init__(_ANY, "variable")
        # The IRI sequence of the column's triple terms: the indices of the last IRI,
        # while another column's triple terms are encoded (see TermEncoder)
        self.triple_prefix_index = -1
        self.triple_name_index = 0
        # The most IRIs a triple term of the column had, so far in the result set
        self.max_triple_iris = 1


class SparqlStream:
    """
    Writes SPARQL query results as the frames of a Jelly-SPARQL stream.

    A result set is a solution sequence (the result of a SELECT query) or a boolean
    result (ASK). Start a solution sequence with start(), add its rows with row(),
    and end it with end(). row() returns a frame when one is full, end() returns the
    last frame of the result set. A boolean result is one frame, made by ask().

    A stream has one result set, or with SparqlOptions(punctuated=True), any number
    of them.

    Args:
        encoder (TermEncoder): encoder of the terms, with the lookup sizes of the
            options.
        options (SparqlOptions | None): options of the stream.

    """

    def __init__(
        self, encoder: TermEncoder, options: SparqlOptions | None = None
    ) -> None:
        if options is None:
            options = SparqlOptions()
        if encoder.lookup_preset != options.lookup_preset:
            msg = "the encoder must have the lookup sizes of the options"
            raise ValueError(msg)
        self.encoder = encoder
        self.options = options
        encoder.column_layout = True
        encoder.rdf_version = options.rdf_version
        self.encoded = EncodedTerm()
        #: The variables of the result set being written, None between result sets
        self.variables: list[str] | None = None
        self.columns: list[VariableColumn] = []
        self.row_count = 0
        self.rows_per_frame = 1
        self.links: list[str] = []
        # Whether the stream options and the header are yet to be written
        self.options_pending = True
        self.header_pending = False
        # Whether nothing more can be written: the result set of a FLAT stream ended,
        # or a row failed to encode
        self.ended = False
        self.failed = False
        # The column whose IRI sequence of triple terms the encoder has, if any
        self.triple_owner: VariableColumn | None = None

    def start(self, variables: Iterable[str], links: Sequence[str] = ()) -> None:
        """
        Start a solution sequence.

        Args:
            variables (Iterable[str]): names of the variables, in projection order. A
                leading "?" or "$" is left out.
            links (Sequence[str]): IRIs of links describing the result set, like
                head.link of SPARQL JSON results.

        """
        self._check_can_start()
        names = variable_names(variables)
        self.variables = names
        self.columns = [VariableColumn() for _ in names]
        self.rows_per_frame = self.options.rows_per_frame(len(names))
        self.header_pending = True
        self.links = list(links)
        self.new_frame()

    def _check_can_start(self) -> None:
        if self.failed:
            raise _failed()
        if self.ended:
            msg = (
                "the stream has ended. A FLAT stream has one result set, use "
                "SparqlOptions(punctuated=True) to write more"
            )
            raise JellyConformanceError(msg)
        if self.variables is not None:
            msg = "the result set has not ended yet, end it with end()"
            raise JellyConformanceError(msg)

    def new_frame(self) -> None:
        columns = self.columns
        iris = len(columns)
        for column in columns:
            # A cell of a triple term can need that many IRIs
            iris += column.max_triple_iris - 1
            column.triple_prefix_index = -1
            column.triple_name_index = 0
        self.encoder.new_frame(iris, len(columns))
        self.triple_owner = None
        self.row_count = 0

    def row(self, values: Sequence[object | None]) -> jelly.SparqlResultsFrame | None:
        """
        Add a row (solution) to the result set.

        Args:
            values (Sequence[object | None]): the values of the variables, None for
                an unbound variable.

        Returns:
            jelly.SparqlResultsFrame | None: a frame, if one is full.

        """
        columns = self.columns
        if self.variables is None or self.failed:
            if self.failed:
                raise _failed()
            msg = "start a result set with start() before adding rows"
            raise JellyConformanceError(msg)
        count = len(values)
        if count != len(columns):
            msg = f"expected {len(columns)} values in the row, got {count}"
            raise JellyConformanceError(msg)
        frame = None
        if self.row_count and (
            self.row_count >= MAX_ROWS or not self.encoder.has_room()
        ):
            frame = self.full_frame()
        try:
            for i in range(count):
                self.add(columns[i], values[i])
        except BaseException:
            # The frame is half-written: it can't be completed
            self.failed = True
            raise
        self.row_count += 1
        if self.row_count >= self.rows_per_frame and self.options.delimited:
            return self.end_frame()
        return frame

    def rows(
        self, rows: Iterable[Sequence[object | None]]
    ) -> Iterator[jelly.SparqlResultsFrame]:
        """
        Add rows to the result set, see row().

        Yields:
            jelly.SparqlResultsFrame: full frames.

        """
        for values in rows:
            frame = self.row(values)
            if frame is not None:
                yield frame

    def full_frame(self) -> jelly.SparqlResultsFrame:
        """End the frame, which has no room for another row."""
        if not self.options.delimited:
            msg = (
                "the results do not fit in one frame: the lookups can't hold all of "
                "their terms. A non-delimited stream can only have one frame. Write "
                "a delimited stream, or use larger lookups."
            )
            raise JellyConformanceError(msg)
        return self.end_frame()

    def add(self, column: VariableColumn, term: object | None) -> None:
        """Add a value to its column, None for an unbound one."""
        if term is None:
            if not column.run_unbound:
                column.end_run()
                column.run_unbound = True
            column.run_length += 1
            return
        if (
            column.run_length
            and not column.run_unbound
            and self.encoder.same_term(column.run_term, term)
        ):
            column.run_length += 1
            return
        column.end_run()
        if self.triple_owner is not None and self.triple_owner is not column:
            self._switch_triple_iris(column)
        encoded = self.encoded
        self.encoder.encode_spo(term, encoded)
        column.add_value(encoded)
        column.run_term = term
        column.run_length = 1
        if encoded.triple_term is not None:
            self._triple_term_added(column)

    def _switch_triple_iris(self, column: VariableColumn) -> None:
        """Give the encoder the IRI sequence of a column's triple terms."""
        owner = self.triple_owner
        assert owner is not None
        encoder = self.encoder
        owner.triple_prefix_index = encoder.triple_prefix_index
        owner.triple_name_index = encoder.triple_name_index
        encoder.triple_prefix_index = column.triple_prefix_index
        encoder.triple_name_index = column.triple_name_index
        self.triple_owner = column

    def _triple_term_added(self, column: VariableColumn) -> None:
        self.encoded.triple_term = None
        if self.triple_owner is None:
            self.triple_owner = column
        iris = self.encoder.triple_iris
        if iris > column.max_triple_iris:
            # From now on, each row may need this many IRIs for the column
            self.encoder.reserve_iris(iris - column.max_triple_iris)
            column.max_triple_iris = iris

    def end_frame(self) -> jelly.SparqlResultsFrame:
        """End the current frame of the result set, and return it."""
        if self.variables is None or self.failed:
            if self.failed:
                raise _failed()
            msg = "there is no result set to end a frame of"
            raise JellyConformanceError(msg)
        frame = jelly.SparqlResultsFrame()
        self.write_frame(frame)
        return frame

    def write_frame(self, frame: jelly.SparqlResultsFrame) -> None:
        """Write the frame's options, header, lookup entries and rows."""
        self.write_start(frame)
        encoder = self.encoder
        row_count = self.row_count
        frame.row_count = row_count
        encoder.names.packed.write(frame.names)
        encoder.prefixes.packed.write(frame.prefixes)
        encoder.datatypes.packed.write(frame.datatypes)
        # A frame with no rows needs no columns
        if row_count:
            for column in self.columns:
                column.write(frame.columns.add())
        self.new_frame()

    def write_start(self, frame: jelly.SparqlResultsFrame) -> None:
        """Write the stream options and the header, if the frame needs them."""
        if self.options_pending:
            frame.options.CopyFrom(self.stream_options())
            self.options_pending = False
        if self.header_pending:
            assert self.variables is not None
            frame.variables.extend(self.variables)
            self.header_pending = False
            if self.links:
                frame.metadata[LINK_KEY] = links_value(self.links)

    def end(self, error: str = "") -> jelly.SparqlResultsFrame:
        """
        End the result set, and return its last frame.

        Args:
            error (str): why the result set is incomplete, if it is. Readers then
                know that some results are missing.

        Returns:
            jelly.SparqlResultsFrame: the last frame, with the trailer.

        """
        frame = jelly.SparqlResultsFrame()
        if self.failed:
            if not error or self.variables is None:
                raise _failed()
            # The rows of the frame are dropped, with the lookup entries they added,
            # so nothing can follow
            self.write_start(frame)
            self.ended = True
        elif self.variables is None:
            msg = "there is no result set to end, start one with start()"
            raise JellyConformanceError(msg)
        else:
            self.write_frame(frame)
            if not self.options.punctuated:
                self.ended = True
        frame.trailer.SetInParent()
        frame.trailer.error = error
        self.variables = None
        return frame

    def ask(self, value: bool, links: Sequence[str] = ()) -> jelly.SparqlResultsFrame:  # noqa: FBT001
        """
        Write a boolean result (of an ASK query), and return its frame.

        Args:
            value (bool): the result.
            links (Sequence[str]): IRIs of links describing the result.

        Returns:
            jelly.SparqlResultsFrame: the only frame of the result set.

        """
        self._check_can_start()
        frame = jelly.SparqlResultsFrame()
        self.write_start(frame)
        frame.ask_result.value = value
        frame.trailer.SetInParent()
        if links:
            frame.metadata[LINK_KEY] = links_value(links)
        if not self.options.punctuated:
            self.ended = True
        return frame

    def stream_options(self) -> jelly.SparqlResultsOptions:
        options = self.options
        lookup_preset = options.lookup_preset
        return jelly.SparqlResultsOptions(
            stream_name=options.stream_name,
            stream_type=(
                jelly.SPARQL_STREAM_TYPE_PUNCTUATED
                if options.punctuated
                else jelly.SPARQL_STREAM_TYPE_FLAT
            ),
            rdf_version=options.rdf_version,
            max_name_table_size=lookup_preset.max_names,
            max_prefix_table_size=lookup_preset.max_prefixes,
            max_datatype_table_size=lookup_preset.max_datatypes,
            version=SPARQL_VERSION_1_0,
        )


class FrameWriter:
    """Writes the frames of a stream to a file: delimited, or as its only frame."""

    def __init__(self, out: IO[bytes], *, delimited: bool) -> None:
        self.out = out
        self.delimited = delimited
        self.written = False

    def write(self, frame: jelly.SparqlResultsFrame) -> None:
        if self.delimited:
            write_delimited(frame, self.out)
            return
        if self.written:
            msg = (
                "the results do not fit in one frame. A non-delimited stream can "
                "only have one frame. Write a delimited stream, or use larger lookups."
            )
            raise JellyConformanceError(msg)
        write_single(frame, self.out)
        self.written = True


def write_solutions(
    stream: SparqlStream,
    variables: Iterable[str],
    rows: Iterable[Sequence[object | None]],
    writer: FrameWriter,
    links: Sequence[str] = (),
) -> None:
    """
    Write a solution sequence (the result of a SELECT query) as a result set.

    If getting the rows fails midway (e.g., the query fails), the result set ends
    with the error, so that readers know it is incomplete, and the error is
    raised.
    """
    stream.start(variables, links)
    try:
        for values in rows:
            frame = stream.row(values)
            if frame is not None:
                writer.write(frame)
    except Exception as error:
        # If the frame that says so can still be written
        with suppress(Exception):
            writer.write(stream.end(str(error) or type(error).__name__))
        raise
    writer.write(stream.end())
