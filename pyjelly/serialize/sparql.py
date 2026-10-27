"""Jelly-SPARQL writer: SPARQL query results in frames."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import suppress
from typing import IO, Any, Final

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError, JellyIncompleteResultsError
from pyjelly.options import SPARQL_VERSION_1_0, SparqlOptions
from pyjelly.serialize.columns import MAX_ROWS, Column
from pyjelly.serialize.encode import EncodedTerm, TermEncoder
from pyjelly.serialize.ioutils import write_delimited, write_single

#: Metadata key for the links of a result set (head.link in SPARQL JSON results)
LINK_KEY: Final = "link"

# A result column accepts every term type
_ANY: Final = 0b1111


def links_value(links: Sequence[str]) -> bytes:
    r"""
    Join link IRIs with LF, for the "link" metadata key.

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
    Strip a leading "?" or "$" from each variable name.

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
    """Cells of one variable in the frame being built."""

    def __init__(self) -> None:
        super().__init__(_ANY, "variable")
        # Prefix and name indices of the last IRI in this column's triple terms.
        # The encoder holds them while it encodes this column (see TermEncoder).
        self.triple_prefix_index = -1
        self.triple_name_index = 0
        # Largest number of IRIs in one triple term of this column so far
        self.max_triple_iris = 1


class SparqlStream:
    """
    Writer of SPARQL query results as Jelly-SPARQL frames.

    A result set is either a solution sequence (SELECT) or a boolean result (ASK).
    For a solution sequence, call start(), then row() for each row, then end().
    row() returns a frame when one is full, and end() returns the last frame. ask()
    returns the single frame of a boolean result.

    A stream holds one result set, or any number of them with
    SparqlOptions(punctuated=True).

    Args:
        encoder (TermEncoder): term encoder, with the same lookup sizes as the
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
        #: Variables of the current result set, None between result sets
        self.variables: list[str] | None = None
        self.columns: list[VariableColumn] = []
        # Reused buffer for the values of a binding, see binding()
        self.values: list[object | None] = []
        self.row_count = 0
        self.rows_per_frame = 1
        # Set when the single frame of a non-delimited stream is full
        self.full = False
        self.links: list[str] = []
        # Whether the stream options and the header still have to be written
        self.options_pending = True
        self.header_pending = False
        # `ended`: the single result set of a FLAT stream is done. `failed`: a row
        # failed to encode. Either way, nothing more can be written.
        self.ended = False
        self.failed = False
        # Column whose triple-term IRI state the encoder currently holds
        self.triple_owner: VariableColumn | None = None

    def start(self, variables: Iterable[str], links: Sequence[str] = ()) -> None:
        """
        Start a solution sequence.

        Args:
            variables (Iterable[str]): variable names, in projection order. A
                leading "?" or "$" is removed.
            links (Sequence[str]): link IRIs for the result set, as in head.link of
                SPARQL JSON results.

        """
        self._check_can_start()
        names = variable_names(variables)
        self.variables = names
        self.columns = [VariableColumn() for _ in names]
        self.values = [None] * len(names)
        self.rows_per_frame = (
            min(self.options.rows_per_frame(len(names)), MAX_ROWS)
            if self.options.delimited
            else MAX_ROWS
        )
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
            # A triple term can take more than one IRI per cell
            iris += column.max_triple_iris - 1
            column.triple_prefix_index = -1
            column.triple_name_index = 0
        self.encoder.new_frame(iris, len(columns))
        self.triple_owner = None
        self.row_count = 0
        self.full = False

    def row(self, values: Sequence[object | None]) -> jelly.SparqlResultsFrame | None:
        """
        Add a row (solution) to the result set.

        Args:
            values (Sequence[object | None]): one value per variable, None if the
                variable is unbound.

        Returns:
            jelly.SparqlResultsFrame | None: the frame, if this row filled it.

        """
        columns = self.columns
        count = len(values)
        self._check_row(count)
        try:
            # mypyc indexes a typed tuple or list directly. Any other sequence goes
            # through the generic item lookup with a boxed int, which is slow.
            if type(values) is tuple:
                row_tuple: tuple[object | None, ...] = values
                for i in range(count):
                    self.add(columns[i], row_tuple[i])
            elif type(values) is list:
                row_list: list[object | None] = values
                for i in range(count):
                    self.add(columns[i], row_list[i])
            else:
                for i in range(count):
                    self.add(columns[i], values[i])
        except BaseException:
            # The frame is half-written and can't be finished
            self.failed = True
            raise
        self.row_count += 1
        # End the frame as soon as it's full, rather than at the start of the next
        # row, so that a failing next row can't lose it
        if self.row_count >= self.rows_per_frame or not self.encoder.has_room():
            if self.options.delimited:
                return self.end_frame()
            self.full = True
        return None

    def _check_row(self, count: int) -> None:
        """Check that a row of `count` values can be added."""
        if self.variables is None or self.failed:
            if self.failed:
                raise _failed()
            msg = "start a result set with start() before adding rows"
            raise JellyConformanceError(msg)
        if count != len(self.columns):
            msg = f"expected {len(self.columns)} values in the row, got {count}"
            raise JellyConformanceError(msg)
        if self.full:
            msg = (
                "the results do not fit in one frame: the lookups can't hold all of "
                "their terms. A non-delimited stream can only have one frame. Write "
                "a delimited stream, or use larger lookups."
            )
            raise JellyConformanceError(msg)

    def binding(
        self, binding: Mapping[Any, object | None], variables: Sequence[object]
    ) -> jelly.SparqlResultsFrame | None:
        """
        Add a row (solution) given as a mapping from variables to values.

        Args:
            binding (Mapping[Any, object | None]): values of the bound variables,
                keyed by variable or by variable name.
            variables (Sequence[object]): variables of the result set, in order.

        Returns:
            jelly.SparqlResultsFrame | None: the frame, if this row filled it.

        """
        values = self.values
        if len(variables) != len(values):
            msg = f"expected {len(values)} variables, got {len(variables)}"
            raise JellyConformanceError(msg)
        # A typed list is cheaper to index, see row()
        keys: list[object] = variables if type(variables) is list else list(variables)
        bound = 0
        if type(binding) is dict:
            # Same loop, but with direct dict calls instead of method lookups
            mapping: dict[Any, object | None] = binding
            for i in range(len(values)):
                value = mapping.get(keys[i])
                values[i] = value
                if value is not None:
                    bound += 1
        else:
            for i in range(len(values)):
                value = binding.get(keys[i])
                values[i] = value
                if value is not None:
                    bound += 1
        if bound != len(binding):
            # Some keys are not the variable objects themselves (rdflib's
            # Variable("x") does not equal "x"), so look the values up by name
            by_name = {str(key): value for key, value in binding.items()}
            for i in range(len(values)):
                values[i] = by_name.get(str(variables[i]))
        return self.row(values)

    def rows(
        self, rows: Iterable[Sequence[object | None]]
    ) -> Iterator[jelly.SparqlResultsFrame]:
        """
        Add many rows, see row().

        Yields:
            jelly.SparqlResultsFrame: each frame as it fills up.

        """
        for values in rows:
            frame = self.row(values)
            if frame is not None:
                yield frame

    def add(self, column: VariableColumn, term: object | None) -> None:
        """Add a value to a column. None is an unbound cell."""
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
        """Swap the encoder's triple-term IRI state over to another column."""
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
            # Every following row may need this many IRIs for the column
            self.encoder.reserve_iris(iris - column.max_triple_iris)
            column.max_triple_iris = iris

    def end_frame(self) -> jelly.SparqlResultsFrame:
        """End the current frame and return it."""
        if self.variables is None or self.failed:
            if self.failed:
                raise _failed()
            msg = "there is no result set in progress"
            raise JellyConformanceError(msg)
        frame = jelly.SparqlResultsFrame()
        self.write_frame(frame)
        return frame

    def write_frame(self, frame: jelly.SparqlResultsFrame) -> None:
        """Fill in the frame: options, header, lookup entries and rows."""
        self.write_start(frame)
        encoder = self.encoder
        row_count = self.row_count
        frame.row_count = row_count
        encoder.names.packed.write(frame.names)
        encoder.prefixes.packed.write(frame.prefixes)
        encoder.datatypes.packed.write(frame.datatypes)
        # Columns are left out of a frame with no rows
        if row_count:
            for column in self.columns:
                column.write(frame.columns.add())
        self.new_frame()

    def write_start(self, frame: jelly.SparqlResultsFrame) -> None:
        """Add the stream options and the header, if they are still due."""
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
        End the result set and return its last frame, which has the trailer.

        Args:
            error (str): reason the result set is incomplete, if it is. It tells
                readers that some results are missing.

        Returns:
            jelly.SparqlResultsFrame: the last frame.

        """
        frame = jelly.SparqlResultsFrame()
        if self.failed:
            if not error or self.variables is None:
                raise _failed()
            # Drop the frame's rows and their lookup entries. The lookups are then
            # out of sync with the reader, so the stream must end here.
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

    def ask(
        self,
        value: bool,  # noqa: FBT001
        links: Sequence[str] = (),
        error: str = "",
    ) -> jelly.SparqlResultsFrame:
        """
        Return the frame of a boolean result (of an ASK query).

        Args:
            value (bool): the result.
            links (Sequence[str]): link IRIs for the result.
            error (str): reason the result is incomplete, if it is.

        Returns:
            jelly.SparqlResultsFrame: the result's only frame.

        """
        self._check_can_start()
        frame = jelly.SparqlResultsFrame()
        self.write_start(frame)
        frame.ask_result.value = value
        frame.trailer.SetInParent()
        frame.trailer.error = error
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


def error_message(error: BaseException) -> str:
    """
    Format an exception as the error message of a trailer.

    >>> error_message(KeyError("x"))
    "KeyError: 'x'"
    >>> error_message(JellyIncompleteResultsError("...", "timeout"))
    'timeout'
    """
    if isinstance(error, JellyIncompleteResultsError):
        # Passing on the error of a stream that was itself incomplete
        return error.error
    message = str(error)
    name = type(error).__name__
    return f"{name}: {message}" if message else name


class FrameWriter:
    """
    Writer of frames to a file, either delimited or as a single frame.

    Each frame is flushed, so a reader on the other end of a connection gets the
    rows right away.
    """

    def __init__(self, out: IO[bytes], *, delimited: bool) -> None:
        self.out = out
        self.delimited = delimited
        self.written = False

    def write(self, frame: jelly.SparqlResultsFrame) -> None:
        if self.delimited:
            write_delimited(frame, self.out)
            self.out.flush()
            return
        if self.written:
            msg = (
                "the results do not fit in one frame. A non-delimited stream can "
                "only have one frame. Write a delimited stream, or use larger lookups."
            )
            raise JellyConformanceError(msg)
        write_single(frame, self.out)
        self.out.flush()
        self.written = True


def write_solutions(  # noqa: PLR0913 keyword-only options
    stream: SparqlStream,
    variables: Iterable[str],
    rows: Iterable[Sequence[object | None]],
    writer: FrameWriter,
    *,
    links: Sequence[str] = (),
    error: str = "",
) -> None:
    """
    Write a solution sequence (the result of a SELECT query) as one result set.

    If iterating the rows raises (e.g., the query fails midway), the result set is
    ended with that error so readers know it is incomplete, and the exception is
    re-raised. Pass `error` if the rows are already known to be incomplete.
    """
    stream.start(variables, links)
    try:
        for values in rows:
            frame = stream.row(values)
            if frame is not None:
                writer.write(frame)
    except Exception as failure:
        _end_with(stream, writer, failure)
        raise
    writer.write(stream.end(error))


def write_bindings(  # noqa: PLR0913 keyword-only options
    stream: SparqlStream,
    variables: Sequence[object],
    bindings: Iterable[Mapping[Any, object | None]],
    writer: FrameWriter,
    *,
    links: Sequence[str] = (),
    error: str = "",
) -> None:
    """
    Like write_solutions(), with rows given as bindings.

    Args:
        stream (SparqlStream): the stream.
        variables (Sequence[object]): the variables, as str objects (e.g., rdflib's
            Variable). Their names are written.
        bindings (Iterable[Mapping[Any, object | None]]): the rows, see
            SparqlStream.binding().
        writer (FrameWriter): writer of the frames.
        links (Sequence[str]): link IRIs for the results.
        error (str): reason the rows are incomplete, if known.

    """
    stream.start([str(variable) for variable in variables], links)
    try:
        for binding in bindings:
            frame = stream.binding(binding, variables)
            if frame is not None:
                writer.write(frame)
    except Exception as failure:
        _end_with(stream, writer, failure)
        raise
    writer.write(stream.end(error))


def _end_with(stream: SparqlStream, writer: FrameWriter, failure: Exception) -> None:
    """End the result set with an error, if writing the frame still works."""
    with suppress(Exception):
        writer.write(stream.end(error_message(failure)))
