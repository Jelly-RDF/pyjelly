"""
Jelly-SPARQL reader: SPARQL query results in frames, with one column per variable.

SparqlDecoder decodes frames one at a time. read_result_sets() groups the rows of
the frames into result sets.
"""

from __future__ import annotations

import io
import warnings
from collections.abc import Callable, Iterator
from typing import IO, Any, Final

from google.protobuf.message import DecodeError
from google.protobuf.proto import parse, parse_length_prefixed

from pyjelly import jelly
from pyjelly.errors import (
    JellyConformanceError,
    JellyIncompleteResultsError,
    JellyTruncatedResultsWarning,
)
from pyjelly.options import (
    MAX_DATATYPE_LOOKUP_SIZE,
    MAX_NAME_LOOKUP_SIZE,
    MAX_PREFIX_LOOKUP_SIZE,
    MAX_SPARQL_FRAME_ROWS,
    MAX_SPARQL_FRAME_VALUES,
    MIN_COLUMN_NAME_LOOKUP_SIZE,
    SPARQL_MAX_VERSION,
    VERSION_1_2,
    LookupPreset,
    StreamParameters,
    StreamTypes,
)
from pyjelly.parse.columns import MAX_ROWS, ColumnDecoder
from pyjelly.parse.decode import Adapter, Decoder, ParserOptions
from pyjelly.parse.ioutils import SPARQL_FRAME_TAGS, detect_delimited

#: Metadata key for the links of a result set (head.link in SPARQL JSON results)
LINK_KEY: Final = "link"

AdapterFactory = Callable[[ParserOptions], Adapter]
#: Builds the rows of a frame from the variables, the cells (one list per variable,
#: None for unbound) and the row count
RowFactory = Callable[[list[str], list[list[Any]], int], list[Any]]


def tuple_rows(variables: list[str], cells: list[list[Any]], rows: int) -> list[Any]:
    """Build the rows of a frame as tuples, with one value per variable."""
    if not variables:
        return [()] * rows
    return list(zip(*cells, strict=True))


# Methods are called through the message classes, not the messages: upb tries
# every attribute name of a message as a field name first
_FRAME_HAS_FIELD: Final = jelly.SparqlResultsFrame.HasField

_STREAM_TYPES: Final = (
    jelly.SPARQL_STREAM_TYPE_FLAT,
    jelly.SPARQL_STREAM_TYPE_PUNCTUATED,
)
_RDF_VERSIONS: Final = frozenset(jelly.RdfVersion.values())


def check_sparql_options(options: jelly.SparqlResultsOptions) -> None:
    """
    Check that this parser supports a Jelly-SPARQL stream with these options.

    Raises:
        JellyConformanceError: if it can't.

    """
    version = options.version
    if not 1 <= version <= SPARQL_MAX_VERSION:
        msg = (
            f"unsupported Jelly-SPARQL version {version}, "
            f"expected 1 to {SPARQL_MAX_VERSION}"
        )
        raise JellyConformanceError(msg)
    if options.stream_type not in _STREAM_TYPES:
        msg = f"unknown stream type: {options.stream_type}"
        raise JellyConformanceError(msg)
    if options.rdf_version not in _RDF_VERSIONS:
        msg = f"unknown RDF version: {options.rdf_version}"
        raise JellyConformanceError(msg)
    _check_size(
        "name",
        options.max_name_table_size,
        MIN_COLUMN_NAME_LOOKUP_SIZE,
        MAX_NAME_LOOKUP_SIZE,
    )
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


def sparql_parser_options(options: jelly.SparqlResultsOptions) -> ParserOptions:
    """
    Build the adapter options for a Jelly-SPARQL stream.

    Terms are decoded like in a Jelly-RDF 1.2 stream with the same lookup sizes and
    RDF version. The stream types are left unspecified.
    """
    return ParserOptions(
        stream_types=StreamTypes(),
        lookup_preset=LookupPreset(
            max_names=options.max_name_table_size,
            max_prefixes=options.max_prefix_table_size,
            max_datatypes=options.max_datatype_table_size,
        ),
        params=StreamParameters(
            version=VERSION_1_2,
            stream_name=options.stream_name,
            rdf_version=options.rdf_version,
        ),
    )


def decode_links(value: bytes) -> list[str] | None:
    r"""
    Split the "link" metadata value into IRIs. Return None if it isn't UTF-8.

    >>> decode_links(b"http://example.org/a\nhttp://example.org/b")
    ['http://example.org/a', 'http://example.org/b']
    >>> decode_links(b"")
    []
    >>> decode_links(b"\xff") is None
    True
    """
    try:
        text = value.decode()
    except UnicodeDecodeError:
        return None
    return text.split("\n") if text else []


class DecodedFrame:
    """Contents of one decoded Jelly-SPARQL frame."""

    def __init__(self) -> None:
        #: Whether this is the first frame of a result set
        self.starts = False
        #: Variables of the result set, if this frame starts a solution sequence
        self.variables: list[str] = []
        #: Boolean result, if the frame has one
        self.boolean: bool | None = None
        #: Rows of the frame, built by the decoder's row factory
        self.rows: list[Any] = []
        #: Link IRIs for the result set, if the frame has them
        self.links: list[str] | None = None
        #: Trailer: None if the frame has none, "" if the result set is complete,
        #: otherwise the error message
        self.trailer: str | None = None


class SparqlDecoder:
    """
    Decoder of the frames of a Jelly-SPARQL stream, in stream order.

    Args:
        adapter_factory (AdapterFactory): builds the term adapter from the stream
            options.
        row_factory (RowFactory): builds the rows of a frame. Tuples by default.
        max_rows (int): maximum rows per frame.
        max_values (int): maximum values (rows times variables) per frame. A frame
            is decoded all at once, so this caps its memory use.

    """

    def __init__(
        self,
        adapter_factory: AdapterFactory,
        *,
        row_factory: RowFactory = tuple_rows,
        max_rows: int = MAX_SPARQL_FRAME_ROWS,
        max_values: int = MAX_SPARQL_FRAME_VALUES,
    ) -> None:
        self.adapter_factory = adapter_factory
        self.row_factory = row_factory
        self.max_rows = min(max_rows, MAX_ROWS)
        self.max_values = max_values
        #: Stream options, once read
        self.options: jelly.SparqlResultsOptions | None = None
        self.decoder: Decoder | None = None
        self.columns: ColumnDecoder | None = None
        #: Whether the stream is a sequence of result sets
        self.punctuated = False
        # Variables declared by the first header of the result set, and whether a
        # header is in effect. Repeated stream options cancel the header.
        self.variables: list[str] | None = None
        self.header_in_effect = False
        # Whether the next frame starts a result set, whether the current result
        # set is a boolean result, and whether a trailer came after the options
        self.result_set_start = True
        self.boolean_read = False
        self.trailer_read = False
        # Number of result sets started so far
        self.result_sets = 0

    def decode(self, frame: jelly.SparqlResultsFrame) -> DecodedFrame:
        """Decode a frame."""
        out = DecodedFrame()
        has_options = _FRAME_HAS_FIELD(frame, "options")
        if has_options:
            self.read_options(frame.options)
        elif self.options is None:
            msg = "the stream does not start with the stream options"
            raise JellyConformanceError(msg)
        elif self.trailer_read and not self.punctuated:
            msg = (
                "a frame after the trailer must have the stream options: the trailer "
                "ends the result set"
            )
            raise JellyConformanceError(msg)
        if self.result_set_start:
            self.start_result_set()
            out.starts = True
        if self.boolean_read:
            msg = "no frame of the result set can follow its boolean result"
            raise JellyConformanceError(msg)
        if _FRAME_HAS_FIELD(frame, "ask_result"):
            self.read_boolean(frame, out)
        else:
            self.read_solutions(frame, has_options=has_options, out=out)
        links = frame.metadata.get(LINK_KEY)
        if links is not None:
            out.links = decode_links(links)
        if _FRAME_HAS_FIELD(frame, "trailer"):
            out.trailer = frame.trailer.error
            self.trailer_read = True
            self.result_set_start = self.punctuated
        else:
            self.result_set_start = False
        return out

    def read_options(self, options: jelly.SparqlResultsOptions) -> None:
        """Read the stream options. Repeated options also empty the lookups."""
        check_sparql_options(options)
        punctuated = options.stream_type == jelly.SPARQL_STREAM_TYPE_PUNCTUATED
        parser_options = sparql_parser_options(options)
        current = self.options
        if current is None:
            decoder = Decoder(self.adapter_factory(parser_options))
            self.decoder = decoder
            self.columns = decoder.column_decoder
        else:
            if options.stream_type != current.stream_type:
                msg = "the stream type must be the same in all stream options"
                raise JellyConformanceError(msg)
            if punctuated and not self.result_set_start:
                msg = (
                    "in a PUNCTUATED stream, the stream options can only be repeated "
                    "in the first frame of a result set"
                )
                raise JellyConformanceError(msg)
            current_decoder = self.decoder
            assert current_decoder is not None
            current_decoder.adapter.options = parser_options
            current_decoder.new_lookups()
            self.header_in_effect = False
        self.options = options
        self.punctuated = punctuated
        if not punctuated:
            self.trailer_read = False

    def start_result_set(self) -> None:
        """Reset the per-result-set state, for the next result set."""
        self.variables = None
        self.header_in_effect = False
        self.boolean_read = False
        self.trailer_read = False
        if self.result_sets:
            # Each result set has its own blank node labels
            columns = self.columns
            assert columns is not None
            columns.bnode_scope = f"r{self.result_sets}-"
        self.result_sets += 1

    def read_boolean(self, frame: jelly.SparqlResultsFrame, out: DecodedFrame) -> None:
        if not out.starts:
            msg = "a boolean result can only be in the first frame of a result set"
            raise JellyConformanceError(msg)
        if frame.row_count or frame.variables or frame.columns:
            msg = "a frame with a boolean result cannot have variables, rows or columns"
            raise JellyConformanceError(msg)
        self.boolean_read = True
        out.boolean = frame.ask_result.value
        # The next result set of a PUNCTUATED stream may refer to these entries
        columns = self.columns
        assert columns is not None
        columns.apply_entries(frame.names, frame.prefixes, frame.datatypes)

    def read_solutions(
        self, frame: jelly.SparqlResultsFrame, *, has_options: bool, out: DecodedFrame
    ) -> None:
        variables = self.read_variables(frame, has_options=has_options)
        if out.starts:
            out.variables = variables
        rows = frame.row_count
        if rows > MAX_ROWS:
            msg = f"a frame can have at most {MAX_ROWS} rows, not {rows}"
            raise JellyConformanceError(msg)
        if rows > self.max_rows:
            msg = f"the frame has {rows} rows, more than the {self.max_rows} allowed"
            raise JellyConformanceError(msg)
        if rows * len(variables) > self.max_values:
            msg = (
                f"the frame has {rows} rows of {len(variables)} variables, more than "
                f"the {self.max_values} values allowed"
            )
            raise JellyConformanceError(msg)

        columns = self.columns
        assert columns is not None
        # Apply all lookup entries before decoding the columns that use them
        columns.apply_entries(frame.names, frame.prefixes, frame.datatypes)
        column_count = len(frame.columns)
        if not variables:
            if column_count:
                msg = "a result set with no variables cannot have columns"
                raise JellyConformanceError(msg)
            out.rows = self.row_factory(variables, [], rows)
        elif column_count == len(variables):
            cells = [
                columns.cells(column, rows, "?" + name)
                for column, name in zip(frame.columns, variables, strict=True)
            ]
            out.rows = self.row_factory(variables, cells, rows)
        elif column_count or rows:
            msg = (
                f"the frame has {column_count} columns, but the result set has "
                f"{len(variables)} variables"
            )
            raise JellyConformanceError(msg)

    def read_variables(
        self, frame: jelly.SparqlResultsFrame, *, has_options: bool
    ) -> list[str]:
        """Read the frame's header, if any, and return the current variables."""
        if frame.variables:
            if not has_options and not self.result_set_start:
                msg = (
                    "the variables can only be declared in the first frame of a "
                    "result set, or in a frame with the stream options"
                )
                raise JellyConformanceError(msg)
            self.read_header(list(frame.variables))
        elif not self.header_in_effect:
            # A missing header next to the options, or at the start of a result
            # set, means zero variables
            if self.variables:
                msg = (
                    "the frame repeats the stream options, so it must repeat the "
                    "variables too"
                )
                raise JellyConformanceError(msg)
            self.read_header([])
        variables = self.variables
        assert variables is not None
        return variables

    def read_header(self, names: list[str]) -> None:
        for name in names:
            if not name:
                msg = "variable names cannot be empty"
                raise JellyConformanceError(msg)
        if self.variables is None:
            self.variables = names
        elif names != self.variables:
            msg = (
                "the repeated variables must be the same as the first ones: "
                f"{names} instead of {self.variables}"
            )
            raise JellyConformanceError(msg)
        self.header_in_effect = True


def sparql_frames(inp: IO[bytes] | bytes) -> Iterator[jelly.SparqlResultsFrame]:
    """
    Iterate the frames of a Jelly-SPARQL stream, delimited or not.

    Raises:
        JellyConformanceError: if the stream is empty, or isn't Jelly-SPARQL.

    """
    if isinstance(inp, (bytes, bytearray, memoryview)):
        inp = io.BytesIO(inp)
    inp, delimited = detect_delimited(inp, SPARQL_FRAME_TAGS)
    if not delimited:
        data = inp.read()
        if not data:
            msg = "the stream is empty, but it must have at least one frame"
            raise JellyConformanceError(msg)
        try:
            return iter((parse(jelly.SparqlResultsFrame, data),))
        except DecodeError as error:
            raise _not_sparql(error) from None
    return _delimited_frames(inp)


def _not_sparql(error: Exception) -> JellyConformanceError:
    msg = (
        f"the stream is not valid Jelly-SPARQL ({error}). Is it cut short, or is it "
        "Jelly-RDF?"
    )
    return JellyConformanceError(msg)


def _delimited_frames(inp: IO[bytes]) -> Iterator[jelly.SparqlResultsFrame]:
    while True:
        try:
            # protobuf annotates `input_bytes` as io.BytesIO, but any IO[bytes] works.
            frame = parse_length_prefixed(jelly.SparqlResultsFrame, inp)  # type: ignore[arg-type]
        except DecodeError as error:
            raise _not_sparql(error) from None
        except ValueError as error:
            if getattr(inp, "closed", False):
                msg = (
                    "the input was closed before all results were read. Read the "
                    "rows before closing it."
                )
                raise ValueError(msg) from error
            raise
        if frame is None:
            return
        yield frame


class ResultSetReader:
    """Reads and decodes frames one at a time, for the result sets."""

    def __init__(
        self, frames: Iterator[jelly.SparqlResultsFrame], decoder: SparqlDecoder
    ) -> None:
        self.frames = frames
        self.decoder = decoder
        # A frame read ahead that belongs to the next result set
        self.pending: DecodedFrame | None = None

    def next(self) -> DecodedFrame | None:
        """Return the next frame, or None at the end of the stream."""
        frame = self.pending
        if frame is not None:
            self.pending = None
            return frame
        message = next(self.frames, None)
        if message is None:
            return None
        return self.decoder.decode(message)


def _incomplete(error: str, result: object) -> JellyIncompleteResultsError:
    msg = f"the results are incomplete: {error}"
    return JellyIncompleteResultsError(msg, error, result)


class SparqlResultSet:
    """
    One result set of a Jelly-SPARQL stream: a solution sequence or a boolean result.

    Rows are read from the stream lazily, during iteration. Each row is a tuple
    with one value per variable, None if the variable is unbound. A loop that
    stops early can be resumed later. Once all rows have been read, iterating
    again raises an error. If the next result set is read first, the unread rows
    are kept in memory.

    If the producer ended the result set with an error, iterating raises
    JellyIncompleteResultsError after the last row, and so does `boolean`. If the
    stream ends without a trailer, `truncated` is set and a
    JellyTruncatedResultsWarning is issued.
    """

    def __init__(self, reader: ResultSetReader, first: DecodedFrame) -> None:
        self.reader = reader
        self.first: DecodedFrame | None = first
        #: Variables in projection order. Empty for a boolean result.
        self.variables: list[str] = first.variables
        self._boolean = first.boolean
        #: Link IRIs for the result set (head.link in SPARQL JSON)
        self.links: list[str] = first.links or []
        #: Error message from the producer, if the result set is incomplete
        self.error = ""
        #: Whether the stream ended without a trailer, so rows may be missing.
        #: Only known after all rows have been read.
        self.truncated = False
        # Row lists of the coming frames, and the unread rows of the current one
        self._frames = self._read()
        self._rows: Iterator[Any] = iter(())
        # Whether an iteration has read all rows
        self._done = False
        if first.boolean is not None:
            # A boolean result is a single frame, so its trailer is already known
            if first.trailer:
                self.error = first.trailer
            elif first.trailer is None:
                self._truncated()

    @property
    def boolean(self) -> bool | None:
        """
        Boolean result of an ASK query, or None for a solution sequence.

        Raises:
            JellyIncompleteResultsError: if the producer reported an error.

        """
        if self.error and self._boolean is not None:
            raise _incomplete(self.error, self)
        return self._boolean

    @property
    def is_boolean(self) -> bool:
        """Whether this is a boolean result (of an ASK query)."""
        return self._boolean is not None

    def __iter__(self) -> Iterator[Any]:
        self._check_not_done()
        while True:
            # A list iterator keeps its position when a loop stops early, so the
            # next loop resumes from there
            yield from self._rows
            rows = next(self._frames, None)
            if rows is None:
                break
            self._rows = iter(rows)
        self._end()

    def collect(self, rows: list[Any]) -> None:
        """
        Append all remaining rows to a list. Faster than iterating.

        Raises:
            JellyIncompleteResultsError: if the result set is incomplete. The rows
                are appended first.

        """
        self._check_not_done()
        rows.extend(self._rows)
        for frame_rows in self._frames:
            rows.extend(frame_rows)
        self._end()

    def _check_not_done(self) -> None:
        if self._done:
            msg = (
                "the rows of the result set were already read: they can be "
                "iterated only once"
            )
            raise RuntimeError(msg)

    def _end(self) -> None:
        self._done = True
        if self.error:
            raise _incomplete(self.error, self)

    def _truncated(self) -> None:
        if not self.truncated:
            self.truncated = True
            warnings.warn(
                "the stream ended without saying that the results are complete, so "
                "some may be missing",
                JellyTruncatedResultsWarning,
                stacklevel=3,
            )

    def _read(self) -> Iterator[list[Any]]:
        """Yield each frame's rows until the result set ends, recording trailers."""
        reader = self.reader
        frame = self.first
        self.first = None
        ended = False
        while frame is not None:
            if frame.trailer is not None:
                ended = True
                if frame.trailer:
                    # Keep the first error, even if more rows follow in a
                    # concatenated stream
                    self.error = self.error or frame.trailer
            else:
                # In a FLAT stream, repeated options after a trailer continue the
                # result set (concatenated streams), so the trailer no longer counts
                ended = False
            if frame.links is not None and not self.links:
                self.links = frame.links
            if frame.rows:
                yield frame.rows
            if ended and reader.decoder.punctuated:
                return
            frame = self._next_frame(ended=ended)
            if frame is not None and frame.starts:
                # This frame starts the next result set of a PUNCTUATED stream
                reader.pending = frame
                return
        if not ended:
            self._truncated()

    def _next_frame(self, *, ended: bool) -> DecodedFrame | None:
        """Read the next frame, or None at the end of the stream."""
        try:
            return self.reader.next()
        except JellyConformanceError:
            if ended and self.error:
                # The trailer already said that the results are incomplete, so what
                # follows doesn't matter. E.g., Fuseki appends a text message to the
                # response of a query it cancels.
                return None
            raise

    def keep(self) -> None:
        """Read the remaining rows into memory, for iteration later."""
        if not self._done:
            kept = list(self._rows)
            for rows in self._frames:
                kept += rows
            self._rows = iter(kept)


def read_result_sets(
    inp: IO[bytes] | bytes,
    adapter_factory: AdapterFactory,
    *,
    row_factory: RowFactory = tuple_rows,
    max_rows: int = MAX_SPARQL_FRAME_ROWS,
    max_values: int = MAX_SPARQL_FRAME_VALUES,
) -> Iterator[SparqlResultSet]:
    """
    Read the result sets of a Jelly-SPARQL stream.

    A FLAT stream (the default) has one result set, a PUNCTUATED stream any
    number. Moving on to the next result set reads the rest of the current one
    and keeps its unread rows in memory.

    Args:
        inp (IO[bytes] | bytes): the stream, delimited or not.
        adapter_factory (AdapterFactory): builds the term adapter from the stream
            options.
        row_factory (RowFactory): builds the rows. Tuples by default.
        max_rows (int): maximum rows per frame.
        max_values (int): maximum values (rows times variables) per frame.

    Yields:
        SparqlResultSet: the result sets, in order.

    """
    decoder = SparqlDecoder(
        adapter_factory,
        row_factory=row_factory,
        max_rows=max_rows,
        max_values=max_values,
    )
    reader = ResultSetReader(sparql_frames(inp), decoder)
    while (first := reader.next()) is not None:
        result_set = SparqlResultSet(reader, first)
        yield result_set
        result_set.keep()


def read_result_set(
    inp: IO[bytes] | bytes,
    adapter_factory: AdapterFactory,
    *,
    row_factory: RowFactory = tuple_rows,
    max_rows: int = MAX_SPARQL_FRAME_ROWS,
    max_values: int = MAX_SPARQL_FRAME_VALUES,
) -> SparqlResultSet:
    """
    Read the single result set of a FLAT Jelly-SPARQL stream.

    The first frame is read now, the rows during iteration. For PUNCTUATED
    streams, use read_result_sets().

    Raises:
        JellyConformanceError: if the stream is PUNCTUATED.
        JellyIncompleteResultsError: for a boolean result that the producer
            could not complete.

    """
    result_sets = read_result_sets(
        inp,
        adapter_factory,
        row_factory=row_factory,
        max_rows=max_rows,
        max_values=max_values,
    )
    result_set = next(result_sets)
    if result_set.reader.decoder.punctuated:
        msg = (
            "the stream is a sequence of result sets (PUNCTUATED). Read it with "
            "parse_jelly_sparql_sequence() of pyjelly.integrations.rdflib.sparql "
            "or pyjelly.integrations.generic.sparql"
        )
        raise JellyConformanceError(msg)
    if result_set.is_boolean and result_set.error:
        raise _incomplete(result_set.error, result_set)
    return result_set
