import io
import os
from collections.abc import Generator, Iterator
from itertools import chain
from typing import IO, Final, TypeVar, cast

from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.message import DecodeError, Message
from google.protobuf.proto import parse, parse_length_prefixed

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.options import MIN_COLUMN_NAME_LOOKUP_SIZE, VERSION_1_2
from pyjelly.parse.decode import ParserOptions, options_from_frame, parser_options

# Protobuf wire types. Fields of any other type use a varint.
_WIRE_VARINT: Final = 0
_WIRE_FIXED64: Final = 1
_WIRE_LENGTH_DELIMITED: Final = 2
_WIRE_FIXED32: Final = 5

_WIRE_TYPE_MASK: Final = 0b111
_VARINT_CONTINUATION: Final = 0b1000_0000
_VARINT_PAYLOAD: Final = 0b0111_1111
# A protobuf varint is at most 5 bytes long when it encodes a length.
_MAX_VARINT_SHIFT: Final = 35

# Fields numbered above this need more than one byte for their tag.
_MAX_SINGLE_BYTE_FIELD: Final = 15

_WIRE_TYPES: Final = {
    FieldDescriptor.TYPE_MESSAGE: _WIRE_LENGTH_DELIMITED,
    FieldDescriptor.TYPE_STRING: _WIRE_LENGTH_DELIMITED,
    FieldDescriptor.TYPE_BYTES: _WIRE_LENGTH_DELIMITED,
    FieldDescriptor.TYPE_GROUP: _WIRE_LENGTH_DELIMITED,
    FieldDescriptor.TYPE_DOUBLE: _WIRE_FIXED64,
    FieldDescriptor.TYPE_FIXED64: _WIRE_FIXED64,
    FieldDescriptor.TYPE_SFIXED64: _WIRE_FIXED64,
    FieldDescriptor.TYPE_FLOAT: _WIRE_FIXED32,
    FieldDescriptor.TYPE_FIXED32: _WIRE_FIXED32,
    FieldDescriptor.TYPE_SFIXED32: _WIRE_FIXED32,
}


def single_byte_tags(descriptor: Descriptor) -> frozenset[int]:
    """
    Return the single-byte tags that a message's fields can have.

    Only fields numbered 1 to 15 have a one-byte tag. A repeated scalar field
    may be written packed or not, so both of its tags are returned.

    Args:
        descriptor (Descriptor): descriptor of the message

    Returns:
        frozenset[int]: tag bytes of the message's fields

    """
    tags = set()
    for field in descriptor.fields:
        if field.number > _MAX_SINGLE_BYTE_FIELD:
            continue
        tags.add((field.number << 3) | _WIRE_TYPES.get(field.type, _WIRE_VARINT))
        if field.is_repeated:
            tags.add((field.number << 3) | _WIRE_LENGTH_DELIMITED)
    return frozenset(tags)


FRAME_TAGS: Final = single_byte_tags(
    cast("Descriptor", jelly.RdfStreamFrame.DESCRIPTOR)
)
"""Tag bytes a non-delimited frame can start with (currently 0x0A, 0x12, 0x7A)."""

SPARQL_FRAME_TAGS: Final = single_byte_tags(
    cast("Descriptor", jelly.SparqlResultsFrame.DESCRIPTOR)
)
"""Tag bytes that a non-delimited Jelly-SPARQL frame can start with."""


def hint_bytes_needed(first_byte: int, tags: frozenset[int] = FRAME_TAGS) -> int:
    """
    Return how many bytes `delimited_jelly_hint` needs to decide.

    Args:
        first_byte (int): first byte of the stream
        tags (frozenset[int]): tag bytes a frame can start with

    Returns:
        int: number of bytes to read from the start of the stream, at most 128

    >>> hint_bytes_needed(0x11)
    1

    >>> hint_bytes_needed(0x0A)
    11

    """
    if first_byte in tags:
        return first_byte + 1
    return 1


def _is_frame_fields(data: bytes, tags: frozenset[int]) -> bool:
    """
    Check whether `data` is exactly a sequence of fields of a stream frame.

    Args:
        data (bytes): candidate frame body
        tags (frozenset[int]): tag bytes of the frame's fields

    Returns:
        bool: True if the bytes make up whole fields and nothing is left over

    """
    pos = 0
    end = len(data)
    while pos < end:
        tag = data[pos]
        pos += 1
        if tag not in tags:
            return False
        wire_type = tag & _WIRE_TYPE_MASK
        if wire_type == _WIRE_VARINT:
            while pos < end and data[pos] & _VARINT_CONTINUATION:
                pos += 1
            pos += 1
        elif wire_type == _WIRE_FIXED64:
            pos += 8
        elif wire_type == _WIRE_FIXED32:
            pos += 4
        else:
            # Length-delimited. The length must fit in what we have, so it is short.
            length = 0
            shift = 0
            ended = False
            while pos < end and shift < _MAX_VARINT_SHIFT:
                byte = data[pos]
                pos += 1
                length |= (byte & _VARINT_PAYLOAD) << shift
                shift += 7
                if not byte & _VARINT_CONTINUATION:
                    ended = True
                    break
            if not ended or length > end - pos:
                return False
            pos += length
    return pos == end


def delimited_jelly_hint(header: bytes, tags: frozenset[int] = FRAME_TAGS) -> bool:
    """
    Detect whether a Jelly stream is delimited, from its first bytes.

    A non-delimited stream is a single frame, so it starts with the tag of one
    of the frame's fields -- any of them, because a writer may put the fields
    in any order. A delimited stream starts with the size of its first frame.
    So, if the first byte is not a frame tag, the stream is delimited.
    Otherwise it may still be the size of a small first frame, of at most 127
    bytes: the stream is then delimited if the bytes after it are exactly that
    many bytes of frame fields.

    Pass as much of the start of the stream as you have, up to 128 bytes --
    `hint_bytes_needed` tells you how much is actually needed once you have the
    first byte. Given fewer bytes than the check needs, the stream is reported
    as non-delimited.

    Args:
        header (bytes): first bytes of the stream
        tags (frozenset[int]): tag bytes a frame can start with, FRAME_TAGS for
            Jelly-RDF and SPARQL_FRAME_TAGS for Jelly-SPARQL

    Returns:
        bool: True if the stream is delimited

    An empty stream is an empty non-delimited frame:

    >>> delimited_jelly_hint(b"")
    False

    0x11 is not a frame tag, so it is the size of the first frame:

    >>> delimited_jelly_hint(bytes([0x11, 0x0A, 0x0F, 0x0A, 0x0D]))
    True

    A 10-byte frame holding one name entry, written non-delimited. It starts
    with the tag of the rows field, and the 9 bytes after it are not a whole
    frame of 10 bytes:

    >>> frame = bytes([0x0A, 0x08, 0x4A, 0x06, 0x12, 0x04, 0x6E, 0x61, 0x6D, 0x65])
    >>> delimited_jelly_hint(frame)
    False

    The same frame written delimited. Its size, 10, is also the tag of the rows
    field, but the 10 bytes after it are exactly one frame's worth of fields:

    >>> delimited_jelly_hint(bytes([0x0A]) + frame)
    True

    A stream of empty delimited frames is delimited too -- 0 is never a tag:

    >>> delimited_jelly_hint(bytes([0x00, 0x00, 0x00]))
    True

    Two bytes of garbage are not enough to tell, so they count as a frame:

    >>> delimited_jelly_hint(bytes([0x12, 0x34]))
    False

    """
    if not header:
        return False
    first = header[0]
    if first not in tags:
        # Not a frame tag, so it is the size of the first frame. This includes
        # 0, an empty frame: a stream of only empty frames is delimited too.
        return True
    if len(header) < first + 1:
        return False
    return _is_frame_fields(header[1 : first + 1], tags)


def _read_exactly(inp: IO[bytes], size: int) -> bytes:
    """
    Read `size` bytes, or fewer if the stream ends first.

    Args:
        inp (IO[bytes]): stream to read from
        size (int): number of bytes wanted

    Returns:
        bytes: what was read

    """
    chunks = []
    remaining = size
    while remaining > 0:
        chunk = inp.read(remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_hint_header(inp: IO[bytes], tags: frozenset[int] = FRAME_TAGS) -> bytes:
    """
    Read just enough of the start of the stream to detect delimiting.

    Reads one byte, and at most 128. Pass the result to `delimited_jelly_hint`.

    Args:
        inp (IO[bytes]): stream to read from
        tags (frozenset[int]): tag bytes a frame can start with

    Returns:
        bytes: bytes read from the start of the stream

    """
    header = inp.read(1)
    if not header:
        return header
    remaining = hint_bytes_needed(header[0], tags) - 1
    if remaining:
        header += _read_exactly(inp, remaining)
    return header


class _StreamWithHeader:
    """A stream that gives back `header` first, then the rest of `rest`."""

    def __init__(self, header: bytes, rest: IO[bytes]) -> None:
        self._header = header
        self._rest = rest

    def read(self, size: int = -1) -> bytes:
        if not self._header:
            return self._rest.read(size)
        if size < 0:
            data = self._header + self._rest.read()
            self._header = b""
            return data
        taken = self._header[:size]
        self._header = self._header[size:]
        if len(taken) < size:
            taken += self._rest.read(size - len(taken))
        return taken


def detect_delimited(
    inp: IO[bytes], tags: frozenset[int] = FRAME_TAGS
) -> tuple[IO[bytes], bool]:
    """
    Detect from the first bytes whether a stream is delimited.

    Args:
        inp (IO[bytes]): the stream
        tags (frozenset[int]): tag bytes a frame can start with

    Returns:
        tuple[IO[bytes], bool]: a stream positioned at the start again, and
            whether it is delimited

    """
    if inp.seekable():
        header = read_hint_header(inp, tags)
        inp.seek(-len(header), os.SEEK_CUR)
    else:
        # Input may not be seekable (e.g. a network stream). Then the bytes we read
        # are put back in front of it. Only an unbuffered stream gets a buffer: a
        # second buffer around a buffered stream (e.g. from socket.makefile())
        # waits for the inner one to fill up, which blocks streaming.
        # See also: https://github.com/Jelly-RDF/pyjelly/issues/298
        buffered = inp
        if not isinstance(inp, io.BufferedIOBase):
            buffered = io.BufferedReader(inp)  # type: ignore[arg-type, type-var, unused-ignore]
        header = read_hint_header(buffered, tags)
        inp = _StreamWithHeader(header, buffered)  # type: ignore[assignment]
    return inp, delimited_jelly_hint(header, tags)


# What protobuf raises for bytes it can't decode. The pure-Python implementation
# raises UnicodeDecodeError for a string that isn't UTF-8, the others DecodeError.
DECODE_ERRORS: Final = (DecodeError, UnicodeDecodeError)

MessageT = TypeVar("MessageT", bound=Message)


def parse_delimited(message_class: type[MessageT], inp: IO[bytes]) -> MessageT | None:
    """
    Read the next length-prefixed message, or None at the end of the input.

    Raises:
        DecodeError: if the bytes are not a message of the class, or the input
            ends in the middle of one. Or UnicodeDecodeError, see DECODE_ERRORS.
        ValueError: if the input is closed.

    """
    try:
        # protobuf annotates `input_bytes` as io.BytesIO, but any IO[bytes] works.
        return parse_length_prefixed(message_class, inp)  # type: ignore[arg-type]
    except DECODE_ERRORS:
        raise
    except (ValueError, OverflowError) as error:
        if getattr(inp, "closed", False):
            raise
        # The length prefix is cut short, or too large to read
        msg = f"invalid length prefix ({error})"
        raise DecodeError(msg) from None


def _not_jelly(error: Exception) -> JellyConformanceError:
    msg = f"the stream is not valid Jelly-RDF ({error}). Is it cut short?"
    return JellyConformanceError(msg)


def frame_iterator(inp: IO[bytes]) -> Generator[jelly.RdfStreamFrame]:
    while True:
        try:
            frame = parse_delimited(jelly.RdfStreamFrame, inp)
        except DECODE_ERRORS as error:
            raise _not_jelly(error) from None
        if frame is None:
            return
        yield frame


def _empty_stream_options(*, delimited: bool) -> ParserOptions:
    """Return the options of a stream that has none: an empty graph."""
    options = jelly.RdfStreamOptions(
        physical_type=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
        max_name_table_size=MIN_COLUMN_NAME_LOOKUP_SIZE,
        version=VERSION_1_2,
    )
    return parser_options(options, delimited=delimited)


def get_options_and_frames(
    inp: IO[bytes],
) -> tuple[ParserOptions, Iterator[jelly.RdfStreamFrame]]:
    """
    Return stream options and frames from the buffered binary stream.

    The options are in the first frame that has rows. A stream with no such frame
    (e.g., an empty file) is an empty graph, with the options of an empty
    Jelly-RDF 1.2 stream of triples.

    Args:
        inp (IO[bytes]): jelly buffered binary stream

    Raises:
        JellyConformanceError: if the parser can't read the stream

    Returns:
        tuple[ParserOptions, Iterator[jelly.RdfStreamFrame]]: ParserOptions holds:
            stream types, lookup presets and other stream options

    """
    inp, is_delimited = detect_delimited(inp)

    if is_delimited:
        first_frame = None
        skipped_frames = []
        frames = frame_iterator(inp)
        for frame in frames:
            if not frame.rows:
                skipped_frames.append(frame)
            else:
                first_frame = frame
                break
        if first_frame is None:
            return _empty_stream_options(delimited=True), iter(skipped_frames)

        options = options_from_frame(first_frame, delimited=True)
        return options, chain(skipped_frames, (first_frame,), frames)

    try:
        frame = parse(jelly.RdfStreamFrame, inp.read())
    except DECODE_ERRORS as error:
        raise _not_jelly(error) from None

    if not frame.rows:
        return _empty_stream_options(delimited=False), iter((frame,))

    options = options_from_frame(frame, delimited=False)
    return options, iter((frame,))
