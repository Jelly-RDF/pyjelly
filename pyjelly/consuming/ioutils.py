import os
from collections.abc import Generator, Iterator
from typing import IO

from google.protobuf.proto import parse, parse_length_prefixed

from pyjelly import jelly
from pyjelly.consuming import options_from_frame
from pyjelly.options import StreamOptions


def delimited_jelly_hint(header: bytes) -> bool:
    """
    Detect whether a Jelly file is delimited from its first 3 bytes.

    Truth table (notation: `0A` = `0x0A`, `NN` = `not 0x0A`, `??` = _don't care_):

    | Byte 1 | Byte 2 | Byte 3 | Result                                   |
    |--------|--------|--------|------------------------------------------|
    | `NN`   |  `??`  |  `??`  | Delimited                                |
    | `0A`   |  `NN`  |  `??`  | Non-delimited                            |
    | `0A`   |  `0A`  |  `NN`  | Delimited (size = 10)                    |
    | `0A`   |  `0A`  |  `0A`  | Non-delimited (stream options size = 10) |

    >>> delimited_jelly_hint(bytes([0x00, 0x00, 0x00]))
    True

    >>> delimited_jelly_hint(bytes([0x00, 0x00, 0x0A]))
    True

    >>> delimited_jelly_hint(bytes([0x00, 0x0A, 0x00]))
    True

    >>> delimited_jelly_hint(bytes([0x00, 0x0A, 0x0A]))
    True

    >>> delimited_jelly_hint(bytes([0x0A, 0x00, 0x00]))
    False

    >>> delimited_jelly_hint(bytes([0x0A, 0x00, 0x0A]))
    False

    >>> delimited_jelly_hint(bytes([0x0A, 0x0A, 0x00]))
    True

    >>> delimited_jelly_hint(bytes([0x0A, 0x0A, 0x0A]))
    False
    """
    magic = 0x0A
    return len(header) == 3 and (  # noqa: PLR2004
        header[0] != magic or (header[1] == magic and header[2] != magic)
    )


def chained_frame_iterator(
    first_frame: jelly.RdfStreamFrame,
    input_stream: IO[bytes],
) -> Generator[jelly.RdfStreamFrame]:
    yield first_frame
    while frame := parse_length_prefixed(jelly.RdfStreamFrame, input_stream):
        yield frame


def get_options_and_frames(
    input_stream: IO[bytes],
) -> tuple[StreamOptions, Iterator[jelly.RdfStreamFrame]]:
    is_delimited = delimited_jelly_hint(bytes_read := input_stream.read(3))
    input_stream.seek(-len(bytes_read), os.SEEK_CUR)

    if is_delimited:
        first_frame = parse_length_prefixed(jelly.RdfStreamFrame, input_stream)
        options = options_from_frame(first_frame, delimited=True)
        return options, chained_frame_iterator(first_frame, input_stream)

    first_frame = parse(jelly.RdfStreamFrame, input_stream.read())
    options = options_from_frame(first_frame, delimited=False)
    return options, iter((first_frame,))
