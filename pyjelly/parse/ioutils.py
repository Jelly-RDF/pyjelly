import os
from collections.abc import Generator, Iterator
from itertools import chain
from typing import IO

from google.protobuf.proto import parse, parse_length_prefixed

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.parse.decode import ParserOptions, options_from_frame


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


def frame_iterator(inp: IO[bytes]) -> Generator[jelly.RdfStreamFrame]:
    while frame := parse_length_prefixed(jelly.RdfStreamFrame, inp):
        yield frame


def get_options_and_frames(
    inp: IO[bytes],
) -> tuple[ParserOptions, Iterator[jelly.RdfStreamFrame]]:
    """
    Return stream options and frames from the buffered binary stream.

    Args:
        inp (IO[bytes]): jelly buffered binary stream

    Raises:
        JellyConformanceError: if no non-empty frames detected in the delimited stream
        JellyConformanceError: if non-delimited,
            error is raised if no rows are detected (empty frame)

    Returns:
        tuple[ParserOptions, Iterator[jelly.RdfStreamFrame]]: ParserOptions holds:
            stream types, lookup presets and other stream options

    """
    is_delimited = delimited_jelly_hint(bytes_read := inp.read(3))
    inp.seek(-len(bytes_read), os.SEEK_CUR)

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
            msg = "No non-empty frames found in the stream"
            raise JellyConformanceError(msg)

        options = options_from_frame(first_frame, delimited=True)
        return options, chain(skipped_frames, (first_frame,), frames)

    frame = parse(jelly.RdfStreamFrame, inp.read())

    if not frame.rows:
        msg = "The stream is corrupted (only contains an empty frame)"
        raise JellyConformanceError(msg)

    options = options_from_frame(frame, delimited=False)
    return options, iter((frame,))
