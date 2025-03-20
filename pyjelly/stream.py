from __future__ import annotations

import enum
import io
import os
import typing as t
from collections.abc import Generator

from google.protobuf.proto import parse_length_prefixed

from pyjelly.pb2.rdf_pb2 import RdfStreamFrame


def hints_multiple_frames(header: bytes) -> bool:
    """
    Detect whether a Jelly file is delimited from its first 3 bytes.

    Truth table (notation: `0A` = `0x0A`, `NN` = `not 0x0A`, `??` = _don't care_):

    | Byte 1 | Byte 2 | Byte 3 | Result                                   |
    |--------|--------|--------|------------------------------------------|
    | `NN`   |  `??`  |  `??`  | Delimited                                |
    | `0A`   |  `NN`  |  `??`  | Non-delimited                            |
    | `0A`   |  `0A`  |  `NN`  | Delimited (size = 10)                    |
    | `0A`   |  `0A`  |  `0A`  | Non-delimited (stream options size = 10) |

    >>> hints_multiple_frames(bytes([0x00, 0x00, 0x00]))
    True

    >>> hints_multiple_frames(bytes([0x00, 0x00, 0x0A]))
    True

    >>> hints_multiple_frames(bytes([0x00, 0x0A, 0x00]))
    True

    >>> hints_multiple_frames(bytes([0x00, 0x0A, 0x0A]))
    True

    >>> hints_multiple_frames(bytes([0x0A, 0x00, 0x00]))
    False

    >>> hints_multiple_frames(bytes([0x0A, 0x00, 0x0A]))
    False

    >>> hints_multiple_frames(bytes([0x0A, 0x0A, 0x00]))
    True

    >>> hints_multiple_frames(bytes([0x0A, 0x0A, 0x0A]))
    False
    """
    return len(header) == 3 and (
        header[0] != 0x0A or header[1] == 0x0A and header[2] != 0x0A
    )


def get_frames(input_stream: t.IO[bytes]) -> Generator[RdfStreamFrame]:
    delimited = hints_multiple_frames(bytes_read := input_stream.read(3))
    input_stream.seek(-len(bytes_read), os.SEEK_CUR)

    if delimited:
        while message := parse_length_prefixed(
            RdfStreamFrame, t.cast(io.BytesIO, input_stream)
        ):
            yield message
    else:
        yield RdfStreamFrame.FromString(input_stream.read())


class StreamPhysicalType(str, enum.Enum):
    UNSPECIFIED = "UNSPECIFIED"
    TRIPLES = "TRIPLES"


class StreamLogicalType(str, enum.Enum):
    UNSPECIFIED = "UNSPECIFIED"
    FLAT_TRIPLES = "FLAT_TRIPLES"


class StreamOptions(t.NamedTuple):
    stream_name: str
    physical_type: StreamPhysicalType

    generalized_statements: t.Literal[False]
    """Not supported."""

    rdf_star: t.Literal[False]
    """Not supported."""

    max_name_table_size: int
    max_prefix_table_size: int
    max_datatype_table_size: int

    logical_type: StreamLogicalType

    version: int
