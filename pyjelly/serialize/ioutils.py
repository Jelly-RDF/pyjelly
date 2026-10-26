from typing import IO

from google.protobuf.message import Message

_VARINT_PAYLOAD = 0x7F
_VARINT_CONTINUATION = 0x80


def varint(value: int) -> bytes:
    r"""
    Return a non-negative integer as a protobuf varint.

    >>> varint(300)
    b'\xac\x02'
    """
    out = bytearray()
    while value > _VARINT_PAYLOAD:
        out.append((value & _VARINT_PAYLOAD) | _VARINT_CONTINUATION)
        value >>= 7
    out.append(value)
    return bytes(out)


def write_delimited(frame: Message, output_stream: IO[bytes]) -> None:
    """Write a frame, after its size (the delimited variant of Jelly)."""
    # Not protobuf's serialize_length_prefixed(), which needs write() to return the
    # number of bytes written. Not every stream does, e.g., rdflib's for results.
    data = frame.SerializeToString()
    output_stream.write(varint(len(data)))
    output_stream.write(data)


def write_single(frame: Message, output_stream: IO[bytes]) -> None:
    output_stream.write(frame.SerializeToString(deterministic=True))
