"""
Random messages in the protobuf wire format, built from a message descriptor.

Ported from jelly-jvm's `WireFuzzer`. Every field may be skipped, a repeated
field gets 0-3 values (packed or not, if it can be packed), and the fields are
written in a random order. Values and strings are kept small, so that many
messages are only a few bytes long. The content is not valid Jelly: this is for
code that only looks at the wire format, such as the detection of delimited
streams.
"""

from __future__ import annotations

import random
from typing import Final

from google.protobuf.descriptor import Descriptor, FieldDescriptor

MAX_DEPTH: Final = 4

WIRE_VARINT: Final = 0
WIRE_FIXED64: Final = 1
WIRE_LENGTH_DELIMITED: Final = 2
WIRE_FIXED32: Final = 5

_WIRE_TYPES: Final = {
    FieldDescriptor.TYPE_MESSAGE: WIRE_LENGTH_DELIMITED,
    FieldDescriptor.TYPE_STRING: WIRE_LENGTH_DELIMITED,
    FieldDescriptor.TYPE_BYTES: WIRE_LENGTH_DELIMITED,
    FieldDescriptor.TYPE_DOUBLE: WIRE_FIXED64,
    FieldDescriptor.TYPE_FIXED64: WIRE_FIXED64,
    FieldDescriptor.TYPE_SFIXED64: WIRE_FIXED64,
    FieldDescriptor.TYPE_FLOAT: WIRE_FIXED32,
    FieldDescriptor.TYPE_FIXED32: WIRE_FIXED32,
    FieldDescriptor.TYPE_SFIXED32: WIRE_FIXED32,
}


def varint(value: int) -> bytes:
    """Encode an unsigned 64-bit value as a protobuf varint."""
    out = bytearray()
    value &= (1 << 64) - 1
    while True:
        byte = value & 0x7F
        value >>= 7
        if not value:
            out.append(byte)
            return bytes(out)
        out.append(byte | 0x80)


def message(descriptor: Descriptor, rnd: random.Random, depth: int = 0) -> bytes:
    """Write a random message of the given type in the wire format."""
    parts = []
    for field in descriptor.fields:
        if field.type == FieldDescriptor.TYPE_GROUP:
            continue
        if field.type == FieldDescriptor.TYPE_MESSAGE and depth >= MAX_DEPTH:
            continue
        if field.is_repeated:
            count = rnd.randrange(4)
            if count and _packable(field) and rnd.randrange(2):
                parts.append(_packed(field, count, rnd))
            else:
                parts.extend(_single(field, rnd, depth) for _ in range(count))
        elif rnd.randrange(3) == 0:
            parts.append(_single(field, rnd, depth))
    rnd.shuffle(parts)
    return b"".join(parts)


def _wire_type(field: FieldDescriptor) -> int:
    return _WIRE_TYPES.get(field.type, WIRE_VARINT)


def _packable(field: FieldDescriptor) -> bool:
    return _wire_type(field) != WIRE_LENGTH_DELIMITED


def _tag(field: FieldDescriptor, wire_type: int) -> bytes:
    return varint((field.number << 3) | wire_type)


def _single(field: FieldDescriptor, rnd: random.Random, depth: int) -> bytes:
    if field.type == FieldDescriptor.TYPE_MESSAGE:
        assert field.message_type is not None
        body = message(field.message_type, rnd, depth + 1)
    elif field.type in (FieldDescriptor.TYPE_STRING, FieldDescriptor.TYPE_BYTES):
        body = bytes(rnd.randrange(ord("a"), ord("z") + 1) for _ in range(_length(rnd)))
    else:
        return _tag(field, _wire_type(field)) + _value(field, rnd)
    return _tag(field, WIRE_LENGTH_DELIMITED) + varint(len(body)) + body


def _packed(field: FieldDescriptor, count: int, rnd: random.Random) -> bytes:
    values = b"".join(_value(field, rnd) for _ in range(count))
    return _tag(field, WIRE_LENGTH_DELIMITED) + varint(len(values)) + values


def _value(field: FieldDescriptor, rnd: random.Random) -> bytes:
    wire_type = _wire_type(field)
    if wire_type == WIRE_FIXED64:
        return rnd.getrandbits(64).to_bytes(8, "little")
    if wire_type == WIRE_FIXED32:
        return rnd.getrandbits(32).to_bytes(4, "little")
    # Mostly small values, sometimes up to the full 64 bits
    choice = rnd.randrange(4)
    if choice == 0:
        return varint(rnd.getrandbits(64))
    if choice == 1:
        return varint(rnd.randrange(1 << 20))
    return varint(rnd.randrange(130))


def _length(rnd: random.Random) -> int:
    return rnd.randrange(200) if rnd.randrange(8) == 0 else rnd.randrange(13)
