from __future__ import annotations

import io
import random
import typing
from typing import cast

import pytest
from google.protobuf.descriptor import Descriptor

from pyjelly import jelly
from pyjelly.parse.ioutils import (
    FRAME_TAGS,
    delimited_jelly_hint,
    get_options_and_frames,
    read_hint_header,
)
from tests.utils import wire_fuzzer

FRAME_DESCRIPTOR = cast("Descriptor", jelly.RdfStreamFrame.DESCRIPTOR)

# A frame of 25 bytes: too long for its first byte to be a size.
FRAME_LARGE = jelly.RdfStreamFrame(
    rows=[
        jelly.RdfStreamRow(name=jelly.RdfNameEntry(id=0, value="name name name name"))
    ]
)
# A frame of exactly 10 bytes: its size is also the tag of the rows field.
FRAME_SIZE_10 = jelly.RdfStreamFrame(
    rows=[jelly.RdfStreamRow(name=jelly.RdfNameEntry(id=0, value="name"))]
)
# A frame of exactly 18 bytes: its size is also the tag of the columns field.
FRAME_SIZE_18 = jelly.RdfStreamFrame(
    rows=[
        jelly.RdfStreamRow(options=jelly.RdfStreamOptions(stream_name="abcdefghijkl"))
    ]
)


def detect(data: bytes) -> tuple[bool, bytes]:
    """Run the detection and return its answer plus every byte of the input."""
    inp = io.BytesIO(data)
    header = read_hint_header(inp)
    return delimited_jelly_hint(header), header + inp.read()


def delimit(*frames: jelly.RdfStreamFrame) -> bytes:
    out = b""
    for frame in frames:
        body = frame.SerializeToString()
        out += wire_fuzzer.varint(len(body)) + body
    return out


@pytest.mark.parametrize(
    "frame",
    [FRAME_LARGE, FRAME_SIZE_10, FRAME_SIZE_18],
    ids=["large", "size10", "size18"],
)
def test_detects_frames_written_both_ways(frame: jelly.RdfStreamFrame) -> None:
    non_delimited = frame.SerializeToString()
    assert detect(non_delimited) == (False, non_delimited)
    delimited = delimit(frame)
    assert detect(delimited) == (True, delimited)


def test_frame_sizes_are_what_the_cases_need() -> None:
    assert len(FRAME_LARGE.SerializeToString()) == 25
    assert len(FRAME_SIZE_10.SerializeToString()) == 10
    assert len(FRAME_SIZE_18.SerializeToString()) == 18
    # The sizes of the two small frames are themselves frame tags
    assert sorted(FRAME_TAGS) == [0x0A, 0x12, 0x7A]


def test_detects_a_lone_row_as_non_delimited() -> None:
    # 10 bytes that start with 0x0A, but are not a whole frame of 10 bytes
    row = jelly.RdfStreamRow(
        options=jelly.RdfStreamOptions(stream_name="name12")
    ).SerializeToString()
    assert len(row) == 10
    assert row[0] == 0x0A
    assert detect(row) == (False, row)


def test_detects_a_frame_with_the_columns_written_first() -> None:
    # Writers may put the fields of a frame in any order
    columns = jelly.RdfColumnBatch(row_count=0).SerializeToString()
    row = jelly.RdfStreamRow(
        options=jelly.RdfStreamOptions(physical_type=jelly.PHYSICAL_STREAM_TYPE_TRIPLES)
    ).SerializeToString()
    frame = (
        bytes([0x12])
        + wire_fuzzer.varint(len(columns))
        + columns
        + bytes([0x0A])
        + wire_fuzzer.varint(len(row))
        + row
    )
    assert frame[0] == 0x12
    assert detect(frame) == (False, frame)
    delimited = wire_fuzzer.varint(len(frame)) + frame
    assert detect(delimited) == (True, delimited)


def test_detects_empty_input_as_non_delimited() -> None:
    assert detect(b"") == (False, b"")


def test_detects_two_bytes_of_garbage_as_non_delimited() -> None:
    assert detect(bytes([0x12, 0x34])) == (False, bytes([0x12, 0x34]))


@pytest.mark.parametrize("count", [1, 2, 3])
def test_detects_empty_delimited_frames(count: int) -> None:
    # 0 is never a tag, so it can only be the size of an empty frame
    data = bytes(count)
    assert detect(data) == (True, data)


def test_detects_a_frame_whose_last_length_does_not_end() -> None:
    # Size 10, then a rows field whose length varint is cut off by the end
    data = bytes([0x0A, 0x0A, 0x00, 0x0A, 0x00, 0x0A, 0x00, 0x0A, 0x00, 0x0A, 0x80])
    assert detect(data) == (False, data)


def test_reads_only_what_the_hint_needs() -> None:
    inp = io.BytesIO(delimit(FRAME_LARGE))
    # 0x19 is not a frame tag, so one byte settles it
    assert read_hint_header(inp) == bytes([0x19])
    inp = io.BytesIO(delimit(FRAME_SIZE_10))
    assert len(read_hint_header(inp)) == 11


def test_reads_the_header_from_a_stream_that_dribbles() -> None:
    data = delimit(FRAME_SIZE_10)

    class OneByteAtATime(io.RawIOBase):
        def __init__(self) -> None:
            self.pos = 0

        def readable(self) -> bool:
            return True

        def read(self, size: int = -1) -> bytes:  # noqa: ARG002
            chunk = data[self.pos : self.pos + 1]
            self.pos += len(chunk)
            return chunk

    assert (
        read_hint_header(typing.cast("typing.IO[bytes]", OneByteAtATime())) == data[:11]
    )


ITERATIONS = 5_000


def test_fuzz_detects_random_non_delimited_frames() -> None:
    rnd = random.Random(1)
    failures = 0
    ambiguous = 0
    for _ in range(ITERATIONS):
        data = wire_fuzzer.message(FRAME_DESCRIPTOR, rnd)
        if data and data[0] in FRAME_TAGS:
            ambiguous += 1
        is_delimited, kept = detect(data)
        assert kept == data
        failures += is_delimited
    # A frame that does not start with the stream options can, by chance, also be a
    # valid delimited stream. That is rare and accepted.
    assert failures <= ITERATIONS // 1_000
    # Most cases start with a byte that could also be the size of a delimited frame
    assert ambiguous > ITERATIONS // 2


def test_fuzz_detects_random_delimited_streams() -> None:
    rnd = random.Random(2)
    ambiguous = 0
    for _ in range(ITERATIONS):
        data = b""
        for _ in range(rnd.randrange(1, 4)):
            body = wire_fuzzer.message(FRAME_DESCRIPTOR, rnd)
            data += wire_fuzzer.varint(len(body)) + body
        if data[0] in FRAME_TAGS:
            ambiguous += 1
        assert detect(data) == (True, data)
    # The first frame is sometimes 10, 18 or 122 bytes long: sizes that are also tags
    assert ambiguous > 0


def test_fuzz_keeps_every_byte_of_random_input() -> None:
    rnd = random.Random(3)
    for _ in range(ITERATIONS):
        length = rnd.randrange(400) if rnd.randrange(10) == 0 else rnd.randrange(24)
        data = bytes(rnd.randrange(256) for _ in range(length))
        _, kept = detect(data)
        assert kept == data


# A frame holding only the stream options.
OPTIONS_FRAME = b"\x0a\x0f\x0a\x0d\x10\x01\x48\x80\x01\x50\x0f\x58\x0f\x70\x01\x78\x01"


# Regression test for https://github.com/Jelly-RDF/pyjelly/issues/298
@pytest.mark.parametrize("seekable", [True, False], ids=["seekable", "no-seek"])
@pytest.mark.parametrize("delimited", [True, False], ids=["delimited", "non-delimited"])
def test_get_options_and_frames(
    monkeypatch: pytest.MonkeyPatch,
    seekable: bool,  # noqa: FBT001
    delimited: bool,  # noqa: FBT001
) -> None:
    data = OPTIONS_FRAME
    if delimited:
        data = wire_fuzzer.varint(len(data)) + data
    inp = io.BytesIO(data)
    if not seekable:
        make_unseekable(inp, monkeypatch)
    options, frames = get_options_and_frames(inp)
    assert options.params.delimited is delimited
    assert len(list(frames)) == 1


def make_unseekable(inp: io.BytesIO, monkeypatch: pytest.MonkeyPatch) -> None:
    """Take seek away from a bytes IO."""
    monkeypatch.setattr(inp, "seekable", lambda: False)

    def seek(
        offset: int,  # noqa: ARG001
        whence: typing.Any = 0,  # noqa: ARG001
    ) -> None:
        raise io.UnsupportedOperation

    monkeypatch.setattr(inp, "seek", seek)
