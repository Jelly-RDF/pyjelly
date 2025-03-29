# ruff: noqa: PGH004
# ruff: noqa
from __future__ import annotations

import contextvars
import io
import os
import typing as t
from collections import Counter
from collections.abc import Generator
from dataclasses import dataclass
from operator import itemgetter
from pathlib import Path

import rich
import rich.repr
import typer
from google.protobuf.proto import (  # type: ignore
    parse_length_prefixed,
)

from pyjelly.pb2.rdf_pb2 import RdfStreamFrame

app = typer.Typer(no_args_is_help=True)

verbose_ctx = contextvars.ContextVar[bool]("verbose_ctx")


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
        header[0] != 0x0A or (header[1] == 0x0A and header[2] != 0x0A)
    )


def get_frames(input_stream: t.IO[bytes]) -> Generator[RdfStreamFrame]:
    delimited = hints_multiple_frames(bytes_read := input_stream.read(3))
    input_stream.seek(-len(bytes_read), os.SEEK_CUR)

    if delimited:
        while message := parse_length_prefixed(
            RdfStreamFrame, t.cast("io.BytesIO", input_stream)
        ):
            yield message
    else:
        yield RdfStreamFrame.FromString(input_stream.read())


@rich.repr.auto
@dataclass
class Frame:
    pb_frame: RdfStreamFrame

    def __rich_repr__(self) -> Generator[tuple[str, int]]:
        yield from sorted(
            Counter(
                row_field.name
                for row in self.pb_frame.rows
                for row_field, _ in row.ListFields()
            ).items(),
        )


@rich.repr.auto
class StreamOptions:
    def __init__(self, **kwds: t.Any):
        self.__dict__.update(kwds)

    def __rich_repr__(self) -> Generator[tuple[str, t.Any]]:
        yield from self.__dict__.items()


@app.command("frames", help="Print protocol objects of a serialized Jelly file")
def print_frames(file: Path, out: Path) -> None:
    count = 0

    with file.open("rb+") as stream, out.open("w") as out_file:
        for count, frame in enumerate(get_frames(stream)):
            for row in frame.rows:
                if row.WhichOneof("row") == "options":
                    rich.print(
                        StreamOptions(
                            **{
                                field.name: value
                                for field, value in row.options.ListFields()
                            }
                        ),
                    )
            if verbose_ctx.get():
                rich.print(frame)
            else:
                rich.print(Frame(frame))

            out_file.write(f"# Frame {count}\n")
            out_file.write(repr(frame))

    rich.print(f"{count + 1} frame(s) total")


@app.callback(invoke_without_command=True)
def main(verbose: t.Annotated[bool, typer.Option("-v")] = False) -> None:
    verbose_ctx.set(verbose)


if __name__ == "__main__":
    app()
