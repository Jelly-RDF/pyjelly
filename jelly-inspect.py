import contextvars
import typing as t
from collections import Counter
from collections.abc import Generator
from dataclasses import dataclass
from operator import itemgetter
from pathlib import Path

import rich
import rich.repr
import typer

from pyjelly.pb2.rdf_pb2 import RdfStreamFrame
from pyjelly.stream import get_frames

app = typer.Typer(no_args_is_help=True)

verbose_ctx = contextvars.ContextVar[bool]("verbose_ctx")


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
            key=itemgetter(slice(None, None, -1)),  # Sort by (value, name)
        )


@app.command("frames", help="Print protocol objects of a serialized Jelly file")
def print_frames(file: Path) -> None:
    counter = 0

    with file.open("rb+") as stream:
        for frame in get_frames(stream):
            if verbose_ctx.get():
                rich.print(frame)
            else:
                rich.print(Frame(frame))
            counter += 1

    rich.print(f"{counter} frames total")


@app.callback(invoke_without_command=True)
def main(verbose: t.Annotated[bool, typer.Option("-v")] = False) -> None:
    verbose_ctx.set(verbose)


if __name__ == "__main__":
    app()
