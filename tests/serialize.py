"""pyjelly CLI with RDFLib backend for tests."""

from __future__ import annotations

import argparse
from pathlib import Path

import rdflib

from pyjelly.consuming.ioutils import get_options_and_frames
from pyjelly.options import ConsumerStreamOptions
from pyjelly.producing.producers import ManualFrameProducer
from tests.utils.ordered_memory import OrderedMemory


def write_dataset(
    filenames: list[str | Path],
    out_filename: str | Path,
    *,
    quads: bool = False,
    options_from: str | Path | None = None,
) -> None:
    options = get_options_from(options_from)
    dataset = rdflib.Dataset()
    for filename in map(str, filenames):
        if filename.endswith(".nq"):
            dataset.parse(location=filename)
        else:
            graph = rdflib.Graph(identifier=filename, store=OrderedMemory())
            graph.parse(location=filename)
            dataset.add_graph(graph)
    with Path(out_filename).open("wb") as file:
        dataset.serialize(
            destination=file,
            quads=quads,
            format="jelly",
            options=options,
        )


def write_graph(
    filename: str | Path,
    *,
    out_filename: str | Path,
    options_from: str | Path | None = None,
    one_frame: bool = False,
) -> None:
    options = get_options_from(options_from)
    graph = rdflib.Graph(store=OrderedMemory())
    graph.parse(location=str(filename))
    producer = None
    if one_frame:
        producer = ManualFrameProducer(jelly_type=options.logical_type)
    with Path(out_filename).open("wb") as file:
        graph.serialize(
            destination=file,
            format="jelly",
            options=options,
            producer=producer,
        )


def get_options_from(
    options_filename: str | Path | None = None,
) -> ConsumerStreamOptions | None:
    if options_filename is not None:
        with Path(options_filename).open("rb") as options_file:
            options, _ = get_options_and_frames(options_file)
    else:
        options = None
    return options


def write_graph_or_dataset(
    first: str | Path,
    *extra: str | Path,
    graphs: bool = False,
    out_filename: str | Path = "out.jelly",
    options_from: str | Path | None = None,
    one_per_frame: bool = False,
) -> None:
    if str(first).endswith(".nq") or extra or graphs:
        write_dataset(
            [first, *extra],
            out_filename=out_filename,
            quads=not graphs,
            options_from=options_from,
        )
    else:
        write_graph(
            first,
            out_filename=out_filename,
            options_from=options_from,
            one_frame=one_per_frame,
        )


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("first", type=str)
    cli.add_argument("extra", nargs="*", type=str)
    cli.add_argument("out", nargs="?", default="out.jelly", type=str)
    cli.add_argument("--graphs", action="store_true")
    cli.add_argument("--options-from", type=str)
    args = cli.parse_args()
    write_graph_or_dataset(
        args.first,
        *args.extra,
        graphs=args.graphs,
        out_filename=args.out,
        options_from=args.options_from,
    )
