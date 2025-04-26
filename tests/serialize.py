"""pyjelly CLI with RDFLib backend for tests."""

from __future__ import annotations

import argparse
from pathlib import Path

import rdflib

from tests.utils.ordered_memory import OrderedMemory


def write_dataset(
    filenames: list[str],
    output_filename: str | Path,
    *,
    quads: bool = False,
) -> None:
    dataset = rdflib.Dataset()
    for filename in filenames:
        if filename.endswith(".nq"):
            dataset.parse(location=filename)
        else:
            graph = rdflib.Graph(identifier=filename, store=OrderedMemory())
            graph.parse(location=filename)
            dataset.add_graph(graph)
    with Path(output_filename).open("wb") as file:
        dataset.serialize(destination=file, quads=quads, format="jelly")


def write_graph(filename: str, output_filename: str | Path) -> None:
    graph = rdflib.Graph(store=OrderedMemory())
    graph.parse(location=filename)
    with Path(output_filename).open("wb") as file:
        graph.serialize(destination=file, format="jelly")


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("first", type=str)
    cli.add_argument("extra", nargs="*", type=str)
    cli.add_argument("output", nargs="?", default="out.jelly", type=str)
    cli.add_argument("--graphs", action="store_true")
    args = cli.parse_args()
    quads = args.first.endswith(".nq")
    if quads or args.extra or args.graphs:
        write_dataset(
            [args.first, *args.extra],
            output_filename=args.output,
            quads=not args.graphs,
        )
    else:
        write_graph(args.first, output_filename=args.output)
