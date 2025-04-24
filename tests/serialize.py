"""pyjelly CLI with RDFLib backend for tests."""

from __future__ import annotations

import argparse
from pathlib import Path

import rdflib

from tests.utils.ordered_memory import OrderedMemory


def serialize_dataset(
    locations: list[str],
    output: str | Path,
    *,
    quads: bool = False,
) -> None:
    dataset = rdflib.Dataset()
    for location in locations:
        if quads:
            dataset.parse(location=location)
        else:
            graph = rdflib.Graph(identifier=location, store=OrderedMemory())
            graph.parse(location=location)
            dataset.add_graph(graph)
    with Path(output).open("wb") as file:
        dataset.serialize(destination=file, quads=quads, format="jelly")


def serialize_graph(location: str, output: str | Path) -> None:
    graph = rdflib.Graph(store=OrderedMemory())
    graph.parse(location=location)
    with Path(output).open("wb") as file:
        graph.serialize(destination=file, format="jelly")


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("first", type=str)
    cli.add_argument("extra", nargs="*", type=str)
    cli.add_argument("output", nargs="?", default="out.jelly", type=str)
    args = cli.parse_args()
    quads = args.first.endswith(".nq")
    if quads or args.extra:
        serialize_dataset(
            [args.first, *args.extra],
            output=args.output,
            quads=args.first.endswith(".nq"),
        )
    else:
        serialize_graph(args.first, output=args.output)
