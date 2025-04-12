"""pyjelly CLI with RDFLib backend for tests."""

from __future__ import annotations

import argparse
from pathlib import Path

import rdflib

from tests.utils.ordered_memory import OrderedMemory


def main(location: str, output: str | Path) -> None:
    graph = rdflib.Graph(store=OrderedMemory())
    graph.parse(location=location)
    with Path(output).open("wb") as file:
        graph.serialize(destination=file, format="jelly")


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("location", type=str)
    cli.add_argument("output", type=str)
    args = cli.parse_args()
    main(location=args.location, output=args.output)
