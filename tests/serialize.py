"""pyjelly CLI with RDFLib backend for tests."""

from __future__ import annotations

import argparse
from pathlib import Path

import rdflib

from pyjelly.options import register_mimetypes
from tests.utils.ordered_memory import OrderedMemory


def main(location: str, output: str | Path) -> None:
    graph = rdflib.Graph(store=OrderedMemory())
    graph.parse(location=location)
    with Path(output).open("wb") as file:
        graph.serialize(destination=file, format="jelly")


if __name__ == "__main__":
    register_mimetypes()
    cli = argparse.ArgumentParser()
    cli.add_argument("location", type=str)
    cli.add_argument("output", nargs="?", default="out.jelly", type=str)
    args = cli.parse_args()
    main(location=args.location, output=args.output)
