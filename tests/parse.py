"""pyjelly CLI with RDFLib backend for tests."""

from __future__ import annotations

import argparse

import rdflib

from pyjelly.options import register_mimetypes
from tests.utils.ordered_memory import OrderedMemory


def main(location: str, output: str) -> None:
    graph = rdflib.Graph(store=OrderedMemory())
    graph.parse(location=location, format="jelly")
    graph.serialize(output, format="jelly")


if __name__ == "__main__":
    register_mimetypes()
    cli = argparse.ArgumentParser()
    cli.add_argument("location", nargs="?", default="out.jelly", type=str)
    cli.add_argument("output", nargs="?", default="out-parsed.jelly", type=str)
    args = cli.parse_args()
    main(location=args.location, output=args.output)
