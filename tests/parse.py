"""pyjelly CLI with RDFLib backend for tests."""

from __future__ import annotations

import argparse
from pathlib import Path

import rdflib


def parse_and_serialize_graph_or_dataset(
    location: str | Path,
    output: str | Path,
) -> None:
    ds = rdflib.Dataset()
    ds.parse(location=str(location), format="jelly")
    ds.serialize(output, format="jelly")


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("location", nargs="?", default="out.jelly", type=str)
    cli.add_argument("output", nargs="?", default="out-parsed.jelly", type=str)
    args = cli.parse_args()
    parse_and_serialize_graph_or_dataset(location=args.location, output=args.output)
