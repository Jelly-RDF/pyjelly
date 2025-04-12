"""pyjelly CLI with RDFLib backend for tests."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import TYPE_CHECKING

import rdflib

from tests.utils.ordered_memory import OrderedMemory

if TYPE_CHECKING:
    from _typeshed import StrPath


def main(location: str, output: StrPath) -> None:
    graph = rdflib.Graph(store=OrderedMemory())
    graph.parse(location=location)
    with Path(output).open("wb") as file:
        graph.serialize(destination=file, format="jelly")


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("location", type=str)
    cli.add_argument("output", type=str)
    main(**vars(cli.parse_args()))
