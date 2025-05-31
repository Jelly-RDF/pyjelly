"""pyjelly CLI with RDFLib backend for tests."""

from __future__ import annotations

import argparse
from pathlib import Path

import rdflib

from pyjelly.parse.decode import ParserOptions
from pyjelly.parse.ioutils import get_options_and_frames
from pyjelly.serialize.streams import SerializerOptions, stream_for_type
from tests.utils.ordered_memory import OrderedMemory


def write_dataset(
    filenames: list[str | Path],
    out_filename: str | Path,
    options: str | ParserOptions | Path | None = None,
) -> None:
    if not isinstance(options, ParserOptions):
        options = get_options_from(options)
    assert options
    dataset = rdflib.Dataset(store=OrderedMemory())
    for filename in map(str, filenames):
        if filename.endswith(".nq"):
            dataset.parse(location=filename)
        else:
            graph = rdflib.Graph(identifier=filename, store=OrderedMemory())
            graph.parse(location=filename)
            dataset.add_graph(graph)
    stream = stream_for_type(options.stream_types.physical_type).for_rdflib(
        SerializerOptions(
            logical_type=options.stream_types.logical_type,
            lookup_preset=options.lookup_preset,
            params=options.params,
        )
    )
    with Path(out_filename).open("wb") as file:
        dataset.serialize(destination=file, format="jelly", stream=stream)


def write_graph(
    filename: str | Path,
    *,
    out_filename: str | Path,
    options: str | ParserOptions | Path | None = None,
) -> None:
    if not isinstance(options, ParserOptions):
        options = get_options_from(options)
    assert options
    graph = rdflib.Graph(store=OrderedMemory())
    graph.parse(location=str(filename))

    stream = stream_for_type(options.stream_types.physical_type).for_rdflib(
        SerializerOptions(
            lookup_preset=options.lookup_preset,
            logical_type=options.stream_types.logical_type,
            params=options.params,
        )
    )
    with Path(out_filename).open("wb") as file:
        graph.serialize(
            destination=file,
            format="jelly",
            stream=stream,
        )


def get_options_from(
    options_filename: str | Path | None = None,
) -> ParserOptions | None:
    if options_filename is not None:
        with Path(options_filename).open("rb") as options_file:
            options, _ = get_options_and_frames(options_file)
    else:
        options = None
    return options


def write_graph_or_dataset(
    first: str | Path,
    *extra: str | Path,
    out_filename: str | Path = "out.jelly",
    options: str | Path | ParserOptions | None = None,
) -> None:
    if str(first).endswith(".nq") or extra:
        write_dataset([first, *extra], out_filename=out_filename, options=options)
    else:
        write_graph(first, out_filename=out_filename, options=options)


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("first", type=str)
    cli.add_argument("extra", nargs="*", type=str)
    cli.add_argument("out", nargs="?", default="out.jelly", type=str)
    cli.add_argument("--options-from", type=str)
    args = cli.parse_args()
    write_graph_or_dataset(
        args.first,
        *args.extra,
        out_filename=args.out,
        options=args.options_from,
    )
