"""pyjelly CLI with RDFLib backend for tests."""

from __future__ import annotations

import argparse
from pathlib import Path

import rdflib

from pyjelly.integrations.generic.generic_sink import GenericStatementSink
from pyjelly.integrations.generic.serialize import GenericSinkTermEncoder
from pyjelly.integrations.generic.serialize import (
    stream_frames as generic_stream_frames,
)
from pyjelly.integrations.rdflib.serialize import stream_frames
from pyjelly.parse.decode import ParserOptions
from pyjelly.parse.ioutils import get_options_and_frames
from pyjelly.serialize.ioutils import write_delimited
from pyjelly.serialize.streams import SerializerOptions, stream_for_type
from tests.utils.generic_sink_test_parser import GenericSinkParser
from tests.utils.ordered_memory import OrderedMemory


def write_dataset(
    filenames: list[str | Path],
    out_filename: str | Path,
    options: str | ParserOptions | Path | None = None,
) -> None:
    if not isinstance(options, ParserOptions):
        options = get_options_from(options)
    assert options
    stream = stream_for_type(options.stream_types.physical_type).for_rdflib(
        SerializerOptions(
            logical_type=options.stream_types.logical_type,
            lookup_preset=options.lookup_preset,
            params=options.params,
        )
    )
    with Path(out_filename).open("wb") as file:
        sink: rdflib.Dataset | rdflib.Graph
        for filename in map(str, filenames):
            if filename.endswith(".nq"):
                sink = rdflib.Dataset(store=OrderedMemory())
                sink.parse(location=filename)
            else:
                sink = rdflib.Graph(identifier=filename, store=OrderedMemory())
                sink.parse(location=filename)
            if frames := next(stream_frames(stream, sink)):
                write_delimited(frames, file)


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


def write_generic_sink(
    first: str | Path,
    *extra: str | Path,
    out_filename: str | Path = "out.jelly",
    options: str | Path | ParserOptions | None = None,
) -> None:
    if not isinstance(options, ParserOptions):
        options = get_options_from(options)
    assert options
    if options is not None:
        lookup_preset = options.lookup_preset
    stream_class = stream_for_type(options.stream_types.physical_type)
    stream = stream_class(
        encoder=GenericSinkTermEncoder(lookup_preset=lookup_preset),
        options=SerializerOptions(
            lookup_preset=options.lookup_preset,
            logical_type=options.stream_types.logical_type,
            params=options.params,
        ),
    )
    filenames = [first, *extra]
    with Path(out_filename).open("wb") as file:
        sink: GenericStatementSink
        for filename in map(str, filenames):
            sink = GenericStatementSink()
            sink_parser = GenericSinkParser(sink)
            sink_parser.parse(Path(filename))
            if frames := next(generic_stream_frames(stream, sink)):
                write_delimited(frames, file)


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
