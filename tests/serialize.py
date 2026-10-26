"""Writing Jelly files from N-Triples and N-Quads files, for tests."""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from dataclasses import replace
from pathlib import Path

import rdflib

from pyjelly import jelly
from pyjelly.integrations.generic.generic_sink import GenericStatementSink
from pyjelly.integrations.generic.serialize import GenericSinkTermEncoder
from pyjelly.integrations.generic.serialize import (
    stream_frames as generic_stream_frames,
)
from pyjelly.integrations.rdflib.serialize import stream_frames
from pyjelly.parse.decode import ParserOptions
from pyjelly.parse.ioutils import get_options_and_frames
from pyjelly.serialize.ioutils import write_delimited
from pyjelly.serialize.streams import SerializerOptions, Stream, stream_for_type
from tests.utils.generic_sink_test_parser import GenericSinkParser
from tests.utils.ordered_memory import OrderedMemory


def get_options_from(
    options_filename: str | Path | None = None,
) -> ParserOptions | None:
    if options_filename is not None:
        with Path(options_filename).open("rb") as options_file:
            options, _ = get_options_and_frames(options_file)
    else:
        options = None
    return options


def _stream(options: str | Path | ParserOptions, *, generic: bool) -> Stream:
    """Make a stream with the options of a test (a stream options file)."""
    if not isinstance(options, ParserOptions):
        parsed = get_options_from(options)
        assert parsed
        options = parsed
    serializer_options = SerializerOptions(
        logical_type=options.stream_types.logical_type,
        lookup_preset=options.lookup_preset,
        # The inputs have no namespaces, but rdflib binds some by default
        params=replace(options.params, namespace_declarations=False),
    )
    stream_class = stream_for_type(options.stream_types.physical_type)
    if not generic:
        return stream_class.for_rdflib(serializer_options)
    return stream_class(
        encoder=GenericSinkTermEncoder(lookup_preset=options.lookup_preset),
        options=serializer_options,
    )


def _write(frames: Iterable[jelly.RdfStreamFrame], out_filename: str | Path) -> None:
    with Path(out_filename).open("wb") as file:
        for frame in frames:
            write_delimited(frame, file)


def write_graph_or_dataset(
    first: str | Path,
    *extra: str | Path,
    out_filename: str | Path = "out.jelly",
    options: str | Path | ParserOptions,
) -> None:
    """Write the files with the rdflib integration, one graph or dataset each."""
    stream = _stream(options, generic=False)
    quads = stream.physical_type == jelly.PHYSICAL_STREAM_TYPE_QUADS
    messages = stream.batch is not None and stream.batch.messages
    # Blank node labels are scoped to the stream, or to each RDF Message
    bnode_context: dict[str, rdflib.BNode] = {}

    def frames() -> Iterable[jelly.RdfStreamFrame]:
        for filename in map(str, [first, *extra]):
            sink: rdflib.Graph
            if quads:
                sink = rdflib.Dataset(store=OrderedMemory())
            else:
                sink = rdflib.Graph(store=OrderedMemory())
            sink.parse(
                location=filename,
                format="nquads" if quads else "nt",
                bnode_context={} if messages else bnode_context,
            )
            yield from stream_frames(stream, sink)

    _write(frames(), out_filename)


def write_generic_sink(
    first: str | Path,
    *extra: str | Path,
    out_filename: str | Path = "out.jelly",
    options: str | Path | ParserOptions,
) -> None:
    """Write the files with the generic integration, one sink each."""
    stream = _stream(options, generic=True)

    def frames() -> Iterable[jelly.RdfStreamFrame]:
        for filename in [first, *extra]:
            sink = GenericStatementSink()
            GenericSinkParser(sink).parse(Path(filename))
            yield from generic_stream_frames(stream, sink)

    _write(frames(), out_filename)


if __name__ == "__main__":
    cli = argparse.ArgumentParser()
    cli.add_argument("first", type=str)
    cli.add_argument("extra", nargs="*", type=str)
    cli.add_argument("--out", default="out.jelly", type=str)
    cli.add_argument("--options-from", type=str, required=True)
    args = cli.parse_args()
    write_graph_or_dataset(
        args.first,
        *args.extra,
        out_filename=args.out,
        options=args.options_from,
    )
