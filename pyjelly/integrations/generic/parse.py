from __future__ import annotations

from collections.abc import Callable, Generator, Iterable, MutableMapping
from contextvars import ContextVar
from typing import IO

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.generic.decode import (
    GenericGraphsAdapter,
    GenericQuadsAdapter,
    GenericQuadsBaseAdapter,
    GenericTriplesAdapter,
)
from pyjelly.integrations.generic.generic_sink import (
    GenericStatementSink,
    Prefix,
    Quad,
    Triple,
)
from pyjelly.parse.decode import Decoder, ParserOptions
from pyjelly.parse.ioutils import get_options_and_frames

Statement = Triple | Quad


def parse_triples_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    frame_metadata: ContextVar[MutableMapping[str, bytes]] | None = None,
) -> Generator[Iterable[Triple | Prefix]]:
    """
    Parse flat triple stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options
        frame_metadata: (ContextVar[ScalarMap[str, bytes]]): context variable
                used for extracting frame metadata

    Yields:
        Generator[Iterable[Triple | Prefix]]:
            Generator of iterables of Triple or Prefix objects,
            one iterable per frame.

    """
    adapter = GenericTriplesAdapter(options)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        if frame_metadata is not None:
            frame_metadata.set(
                frame.metadata
            ) if frame.metadata else frame_metadata.set({})
        yield decoder.iter_rows(frame)
    return


def parse_quads_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    frame_metadata: ContextVar[MutableMapping[str, bytes]] | None = None,
) -> Generator[Iterable[Quad | Prefix]]:
    """
    Parse flat quads stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options
        frame_metadata: (ContextVar[ScalarMap[str, bytes]]): context variable
                used for extracting frame metadata

    Yields:
        Generator[Iterable[Quad | Prefix]]:
            Generator of iterables of Quad or Prefix objects,
            one iterable per frame.

    """
    adapter_class: type[GenericQuadsBaseAdapter]
    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_QUADS:
        adapter_class = GenericQuadsAdapter
    else:
        adapter_class = GenericGraphsAdapter
    adapter = adapter_class(options=options)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        if frame_metadata is not None:
            frame_metadata.set(
                frame.metadata
            ) if frame.metadata else frame_metadata.set({})
        yield decoder.iter_rows(frame)
    return


def parse_jelly_grouped(
    inp: IO[bytes],
    sink_factory: Callable[[], GenericStatementSink] = GenericStatementSink,
    *,
    logical_type_strict: bool = False,
    frame_metadata: ContextVar[MutableMapping[str, bytes]] | None = None,
) -> Generator[GenericStatementSink]:
    """
    Take a jelly file and return generators of generic statements sinks.

    Yields one generic statements sink per frame.

    Args:
        inp (IO[bytes]): input jelly buffered binary stream
        sink_factory (Callable): lambda to construct a statement sink.
            By default, creates an empty in-memory GenericStatementSink.
        logical_type_strict (bool): If True, validate the *logical* type
            in stream options and require a grouped logical type.
            Otherwise, only the physical type is used to route parsing.
        frame_metadata: (ContextVar[ScalarMap[str, bytes]]): context variable
                used for extracting frame metadata

    Raises:
        NotImplementedError: is raised if a physical type is not implemented

    Yields:
        Generator[GenericStatementSink]:
            returns generators for GenericStatementSink, regardless of stream type.

    """
    options, frames = get_options_and_frames(inp)

    st = getattr(options, "stream_types", None)
    if logical_type_strict and (
        st is None
        or st.logical_type == jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED
        or st.flat
    ):
        lt_name = (
            "UNSPECIFIED"
            if st is None
            else jelly.LogicalStreamType.Name(st.logical_type)
        )

        msg = (
            "strict logical type check requires options.stream_types"
            if st is None
            else f"expected GROUPED logical type, got {lt_name}"
        )
        raise JellyConformanceError(msg)

    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        for graph in parse_triples_stream(
            frames=frames,
            options=options,
            **{"frame_metadata": frame_metadata} if frame_metadata is not None else {},
        ):
            sink = sink_factory()
            for graph_item in graph:
                if isinstance(graph_item, Prefix):
                    sink.bind(graph_item.prefix, graph_item.iri)
                else:
                    sink.add(graph_item)
            yield sink
        return
    elif options.stream_types.physical_type in (
        jelly.PHYSICAL_STREAM_TYPE_QUADS,
        jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
    ):
        for dataset in parse_quads_stream(
            frames=frames,
            options=options,
            **{"frame_metadata": frame_metadata} if frame_metadata is not None else {},
        ):
            sink = sink_factory()
            for item in dataset:
                if isinstance(item, Prefix):
                    sink.bind(item.prefix, item.iri)
                else:
                    sink.add(item)
            yield sink
        return

    physical_type_name = jelly.PhysicalStreamType.Name(
        options.stream_types.physical_type
    )
    msg = f"the stream type {physical_type_name} is not supported "
    raise NotImplementedError(msg)


def parse_jelly_to_graph(
    inp: IO[bytes],
    sink_factory: Callable[[], GenericStatementSink] = GenericStatementSink,
) -> GenericStatementSink:
    """
    Add statements from Generator to GenericStatementSink.

    Args:
        inp (IO[bytes]): input jelly stream.
        sink_factory (Callable[[], GenericStatementSink]): factory to create
            statement sink.
            By default creates an empty in-memory GenericStatementSink.
            Has no division for datasets/graphs,
            utilizes the same underlying data structures.

    Returns:
        GenericStatementSink: GenericStatementSink with statements.

    """
    options, frames = get_options_and_frames(inp)
    sink = sink_factory()

    for item in parse_jelly_flat(
        inp=inp, frames=frames, options=options, logical_type_strict=False
    ):
        if isinstance(item, Prefix):
            sink.bind(item.prefix, item.iri)  # type: ignore[union-attr, unused-ignore]
        else:
            sink.add(item)
    return sink


def parse_jelly_flat(
    inp: IO[bytes],
    frames: Iterable[jelly.RdfStreamFrame] | None = None,
    options: ParserOptions | None = None,
    *,
    logical_type_strict: bool = False,
) -> Generator[Statement | Prefix]:  # type: ignore[valid-type, unused-ignore]
    """
    Parse jelly file with FLAT logical type into a Generator of stream events.

    Args:
        inp (IO[bytes]): input jelly buffered binary stream.
        frames (Iterable[jelly.RdfStreamFrame | None):
            jelly frames if read before.
        options (ParserOptions | None): stream options
            if read before.
        logical_type_strict (bool): If True, validate the *logical* type
            in stream options and require FLAT (TRIPLES/QUADS).
            Otherwise, only the physical type is used to route parsing.

    Raises:
        NotImplementedError: if physical type is not supported

    Yields:
        Generator[Statement | Prefix]: Generator of stream events

    """
    if frames is None or options is None:
        options, frames = get_options_and_frames(inp)

    st = getattr(options, "stream_types", None)
    if logical_type_strict and (st is None or not st.flat):
        lt_name = (
            "UNSPECIFIED"
            if st is None
            else jelly.LogicalStreamType.Name(st.logical_type)
        )

        msg = (
            "strict logical type check requires options.stream_types"
            if st is None
            else f"expected FLAT logical type (TRIPLES/QUADS), got {lt_name}"
        )
        raise JellyConformanceError(msg)

    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        for triples in parse_triples_stream(frames=frames, options=options):
            yield from triples
        return
    if options.stream_types.physical_type in (
        jelly.PHYSICAL_STREAM_TYPE_QUADS,
        jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
    ):
        for quads in parse_quads_stream(frames=frames, options=options):
            yield from quads
        return
    physical_type_name = jelly.PhysicalStreamType.Name(
        options.stream_types.physical_type
    )
    msg = f"the stream type {physical_type_name} is not supported "
    raise NotImplementedError(msg)
