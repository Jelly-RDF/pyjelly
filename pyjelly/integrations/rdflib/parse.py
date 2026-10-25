from __future__ import annotations

from collections.abc import Callable, Generator, Iterable, MutableMapping
from contextvars import ContextVar
from typing import IO, Final, cast

from rdflib import Node
from rdflib.graph import ConjunctiveGraph, Dataset, Graph
from rdflib.parser import InputSource
from rdflib.parser import Parser as RDFLibParser

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.rdflib.decode import (
    RDFLibGraphsAdapter,
    RDFLibQuadsAdapter,
    RDFLibQuadsBaseAdapter,
    RDFLibTriplesAdapter,
)
from pyjelly.integrations.rdflib.statements import (
    GraphName,
    Prefix,
    Quad,
    Statement,
    Triple,
)
from pyjelly.parse.decode import Decoder, ParserOptions, flat_rows, grouped_rows
from pyjelly.parse.ioutils import get_options_and_frames


def parse_triples_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    frame_metadata: ContextVar[MutableMapping[str, bytes]] | None = None,
    *,
    validate_iris: bool = False,
) -> Generator[Iterable[Triple | Prefix]]:
    """
    Parse flat triple stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options
        frame_metadata: (ContextVar[ScalarMap[str, bytes]]): context variable
            used for extracting frame metadata
        validate_iris (bool): whether rdflib checks the IRIs. An invalid IRI is
            still read, rdflib only logs a warning. Checking takes time.

    Yields:
        Generator[Iterable[Triple | Prefix]]:
            Generator of iterables of Triple or Prefix objects,
            one iterable per frame.

    """
    adapter = RDFLibTriplesAdapter(options, validate_iris=validate_iris)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        if frame_metadata is not None:
            frame_metadata.set(
                frame.metadata
            ) if frame.metadata else frame_metadata.set({})
        yield decoder.iter_rows(frame)
    if options.params.stream_type == jelly.RDF_STREAM_TYPE_MESSAGES:
        yield decoder.end_of_stream()


def parse_quads_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    frame_metadata: ContextVar[MutableMapping[str, bytes]] | None = None,
    *,
    validate_iris: bool = False,
) -> Generator[Iterable[Quad | Prefix]]:
    """
    Parse flat quads stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): iterator over stream frames
        options (ParserOptions): stream options
        frame_metadata: (ContextVar[ScalarMap[str, bytes]]): context variable
            used for extracting frame metadata
        validate_iris (bool): whether rdflib checks the IRIs. An invalid IRI is
            still read, rdflib only logs a warning. Checking takes time.

    Yields:
        Generator[Iterable[Quad | Prefix]]:
            Generator of iterables of Quad or Prefix objects,
            one iterable per frame.

    """
    adapter_class: type[RDFLibQuadsBaseAdapter]
    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_QUADS:
        adapter_class = RDFLibQuadsAdapter
    else:
        adapter_class = RDFLibGraphsAdapter
    adapter = adapter_class(options=options, validate_iris=validate_iris)
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        if frame_metadata is not None:
            frame_metadata.set(
                frame.metadata
            ) if frame.metadata else frame_metadata.set({})
        yield decoder.iter_rows(frame)
    if options.params.stream_type == jelly.RDF_STREAM_TYPE_MESSAGES:
        yield decoder.end_of_stream()


def parse_jelly_grouped(  # noqa: PLR0913 keyword-only options
    inp: IO[bytes],
    graph_factory: Callable[[], Graph] = Graph,
    dataset_factory: Callable[[], Dataset] = Dataset,
    *,
    logical_type_strict: bool = False,
    frame_metadata: ContextVar[MutableMapping[str, bytes]] | None = None,
    validate_iris: bool = False,
) -> Generator[Graph] | Generator[Dataset]:
    """
    Take jelly file and return generators based on the detected physical type.

    Yields one graph/dataset per frame.

    Args:
        inp (IO[bytes]): input jelly buffered binary stream
        graph_factory (Callable): lambda to construct a Graph.
            By default creates an empty in-memory Graph,
            but you can pass something else here.
        dataset_factory (Callable): lambda to construct a Dataset.
            By default creates an empty in-memory Dataset,
            but you can pass something else here.
        logical_type_strict (bool): If True, validate the *logical* type in
            stream options and require a grouped logical type. Otherwise, only the
            physical type is used to route parsing.
        frame_metadata: (ContextVar[ScalarMap[str, bytes]]): context variable
            used for extracting frame metadata
        validate_iris (bool): whether rdflib checks the IRIs. An invalid IRI is
            still read, rdflib only logs a warning. Checking takes time.

    Raises:
        NotImplementedError: is raised if a physical type is not implemented

    Yields:
        Generator[Graph] | Generator[Dataset]:
            returns generators for graphs/datasets based on the type of input

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
        for graph in grouped_rows(
            parse_triples_stream(
                frames=frames,
                options=options,
                frame_metadata=frame_metadata,
                validate_iris=validate_iris,
            ),
            options,
        ):
            sink = graph_factory()
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
        for dataset in grouped_rows(
            parse_quads_stream(
                frames=frames,
                options=options,
                frame_metadata=frame_metadata,
                validate_iris=validate_iris,
            ),
            options,
        ):
            sink = dataset_factory()
            for item in dataset:
                if isinstance(item, Prefix):
                    sink.bind(item.prefix, item.iri)
                else:
                    s, p, o, graph_name = item
                    context = sink.get_context(graph_name)
                    sink.add((s, p, o, context))
            yield sink
        return

    physical_type_name = jelly.PhysicalStreamType.Name(
        options.stream_types.physical_type
    )
    msg = f"the stream type {physical_type_name} is not supported "
    raise NotImplementedError(msg)


ADD_BATCH_SIZE: Final = 1000
# How many context graphs to keep for reuse, by graph name
MAX_CACHED_CONTEXTS: Final = 1024


def parse_jelly_to_graph(
    inp: IO[bytes],
    graph_factory: Callable[[], Graph] = Graph,
    dataset_factory: Callable[[], Dataset] = Dataset,
    *,
    validate_iris: bool = False,
) -> Graph | Dataset:
    """
    Add statements from Generator to provided Graph/Dataset.

    Args:
        inp (IO[bytes]): input jelly stream.
        graph_factory (Callable[[], Graph]): factory to create Graph.
            By default creates an empty in-memory Graph,
            but you can pass something else here.
        dataset_factory (Callable[[], Dataset]): factory to create Dataset.
            By default creates an empty in-memory Dataset,
            but you can pass something else here.
        validate_iris (bool): whether rdflib checks the IRIs. An invalid IRI is
            still read, rdflib only logs a warning. Checking takes time.

    Returns:
        Dataset | Graph: Dataset or Graph with statements.

    """
    options, frames = get_options_and_frames(inp)

    sink: Graph
    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        sink = graph_factory()
        # Graph.add only checks the terms before giving them to the store
        to_store = type(sink).add is Graph.add
    else:
        # Other stream types fail in parse_jelly_flat
        sink = dataset_factory()
        # Dataset.add would also make a new context graph for every quad
        to_store = type(sink).add is ConjunctiveGraph.add

    items = parse_jelly_flat(
        inp=inp, frames=frames, options=options, validate_iris=validate_iris
    )
    if to_store:
        _add_to_store(sink, items)
        return sink

    # The Graph or Dataset class changes add(), so it gets every statement
    for item in items:
        if isinstance(item, Prefix):
            sink.bind(item.prefix, item.iri)
        if isinstance(item, Triple):
            sink.add(item)
        if isinstance(item, Quad):
            s, p, o, graph_name = item
            context = cast(Dataset, sink).get_context(graph_name)
            sink.add((s, p, o, context))  # type: ignore[arg-type]
    return sink


def _context(
    dataset: Dataset, contexts: dict[GraphName, Graph], graph_name: GraphName
) -> Graph:
    """Return the dataset's context graph for a graph name, reusing it if cached."""
    context = contexts.get(graph_name)
    if context is None:
        context = dataset.get_context(graph_name)
        if len(contexts) >= MAX_CACHED_CONTEXTS:
            contexts.clear()
        contexts[graph_name] = context
    return context


def _add_to_store(sink: Graph, items: Iterable[Statement | Prefix]) -> None:
    """Add statements to the Graph's or Dataset's store directly, in batches."""
    store = sink.store
    batch: list[tuple[Node, Node, Node, Graph]] = []
    contexts: dict[GraphName, Graph] = {}
    # Repeated graph names are the same objects, so they are checked first
    last_graph_name: object = None
    context = sink

    for item in items:
        if isinstance(item, Quad):
            s, p, o, graph_name = item
            if graph_name is not last_graph_name:
                context = _context(cast(Dataset, sink), contexts, graph_name)
                last_graph_name = graph_name
            batch.append((s, p, o, context))
        elif isinstance(item, Triple):
            s, p, o = item
            batch.append((s, p, o, sink))
        elif isinstance(item, Prefix):
            # Statements before the prefix are added first, to keep the order
            if batch:
                store.addN(batch)
                batch = []
            sink.bind(item.prefix, item.iri)
        if len(batch) >= ADD_BATCH_SIZE:
            store.addN(batch)
            batch = []
    if batch:
        store.addN(batch)


def parse_jelly_flat(
    inp: IO[bytes],
    frames: Iterable[jelly.RdfStreamFrame] | None = None,
    options: ParserOptions | None = None,
    *,
    logical_type_strict: bool = False,
    validate_iris: bool = False,
) -> Generator[Statement | Prefix]:
    """
    Parse jelly file with FLAT logical type into a Generator of stream events.

    Args:
        inp (IO[bytes]): input jelly buffered binary stream.
        frames (Iterable[jelly.RdfStreamFrame | None):
            jelly frames if read before.
        options (ParserOptions | None): stream options
            if read before.
        logical_type_strict (bool): If True, validate the *logical* type in
            stream options and require FLAT_(TRIPLES|QUADS). Otherwise, only the
            physical type is used to route parsing.
        validate_iris (bool): whether rdflib checks the IRIs. An invalid IRI is
            still read, rdflib only logs a warning. Checking takes time.

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
        yield from flat_rows(
            parse_triples_stream(
                frames=frames, options=options, validate_iris=validate_iris
            ),
            options,
        )
        return
    if options.stream_types.physical_type in (
        jelly.PHYSICAL_STREAM_TYPE_QUADS,
        jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
    ):
        yield from flat_rows(
            parse_quads_stream(frames=frames, options=options), options
        )
        return
    physical_type_name = jelly.PhysicalStreamType.Name(
        options.stream_types.physical_type
    )
    msg = f"the stream type {physical_type_name} is not supported "
    raise NotImplementedError(msg)


class RDFLibJellyParser(RDFLibParser):
    def parse(
        self,
        source: InputSource,
        sink: Graph,
        *,
        validate_iris: bool = False,
    ) -> None:
        """
        Parse jelly file into provided RDFLib Graph.

        Args:
            source (InputSource): jelly file as buffered binary stream InputSource obj
            sink (Graph): RDFLib Graph
            validate_iris (bool): whether rdflib checks the IRIs, see
                parse_jelly_to_graph(). Pass it to Graph.parse().

        Raises:
            TypeError: raises error if invalid input

        """
        byte_stream = source.getByteStream()
        if byte_stream is None:
            msg = "expected source to be a stream of bytes"
            raise TypeError(msg)

        inp = cast(IO[bytes], byte_stream)
        if inp is None:
            msg = "expected source to be a stream of bytes"
            raise TypeError(msg)
        parse_jelly_to_graph(
            inp,
            graph_factory=lambda: Graph(store=sink.store, identifier=sink.identifier),
            dataset_factory=lambda: Dataset(store=sink.store),
            validate_iris=validate_iris,
        )
