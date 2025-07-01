from __future__ import annotations

from collections import deque
from collections.abc import Generator, Iterable
from typing import IO, Any, Callable
from typing_extensions import override

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.parse.decode import Adapter, Decoder, ParserOptions, ParsingMode
from pyjelly.parse.ioutils import get_options_and_frames
from pyjelly.parse.statement_sink import StatementSink


class GenericStatementSink:
    _store: deque[tuple[Any, ...]]

    def __init__(self) -> None:
        self._store: deque[tuple[Any, ...]] = deque()

    def add(self, statement: Iterable[Any]) -> None:
        self._store.append(tuple(statement))

    def bind(self, prefix: str, namespace: str) -> None:
        msg = "namespace declarations are not supported in generic statement sinks"
        raise NotImplementedError(msg)

    def __iter__(self) -> Generator[Any, None, None]:
        while self._store:
            yield self._store.popleft()


class GenericStatementSinkAdapter(Adapter):
    """
    Implement Adapter for generic statements.

    Args:
        Adapter (_type_): base Adapter class

    """

    def __init__(
        self,
        options: ParserOptions,
        factory: Callable[[], StatementSink],
        parsing_mode: ParsingMode = ParsingMode.FLAT,
    ) -> None:
        super().__init__(options, parsing_mode)
        self.statement_sink = factory()
        self.factory = factory

    @override
    def iri(self, iri: str) -> str:
        return iri

    @override
    def bnode(self, bnode: str) -> str:
        return bnode

    @override
    def default_graph(self) -> str:
        return "DEFAULT_GRAPH"

    @override
    def literal(
        self,
        lex: str,
        language: str | None = None,
        datatype: str | None = None,
    ) -> str:
        suffix = ""
        if language:
            suffix = f"@{language}"
        elif datatype:
            suffix = f"^^<{datatype}>"
        return f"{lex}{suffix}"

    @override
    def namespace_declaration(self, name: str, iri: str) -> None:
        self.statement_sink.bind(name, self.iri(iri))

    @override
    def frame(self) -> Any:
        current_statement_sink = self.statement_sink
        self.statement_sink = self.factory()
        return current_statement_sink._store


class GenericStatementSinkTriplesAdapter(GenericStatementSinkAdapter):
    @override
    def triple(self, terms: Iterable[Any]) -> None:
        self.statement_sink.add(tuple(terms))


class GenericStatementSinkQuadsAdapter(GenericStatementSinkAdapter):
    @override
    def quad(self, terms: Iterable[Any]) -> None:
        self.statement_sink.add(tuple(terms))


class GenericStatementSinkGraphsAdapter(GenericStatementSinkAdapter):
    _graph_id: str | None

    def __init__(
        self,
        options: ParserOptions,
        factory: Callable[[], StatementSink],
        parsing_mode: ParsingMode = ParsingMode.FLAT,
    ) -> None:
        super().__init__(
            options=options,
            factory=factory,
            parsing_mode=parsing_mode,
        )
        self._graph_id = None

    @property
    def graph(self) -> None:
        if self._graph_id is None:
            msg = "new graph was not started"
            raise JellyConformanceError(msg)

    @override
    def graph_start(self, graph_id: str) -> None:
        self._graph_id = graph_id

    @override
    def triple(self, terms: Iterable[Any]) -> None:
        self.statement_sink.add((*terms, self._graph_id))

    @override
    def graph_end(self) -> None:
        self._graph_id = None


def parse_triples_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    factory: Callable[[], StatementSink],
    parsing_mode: ParsingMode = ParsingMode.FLAT,
) -> Generator[Any, None, None]:
    """
    Parse generic triples stream.

    Notes:
        Emits statements

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): jelly frames
        options (ParserOptions): stream oprions
        factory (Callable[[], StatementSink]): factory to create graph,
            here, GenericStatementSink that saves generic statements
        parsing_mode (ParsingMode, optional): whether to treat
            each frame as a separate deque of statements. Defaults to ParsingMode.FLAT.

    Yields:
        Generator[Any, None, None]: returns generator per statement or per frame

    """
    adapter = GenericStatementSinkTriplesAdapter(
        options, factory=factory, parsing_mode=parsing_mode
    )
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        if parsing_mode is ParsingMode.FLAT:
            for _ in decoder.iter_rows(frame):
                yield from adapter.statement_sink
        else:
            yield decoder.decode_frame(frame)
    return


def parse_quads_stream(
    frames: Iterable[jelly.RdfStreamFrame],
    options: ParserOptions,
    factory: Callable[[], StatementSink],
    parsing_mode: ParsingMode = ParsingMode.FLAT,
) -> Generator[Any, None, None]:
    """
    Parse generic quads stream.

    Args:
        frames (Iterable[jelly.RdfStreamFrame]): jelly frames
        options (ParserOptions): stream oprions
        factory (Callable[[], StatementSink]): factory to create dataset,
            here, GenericStatementSink that saves generic statements
        parsing_mode (ParsingMode, optional): whether to treat
            each frame as a separate deque of statements. Defaults to ParsingMode.FLAT.

    Yields:
        Generator[Any, None, None]: returns generator per statement or per frame

    """
    adapter_class: type[GenericStatementSinkAdapter]

    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_QUADS:
        adapter_class = GenericStatementSinkQuadsAdapter
    else:
        adapter_class = GenericStatementSinkGraphsAdapter
    adapter = adapter_class(
        options=options,
        factory=factory,
        parsing_mode=parsing_mode,
    )
    decoder = Decoder(adapter=adapter)
    for frame in frames:
        if parsing_mode is ParsingMode.FLAT:
            for _ in decoder.iter_rows(frame):
                yield from adapter.statement_sink
        else:
            yield decoder.decode_frame(frame)
    return


def parse_jelly_grouped(
    inp: IO[bytes],
    factory: Callable[[], StatementSink],
) -> Generator[Any, None, None]:
    """
    Take jelly file and return generators based on the detected logical type.

    Yields one graph/dataset per frame.

    Args:
        inp (IO[bytes]): input jelly buffered binary stream
        factory (Callable): lambda to construct a Generic Statement Sink

    Yields:
        Generator[Any]:
            returns generators for GenericStatementSink._store based on input type
                here, deque[tuple()] -- deque of triples/quads

    """
    options, frames = get_options_and_frames(inp)

    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        yield from parse_triples_stream(
            frames=frames,
            options=options,
            factory=factory,
            parsing_mode=ParsingMode.GROUPED,
        )
        return

    if options.stream_types.physical_type in (
        jelly.PHYSICAL_STREAM_TYPE_QUADS,
        jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
    ):
        yield from parse_quads_stream(
            frames=frames,
            options=options,
            factory=factory,
            parsing_mode=ParsingMode.GROUPED,
        )
        return


def parse_jelly_flat(
    inp: IO[bytes],
    factory: Callable[[], StatementSink],
) -> Generator[Any, None, None]:
    """
    Parse jelly file with FLAT physical type into one generic statement sink generator.

    Args:
        inp (IO[bytes]): input jelly buffered binary stream
        factory (Callable): lambda to construct a Generic Statement Sink

    Yields:
        Generator[Any] Generator of statements, here tuple() -- triples/quads

    """
    options, frames = get_options_and_frames(inp)

    if options.stream_types.physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES:
        yield from parse_triples_stream(
            frames=frames,
            options=options,
            factory=factory,
            parsing_mode=ParsingMode.FLAT,
        )

    if options.stream_types.physical_type in (
        jelly.PHYSICAL_STREAM_TYPE_QUADS,
        jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
    ):
        yield from parse_quads_stream(
            frames=frames,
            options=options,
            factory=factory,
            parsing_mode=ParsingMode.FLAT,
        )
